use std::{
  borrow::Cow,
  ffi::OsStr,
  fs,
  io::{Read, Write},
  path::{Component, Path, PathBuf},
};

use anyhow::{Context, anyhow};
use cap_fs_ext::{
  DirExt as _, FollowSymlinks, OpenOptionsFollowExt as _,
};
use cap_std::fs::{Dir, OpenOptions};
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use komodo_client::entities::file_manager::{
  FileManagerArchiveFormat, FileManagerConflictAction,
  FileManagerConflictDecision,
};
use sevenz_rust2::{ArchiveEntry, ArchiveWriter};
use tar::{
  Archive as TarArchive, Builder as TarBuilder, EntryType, Header,
};
use uuid::Uuid;
use zip::{
  CompressionMethod, ZipArchive, ZipWriter, write::SimpleFileOptions,
};

use super::{
  MAX_ARCHIVE_EXPANDED_BYTES, MAX_ARCHIVE_EXPANSION_RATIO,
  MAX_ENTRIES, MINIMUM_FREE_BYTES, OperationProgress,
  copy_host_to_capability, copy_with_progress, decision_for,
  ensure_free_space, journal_root, path::MAX_DEPTH,
  path::open_parent_nofollow, path::relative_path, path_string,
  remove_entry,
};

struct TemporaryDirectory(PathBuf);

impl Drop for TemporaryDirectory {
  fn drop(&mut self) {
    let _ = fs::remove_dir_all(&self.0);
  }
}

pub fn create(
  root: &Dir,
  paths: &[String],
  destination: &str,
  format: FileManagerArchiveFormat,
  decisions: &[FileManagerConflictDecision],
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  if paths.is_empty() {
    return Err(anyhow!("Select at least one entry to archive"));
  }
  let destination = relative_path(destination, false)?;
  let (parent, name) = open_parent_nofollow(root, &destination)?;
  let destination_string = path_string(&destination)?;
  if parent.symlink_metadata(&name).is_ok() {
    match decision_for(&destination_string, decisions) {
      Some(FileManagerConflictAction::Skip) => return Ok(()),
      Some(FileManagerConflictAction::Overwrite) => {
        remove_entry(&parent, &name)?
      }
      None => {
        return Err(anyhow!("Archive destination already exists"));
      }
    }
  }

  let temporary = format!(".komodo-archive-{}.tmp", Uuid::new_v4());
  let mut options = OpenOptions::new();
  options
    .write(true)
    .read(true)
    .create_new(true)
    .follow(FollowSymlinks::No);
  let output = parent.open_with(&temporary, &options)?;
  let result = match format {
    FileManagerArchiveFormat::Zip => {
      create_zip(root, paths, output, progress)
    }
    FileManagerArchiveFormat::Tar => {
      create_tar(root, paths, output, progress)
    }
    FileManagerArchiveFormat::TarGz => {
      create_tar_gz(root, paths, output, progress)
    }
    FileManagerArchiveFormat::SevenZip => {
      create_seven_zip(root, paths, output, progress)
    }
  };
  if let Err(error) = result {
    let _ = parent.remove_file(&temporary);
    return Err(error);
  }
  parent.rename(temporary, &parent, name)?;
  Ok(())
}

pub fn create_download_zip(
  root: &Dir,
  paths: &[String],
  output: &Path,
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  if paths.is_empty() {
    return Err(anyhow!("Select at least one entry to download"));
  }
  let file = fs::File::create(output)?;
  let mut writer = ZipWriter::new(file);
  let options = SimpleFileOptions::default()
    .compression_method(CompressionMethod::Deflated)
    .unix_permissions(0o644);
  let directory_options = SimpleFileOptions::default()
    .compression_method(CompressionMethod::Stored)
    .unix_permissions(0o755);
  let options = ZipEntryOptions {
    file: options,
    directory: directory_options,
  };
  let mut count = 0;
  for path in paths {
    let path = relative_path(path, false)?;
    let (parent, name) = open_parent_nofollow(root, &path)?;
    add_zip_entry(
      &mut writer,
      &parent,
      &name,
      &path_string(&path)?,
      options,
      &mut count,
      progress,
    )?;
  }
  writer.finish()?.sync_all()?;
  Ok(())
}

pub fn extract(
  root: &Dir,
  archive_path: &str,
  destination: &str,
  decisions: &[FileManagerConflictDecision],
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  let archive_path = relative_path(archive_path, false)?;
  let destination = relative_path(destination, false)?;
  let (archive_parent, archive_name) =
    open_parent_nofollow(root, &archive_path)?;
  let archive_metadata =
    archive_parent.symlink_metadata(&archive_name)?;
  if !archive_metadata.is_file()
    || archive_metadata.file_type().is_symlink()
  {
    return Err(anyhow!("Archive must be a regular file"));
  }

  let staging = journal_root()
    .join("transfers")
    .join(Uuid::new_v4().to_string());
  let extracted = staging.join("extracted");
  fs::create_dir_all(&extracted)?;
  let _cleanup = TemporaryDirectory(staging.clone());
  let conservative_expanded_bytes = archive_metadata
    .len()
    .saturating_mul(MAX_ARCHIVE_EXPANSION_RATIO)
    .min(MAX_ARCHIVE_EXPANDED_BYTES);
  ensure_free_space(
    &staging,
    archive_metadata
      .len()
      .saturating_add(conservative_expanded_bytes)
      .saturating_add(MINIMUM_FREE_BYTES),
  )?;
  let staged_archive = staging.join("archive");
  let mut read_options = OpenOptions::new();
  read_options.read(true).follow(FollowSymlinks::No);
  let mut source =
    archive_parent.open_with(archive_name, &read_options)?;
  let mut staged = fs::File::create(&staged_archive)?;
  copy_with_progress(&mut source, &mut staged, progress)?;
  staged.sync_all()?;

  (|| {
    match detect_format(&staged_archive)? {
      DetectedArchive::Zip => {
        extract_zip(&staged_archive, &extracted, progress)?
      }
      DetectedArchive::Tar => {
        extract_tar(&staged_archive, &extracted, progress)?
      }
      DetectedArchive::TarGz => {
        extract_tar_gz(&staged_archive, &extracted, progress)?
      }
      DetectedArchive::SevenZip => {
        extract_seven_zip(&staged_archive, &extracted, progress)?
      }
      DetectedArchive::Rar => {
        return Err(anyhow!(
          "RAR extraction is not enabled in this release"
        ));
      }
    }
    validate_staged_tree(&extracted)?;

    let (destination_parent, destination_name) =
      open_parent_nofollow(root, &destination)?;
    let destination_string = path_string(&destination)?;
    if destination_parent
      .symlink_metadata(&destination_name)
      .is_ok()
    {
      match decision_for(&destination_string, decisions) {
        Some(FileManagerConflictAction::Skip) => return Ok(()),
        Some(FileManagerConflictAction::Overwrite) => {
          remove_entry(&destination_parent, &destination_name)?
        }
        None => {
          return Err(anyhow!(
            "Extraction destination already exists"
          ));
        }
      }
    }
    copy_host_to_capability(
      &extracted,
      &destination_parent,
      &destination_name,
      progress,
    )?;
    Ok(())
  })()
}

fn create_zip(
  root: &Dir,
  paths: &[String],
  output: cap_std::fs::File,
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  let mut writer = ZipWriter::new(output);
  let options = SimpleFileOptions::default()
    .compression_method(CompressionMethod::Deflated)
    .unix_permissions(0o644);
  let directory_options = SimpleFileOptions::default()
    .compression_method(CompressionMethod::Stored)
    .unix_permissions(0o755);
  let options = ZipEntryOptions {
    file: options,
    directory: directory_options,
  };
  let mut count = 0_u64;
  for path in paths {
    let path = relative_path(path, false)?;
    let (parent, name) = open_parent_nofollow(root, &path)?;
    add_zip_entry(
      &mut writer,
      &parent,
      &name,
      &path_string(&path)?,
      options,
      &mut count,
      progress,
    )?;
  }
  writer.finish()?.sync_all()?;
  Ok(())
}

#[derive(Clone, Copy)]
struct ZipEntryOptions {
  file: SimpleFileOptions,
  directory: SimpleFileOptions,
}

fn add_zip_entry<W: Write + std::io::Seek>(
  writer: &mut ZipWriter<W>,
  parent: &Dir,
  name: &OsStr,
  archive_name: &str,
  options: ZipEntryOptions,
  count: &mut u64,
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  *count += 1;
  if *count > MAX_ENTRIES {
    return Err(anyhow!("Archive exceeds the entry limit"));
  }
  let metadata = parent.symlink_metadata(name)?;
  if metadata.file_type().is_symlink() {
    return Err(anyhow!("Archives cannot contain symbolic links"));
  }
  if metadata.is_file() {
    writer.start_file(archive_name, options.file)?;
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    let mut file = parent.open_with(name, &options)?;
    copy_with_progress(&mut file, writer, progress)?;
    if let Some(progress) = progress {
      progress.add_entry();
    }
  } else if metadata.is_dir() {
    writer
      .add_directory(format!("{archive_name}/"), options.directory)?;
    let dir = parent.open_dir_nofollow(name)?;
    for entry in dir.entries()? {
      let entry = entry?;
      let child = entry.file_name().into_string().map_err(|_| {
        anyhow!("Non-UTF-8 filenames are unsupported")
      })?;
      add_zip_entry(
        writer,
        &dir,
        OsStr::new(&child),
        &format!("{archive_name}/{child}"),
        options,
        count,
        progress,
      )?;
    }
    if let Some(progress) = progress {
      progress.add_entry();
    }
  } else {
    return Err(anyhow!("Archives cannot contain special entries"));
  }
  Ok(())
}

fn create_tar(
  root: &Dir,
  paths: &[String],
  output: cap_std::fs::File,
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  let mut builder = TarBuilder::new(output);
  append_tar_paths(root, paths, &mut builder, progress)?;
  builder.finish()?;
  builder.into_inner()?.sync_all()?;
  Ok(())
}

fn create_tar_gz(
  root: &Dir,
  paths: &[String],
  output: cap_std::fs::File,
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  let encoder = GzEncoder::new(output, Compression::default());
  let mut builder = TarBuilder::new(encoder);
  append_tar_paths(root, paths, &mut builder, progress)?;
  builder.finish()?;
  let encoder = builder.into_inner()?;
  encoder.finish()?.sync_all()?;
  Ok(())
}

fn append_tar_paths<W: Write>(
  root: &Dir,
  paths: &[String],
  builder: &mut TarBuilder<W>,
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  let mut count = 0_u64;
  for path in paths {
    let path = relative_path(path, false)?;
    let (parent, name) = open_parent_nofollow(root, &path)?;
    append_tar_entry(
      builder,
      &parent,
      &name,
      &path_string(&path)?,
      &mut count,
      progress,
    )?;
  }
  Ok(())
}

fn append_tar_entry<W: Write>(
  builder: &mut TarBuilder<W>,
  parent: &Dir,
  name: &OsStr,
  archive_name: &str,
  count: &mut u64,
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  *count += 1;
  if *count > MAX_ENTRIES {
    return Err(anyhow!("Archive exceeds the entry limit"));
  }
  let metadata = parent.symlink_metadata(name)?;
  if metadata.file_type().is_symlink() {
    return Err(anyhow!("Archives cannot contain symbolic links"));
  }
  let mut header = Header::new_gnu();
  if metadata.is_file() {
    header.set_size(metadata.len());
    header.set_mode(0o644);
    header.set_entry_type(EntryType::Regular);
    header.set_cksum();
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    let file = parent.open_with(name, &options)?;
    builder.append_data(
      &mut header,
      archive_name,
      ProgressReader::new(file, progress),
    )?;
    if let Some(progress) = progress {
      progress.add_entry();
    }
  } else if metadata.is_dir() {
    header.set_size(0);
    header.set_mode(0o755);
    header.set_entry_type(EntryType::Directory);
    header.set_cksum();
    builder.append_data(
      &mut header,
      format!("{archive_name}/"),
      std::io::empty(),
    )?;
    let dir = parent.open_dir_nofollow(name)?;
    for entry in dir.entries()? {
      let entry = entry?;
      let child = entry.file_name().into_string().map_err(|_| {
        anyhow!("Non-UTF-8 filenames are unsupported")
      })?;
      append_tar_entry(
        builder,
        &dir,
        OsStr::new(&child),
        &format!("{archive_name}/{child}"),
        count,
        progress,
      )?;
    }
    if let Some(progress) = progress {
      progress.add_entry();
    }
  } else {
    return Err(anyhow!("Archives cannot contain special entries"));
  }
  Ok(())
}

fn create_seven_zip(
  root: &Dir,
  paths: &[String],
  output: cap_std::fs::File,
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  let mut writer = ArchiveWriter::new(output)?;
  let mut count = 0_u64;
  for path in paths {
    let path = relative_path(path, false)?;
    let (parent, name) = open_parent_nofollow(root, &path)?;
    add_seven_zip_entry(
      &mut writer,
      &parent,
      &name,
      &path_string(&path)?,
      &mut count,
      progress,
    )?;
  }
  writer.finish()?.sync_all()?;
  Ok(())
}

fn add_seven_zip_entry<W: Write + std::io::Seek>(
  writer: &mut ArchiveWriter<W>,
  parent: &Dir,
  name: &OsStr,
  archive_name: &str,
  count: &mut u64,
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  *count += 1;
  if *count > MAX_ENTRIES {
    return Err(anyhow!("Archive exceeds the entry limit"));
  }
  let metadata = parent.symlink_metadata(name)?;
  if metadata.file_type().is_symlink() {
    return Err(anyhow!("Archives cannot contain symbolic links"));
  }
  if metadata.is_file() {
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    let file = parent.open_with(name, &options)?;
    writer.push_archive_entry(
      ArchiveEntry::new_file(archive_name),
      Some(ProgressReader::new(file, progress)),
    )?;
    if let Some(progress) = progress {
      progress.add_entry();
    }
  } else if metadata.is_dir() {
    writer.push_archive_entry::<cap_std::fs::File>(
      ArchiveEntry::new_directory(archive_name),
      None,
    )?;
    let dir = parent.open_dir_nofollow(name)?;
    for entry in dir.entries()? {
      let entry = entry?;
      let child = entry.file_name().into_string().map_err(|_| {
        anyhow!("Non-UTF-8 filenames are unsupported")
      })?;
      add_seven_zip_entry(
        writer,
        &dir,
        OsStr::new(&child),
        &format!("{archive_name}/{child}"),
        count,
        progress,
      )?;
    }
    if let Some(progress) = progress {
      progress.add_entry();
    }
  } else {
    return Err(anyhow!("Archives cannot contain special entries"));
  }
  Ok(())
}

#[derive(Debug, Clone, Copy)]
enum DetectedArchive {
  Zip,
  Tar,
  TarGz,
  SevenZip,
  Rar,
}

struct ProgressReader<'a, R> {
  inner: R,
  progress: Option<&'a OperationProgress>,
}

impl<'a, R> ProgressReader<'a, R> {
  fn new(inner: R, progress: Option<&'a OperationProgress>) -> Self {
    Self { inner, progress }
  }
}

impl<R: Read> Read for ProgressReader<'_, R> {
  fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
    let read = self.inner.read(buffer)?;
    if let Some(progress) = self.progress {
      progress.add_bytes(read as u64);
    }
    Ok(read)
  }
}

impl<R: std::io::Seek> std::io::Seek for ProgressReader<'_, R> {
  fn seek(
    &mut self,
    position: std::io::SeekFrom,
  ) -> std::io::Result<u64> {
    self.inner.seek(position)
  }
}

fn detect_format(path: &Path) -> anyhow::Result<DetectedArchive> {
  let mut file = fs::File::open(path)?;
  let mut header = [0_u8; 512];
  let read = file.read(&mut header)?;
  if read >= 6 && header[..6] == [0x37, 0x7a, 0xbc, 0xaf, 0x27, 0x1c]
  {
    return Ok(DetectedArchive::SevenZip);
  }
  if read >= 4 && &header[..2] == b"PK" {
    return Ok(DetectedArchive::Zip);
  }
  if read >= 2 && header[..2] == [0x1f, 0x8b] {
    return Ok(DetectedArchive::TarGz);
  }
  if (read >= 7 && &header[..7] == b"Rar!\x1a\x07\x00")
    || (read >= 8 && &header[..8] == b"Rar!\x1a\x07\x01\x00")
  {
    return Ok(DetectedArchive::Rar);
  }
  if read >= 262 && &header[257..262] == b"ustar" {
    return Ok(DetectedArchive::Tar);
  }
  Err(anyhow!("Archive signature is unsupported"))
}

fn extract_zip(
  source: &Path,
  destination: &Path,
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  let file = fs::File::open(source)?;
  let compressed_size = file.metadata()?.len().max(1);
  let mut archive = ZipArchive::new(file)?;
  let mut expanded = 0_u64;
  if archive.len() as u64 > MAX_ENTRIES {
    return Err(anyhow!("Archive exceeds the entry limit"));
  }
  for index in 0..archive.len() {
    let mut entry = archive.by_index(index)?;
    if entry.encrypted() {
      return Err(anyhow!("Encrypted archives are unsupported"));
    }
    validate_archive_name(entry.name())?;
    expanded = expanded
      .checked_add(entry.size())
      .context("Archive expanded size overflow")?;
    enforce_expansion_limits(expanded, compressed_size)?;
    if let Some(mode) = entry.unix_mode() {
      let file_type = mode & 0o170000;
      if file_type != 0
        && file_type != 0o100000
        && file_type != 0o040000
      {
        return Err(anyhow!(
          "Archive contains a link or special entry"
        ));
      }
    }
    let output = destination.join(archive_relative(entry.name())?);
    if entry.is_dir() {
      fs::create_dir_all(&output)?;
    } else {
      if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
      }
      let mut file = create_safe_file(&output)?;
      let copied =
        copy_with_progress(&mut entry, &mut file, progress)?;
      if copied != entry.size() {
        return Err(anyhow!(
          "Archive entry size changed during extraction"
        ));
      }
      file.sync_all()?;
    }
    if let Some(progress) = progress {
      progress.add_entry();
    }
  }
  Ok(())
}

fn extract_tar(
  source: &Path,
  destination: &Path,
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  let file = fs::File::open(source)?;
  let compressed_size = file.metadata()?.len().max(1);
  extract_tar_reader(file, compressed_size, destination, progress)
}

fn extract_tar_gz(
  source: &Path,
  destination: &Path,
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  let file = fs::File::open(source)?;
  let compressed_size = file.metadata()?.len().max(1);
  extract_tar_reader(
    GzDecoder::new(file),
    compressed_size,
    destination,
    progress,
  )
}

fn extract_tar_reader<R: Read>(
  reader: R,
  compressed_size: u64,
  destination: &Path,
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  let mut archive = TarArchive::new(reader);
  let mut count = 0_u64;
  let mut expanded = 0_u64;
  for entry in archive.entries()? {
    count += 1;
    if count > MAX_ENTRIES {
      return Err(anyhow!("Archive exceeds the entry limit"));
    }
    let mut entry = entry?;
    let entry_type = entry.header().entry_type();
    if !entry_type.is_file() && !entry_type.is_dir() {
      return Err(anyhow!(
        "Archive contains a link or special entry"
      ));
    }
    let path = entry.path()?.into_owned();
    let path =
      path.to_str().context("Archive contains a non-UTF-8 path")?;
    validate_archive_name(path)?;
    let size = entry.header().size()?;
    expanded = expanded
      .checked_add(size)
      .context("Archive expanded size overflow")?;
    enforce_expansion_limits(expanded, compressed_size)?;
    let output = destination.join(archive_relative(path)?);
    if entry_type.is_dir() {
      fs::create_dir_all(output)?;
    } else {
      if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
      }
      let mut file = create_safe_file(&output)?;
      let copied =
        copy_with_progress(&mut entry, &mut file, progress)?;
      if copied != size {
        return Err(anyhow!(
          "Archive entry size changed during extraction"
        ));
      }
      file.sync_all()?;
    }
    if let Some(progress) = progress {
      progress.add_entry();
    }
  }
  Ok(())
}

fn extract_seven_zip(
  source: &Path,
  destination: &Path,
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  let compressed_size = fs::metadata(source)?.len().max(1);
  let mut count = 0_u64;
  let mut expanded = 0_u64;
  sevenz_rust2::decompress_with_extract_fn(
    fs::File::open(source)?,
    destination,
    |entry, reader, output| {
      count += 1;
      if count > MAX_ENTRIES {
        return Err(sevenz_rust2::Error::Other(Cow::Borrowed(
          "Archive exceeds the entry limit",
        )));
      }
      validate_archive_name(entry.name()).map_err(|error| {
        sevenz_rust2::Error::Other(Cow::Owned(error.to_string()))
      })?;
      if entry.is_anti_item() {
        return Err(sevenz_rust2::Error::Other(Cow::Borrowed(
          "Archive contains an anti-item",
        )));
      }
      let unix_mode = entry.windows_attributes() >> 16;
      let file_type = unix_mode & 0o170000;
      if file_type != 0
        && file_type != 0o100000
        && file_type != 0o040000
      {
        return Err(sevenz_rust2::Error::Other(Cow::Borrowed(
          "Archive contains a link or special entry",
        )));
      }
      expanded = expanded.checked_add(entry.size()).ok_or({
        sevenz_rust2::Error::Other(Cow::Borrowed(
          "Archive expanded size overflow",
        ))
      })?;
      enforce_expansion_limits(expanded, compressed_size).map_err(
        |error| {
          sevenz_rust2::Error::Other(Cow::Owned(error.to_string()))
        },
      )?;
      let result =
        sevenz_rust2::default_entry_extract_fn(entry, reader, output);
      if result.is_ok()
        && let Some(progress) = progress
      {
        progress.add_bytes(entry.size());
        progress.add_entry();
      }
      result
    },
  )?;
  Ok(())
}

fn validate_archive_name(name: &str) -> anyhow::Result<()> {
  archive_relative(name).map(|_| ())
}

fn archive_relative(name: &str) -> anyhow::Result<PathBuf> {
  if name.is_empty() || name.contains('\0') || name.contains('\\') {
    return Err(anyhow!("Archive entry path is unsafe"));
  }
  let path = Path::new(name);
  if path.is_absolute()
    || name.as_bytes().get(1).is_some_and(|byte| *byte == b':')
  {
    return Err(anyhow!("Archive entry path is absolute"));
  }
  let mut output = PathBuf::new();
  let mut depth = 0;
  for component in path.components() {
    match component {
      Component::Normal(component) => {
        output.push(component);
        depth += 1;
      }
      Component::CurDir => {}
      _ => {
        return Err(anyhow!("Archive entry path traverses its root"));
      }
    }
  }
  if output.as_os_str().is_empty() || depth > MAX_DEPTH {
    return Err(anyhow!("Archive entry path depth is invalid"));
  }
  Ok(output)
}

fn enforce_expansion_limits(
  expanded: u64,
  compressed: u64,
) -> anyhow::Result<()> {
  if expanded > MAX_ARCHIVE_EXPANDED_BYTES {
    return Err(anyhow!("Archive exceeds the expanded-size limit"));
  }
  if expanded / compressed.max(1) > MAX_ARCHIVE_EXPANSION_RATIO {
    return Err(anyhow!("Archive exceeds the expansion-ratio limit"));
  }
  Ok(())
}

fn validate_staged_tree(root: &Path) -> anyhow::Result<()> {
  fn visit(path: &Path, count: &mut u64) -> anyhow::Result<()> {
    for entry in fs::read_dir(path)? {
      *count += 1;
      if *count > MAX_ENTRIES {
        return Err(anyhow!("Archive exceeds the entry limit"));
      }
      let entry = entry?;
      let metadata = fs::symlink_metadata(entry.path())?;
      if metadata.file_type().is_symlink()
        || (!metadata.is_file() && !metadata.is_dir())
      {
        return Err(anyhow!(
          "Archive produced a link or special entry"
        ));
      }
      if metadata.is_dir() {
        visit(&entry.path(), count)?;
      }
    }
    Ok(())
  }
  let mut count = 0;
  visit(root, &mut count)
}

fn create_safe_file(path: &Path) -> anyhow::Result<fs::File> {
  use std::fs::OpenOptions as StdOpenOptions;
  let mut options = StdOpenOptions::new();
  options.write(true).create_new(true);
  #[cfg(unix)]
  {
    use std::os::unix::fs::OpenOptionsExt as _;
    options.mode(0o600);
  }
  options.open(path).map_err(Into::into)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn rejects_archive_traversal_and_windows_paths() {
    for path in [
      "../escape",
      "a/../../escape",
      "/absolute",
      "C:/escape",
      "a\\..\\escape",
    ] {
      assert!(archive_relative(path).is_err(), "accepted {path:?}");
    }
  }

  #[test]
  fn recognizes_supported_and_deferred_signatures() {
    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    fs::create_dir(&directory).unwrap();
    let path = directory.join("archive");
    fs::write(&path, b"Rar!\x1a\x07\x01\x00").unwrap();
    assert!(matches!(
      detect_format(&path).unwrap(),
      DetectedArchive::Rar
    ));
    fs::remove_dir_all(directory).unwrap();
  }

  #[test]
  fn zip_extraction_round_trips_regular_files() {
    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    let archive_path = directory.join("archive.zip");
    let extracted = directory.join("extracted");
    fs::create_dir_all(&extracted).unwrap();
    let file = fs::File::create(&archive_path).unwrap();
    let mut archive = ZipWriter::new(file);
    archive
      .start_file("folder/config.txt", SimpleFileOptions::default())
      .unwrap();
    archive.write_all(b"safe contents").unwrap();
    archive.finish().unwrap();

    extract_zip(&archive_path, &extracted, None).unwrap();
    assert_eq!(
      fs::read(extracted.join("folder/config.txt")).unwrap(),
      b"safe contents"
    );
    fs::remove_dir_all(directory).unwrap();
  }

  #[test]
  fn zip_extraction_rejects_traversal_before_writing_outside() {
    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    let archive_path = directory.join("archive.zip");
    let extracted = directory.join("extracted");
    fs::create_dir_all(&extracted).unwrap();
    let file = fs::File::create(&archive_path).unwrap();
    let mut archive = ZipWriter::new(file);
    archive
      .start_file("../escape.txt", SimpleFileOptions::default())
      .unwrap();
    archive.write_all(b"unsafe").unwrap();
    archive.finish().unwrap();

    assert!(extract_zip(&archive_path, &extracted, None).is_err());
    assert!(!directory.join("escape.txt").exists());
    fs::remove_dir_all(directory).unwrap();
  }
}
