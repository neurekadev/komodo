use std::{
  borrow::Cow,
  ffi::{OsStr, OsString},
  fmt, fs,
  io::{Read, Seek as _, Write},
  path::{Component, Path, PathBuf},
};

use anyhow::{Context, anyhow};
use cap_fs_ext::{
  DirExt as _, FollowSymlinks, OpenOptionsFollowExt as _,
};
use cap_std::fs::{Dir, OpenOptions};
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use komodo_client::entities::file_manager::{
  FileManagerArchiveFormat, FileManagerConflict,
  FileManagerConflictAction, FileManagerConflictDecision,
};
use sevenz_rust2::{
  ArchiveEntry, ArchiveReader, ArchiveWriter, Password,
};
use tar::{
  Archive as TarArchive, Builder as TarBuilder, EntryType, Header,
};
use uuid::Uuid;
use zip::{
  CompressionMethod, ZipArchive, ZipWriter, write::SimpleFileOptions,
};

use super::{
  MAX_ARCHIVE_EXPANSION_RATIO, MINIMUM_FREE_BYTES, OperationProgress,
  WorkTotal, collect_merge_conflicts, copy_with_progress,
  decision_for, ensure_entry_limit, ensure_free_space,
  path::MAX_DEPTH, path::open_parent_nofollow, path::relative_path,
  path_string, remove_entry,
};

struct TemporaryDirectory(PathBuf);

impl Drop for TemporaryDirectory {
  fn drop(&mut self) {
    let _ = fs::remove_dir_all(&self.0);
  }
}

#[derive(Debug)]
struct LatePublishConflict(FileManagerConflict);

impl fmt::Display for LatePublishConflict {
  fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      formatter,
      "Destination changed while extraction was waiting: {}",
      self.0.path
    )
  }
}

impl std::error::Error for LatePublishConflict {}

struct PublishRollback {
  from_parent: Dir,
  from_name: OsString,
  to_parent: Dir,
  to_name: OsString,
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
  root_path: &Path,
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

  let mut read_options = OpenOptions::new();
  read_options.read(true).follow(FollowSymlinks::No);
  let source = archive_parent
    .open_with(archive_name, &read_options)?
    .into_std();

  let destination_parent_path =
    destination.parent().unwrap_or_else(|| Path::new(""));
  let (destination_parent, destination_name) =
    open_parent_nofollow(root, &destination)?;
  let staging_name =
    format!(".komodo-file-manager-staging-{}", Uuid::new_v4());
  destination_parent.create_dir(&staging_name)?;
  let staging =
    root_path.join(destination_parent_path).join(&staging_name);
  let _cleanup = TemporaryDirectory(staging.clone());
  let rollback_name = format!("{staging_name}-rollback");
  destination_parent.create_dir(&rollback_name)?;
  let rollback_path =
    root_path.join(destination_parent_path).join(&rollback_name);
  let _rollback_cleanup = TemporaryDirectory(rollback_path);
  let rollback_dir =
    destination_parent.open_dir_nofollow(&rollback_name)?;
  let format = detect_format(&source)?;

  (|| {
    match format {
      DetectedArchive::Zip => {
        extract_zip(source, &staging, progress)?
      }
      DetectedArchive::Tar => {
        extract_tar(source, &staging, progress)?
      }
      DetectedArchive::TarGz => {
        extract_tar_gz(source, &staging, progress)?
      }
      DetectedArchive::SevenZip => {
        extract_seven_zip(source, &staging, progress)?
      }
      DetectedArchive::Rar => {
        return Err(anyhow!(
          "RAR extraction is not enabled in this release"
        ));
      }
    }
    validate_staged_tree(&staging)?;
    sync_staged_tree(&staging)?;
    if let Some(progress) = progress {
      progress.add_temporary_storage_bytes(host_bytes(&staging)?);
    }

    let staging_relative =
      destination_parent_path.join(&staging_name);
    let mut conflicts = Vec::<FileManagerConflict>::new();
    collect_merge_conflicts(
      root,
      &staging_relative,
      &destination,
      &mut conflicts,
    )?;
    let mut resolved = decisions.to_vec();
    let mut apply_to_all = None;
    for conflict in conflicts {
      if decision_for(&conflict.path, &resolved).is_some() {
        continue;
      }
      let resolution = if let Some(action) = apply_to_all {
        super::ConflictResolution {
          action,
          apply_to_all: true,
        }
      } else {
        let progress = progress.context(
          "Extraction conflict requires an operation status channel",
        )?;
        tokio::runtime::Handle::current()
          .block_on(progress.wait_for_conflict(conflict.clone()))?
      };
      resolved.push(FileManagerConflictDecision {
        path: conflict.path,
        action: resolution.action,
      });
      if resolution.apply_to_all {
        apply_to_all = Some(resolution.action);
      }
    }

    loop {
      let mut rollback = Vec::new();
      let mut backup_index = 0_u64;
      let publish = publish_staged_entry(
        &destination_parent,
        OsStr::new(&staging_name),
        &destination_parent,
        &destination_name,
        &path_string(&destination)?,
        &rollback_dir,
        &mut backup_index,
        &resolved,
        progress,
        &mut rollback,
      );
      match publish {
        Ok(()) => break,
        Err(error) => {
          rollback_publish(rollback).context(
            "Extraction publish failed and could not be rolled back",
          )?;
          let Some(conflict) =
            error.downcast_ref::<LatePublishConflict>()
          else {
            return Err(error.context("Extraction publish failed"));
          };
          let resolution = if let Some(action) = apply_to_all {
            super::ConflictResolution {
              action,
              apply_to_all: true,
            }
          } else {
            let progress = progress.context(
              "Extraction conflict requires an operation status channel",
            )?;
            tokio::runtime::Handle::current().block_on(
              progress.wait_for_conflict(conflict.0.clone()),
            )?
          };
          resolved
            .retain(|decision| decision.path != conflict.0.path);
          resolved.push(FileManagerConflictDecision {
            path: conflict.0.path.clone(),
            action: resolution.action,
          });
          if resolution.apply_to_all {
            apply_to_all = Some(resolution.action);
          }
        }
      }
    }
    Ok(())
  })()
}

#[allow(clippy::too_many_arguments)]
fn publish_staged_entry(
  source_parent: &Dir,
  source_name: &OsStr,
  destination_parent: &Dir,
  destination_name: &OsStr,
  destination_path: &str,
  rollback_dir: &Dir,
  backup_index: &mut u64,
  decisions: &[FileManagerConflictDecision],
  progress: Option<&OperationProgress>,
  rollback: &mut Vec<PublishRollback>,
) -> anyhow::Result<()> {
  if let Some(progress) = progress {
    progress.check_cancelled()?;
  }
  let source_metadata =
    source_parent.symlink_metadata(source_name)?;
  let destination_metadata =
    match destination_parent.symlink_metadata(destination_name) {
      Ok(metadata) => Some(metadata),
      Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
        None
      }
      Err(error) => return Err(error.into()),
    };

  if let Some(destination_metadata) = destination_metadata {
    if source_metadata.is_dir()
      && !source_metadata.file_type().is_symlink()
      && destination_metadata.is_dir()
      && !destination_metadata.file_type().is_symlink()
    {
      let source_dir =
        source_parent.open_dir_nofollow(source_name)?;
      let destination_dir =
        destination_parent.open_dir_nofollow(destination_name)?;
      let mut children = source_dir
        .entries()?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<std::io::Result<Vec<_>>>()?;
      children.sort();
      for child in children {
        let child_name = child
          .to_str()
          .context("Non-UTF-8 filenames are unsupported")?;
        publish_staged_entry(
          &source_dir,
          &child,
          &destination_dir,
          &child,
          &format!("{destination_path}/{child_name}"),
          rollback_dir,
          backup_index,
          decisions,
          progress,
          rollback,
        )?;
      }
      return Ok(());
    }

    match decision_for(destination_path, decisions) {
      Some(FileManagerConflictAction::Skip) => return Ok(()),
      Some(FileManagerConflictAction::Overwrite) => {
        if let Some(progress) = progress {
          progress.add_temporary_storage_bytes(
            super::work_for_entry(
              destination_parent,
              destination_name,
              &destination_metadata,
              0,
            )?
            .bytes,
          );
        }
        let backup_name = OsString::from(backup_index.to_string());
        *backup_index = backup_index.saturating_add(1);
        rename_with_rollback(
          destination_parent,
          destination_name,
          rollback_dir,
          &backup_name,
          rollback,
        )?;
      }
      None => {
        return Err(
          LatePublishConflict(FileManagerConflict {
            path: destination_path.to_string(),
            existing_kind: super::entry_kind(&destination_metadata),
            incoming_kind: super::entry_kind(&source_metadata),
          })
          .into(),
        );
      }
    }
  }

  rename_with_rollback(
    source_parent,
    source_name,
    destination_parent,
    destination_name,
    rollback,
  )
}

fn rename_with_rollback(
  source_parent: &Dir,
  source_name: &OsStr,
  destination_parent: &Dir,
  destination_name: &OsStr,
  rollback: &mut Vec<PublishRollback>,
) -> anyhow::Result<()> {
  source_parent.rename(
    source_name,
    destination_parent,
    destination_name,
  )?;
  rollback.push(PublishRollback {
    from_parent: destination_parent.try_clone()?,
    from_name: destination_name.to_os_string(),
    to_parent: source_parent.try_clone()?,
    to_name: source_name.to_os_string(),
  });
  Ok(())
}

fn rollback_publish(
  rollback: Vec<PublishRollback>,
) -> anyhow::Result<()> {
  for action in rollback.into_iter().rev() {
    action.from_parent.rename(
      &action.from_name,
      &action.to_parent,
      &action.to_name,
    )?;
  }
  Ok(())
}

pub(super) fn extraction_work(
  root: &Dir,
  archive_path: &str,
) -> anyhow::Result<WorkTotal> {
  let archive_path = relative_path(archive_path, false)?;
  let (parent, name) = open_parent_nofollow(root, &archive_path)?;
  let mut options = OpenOptions::new();
  options.read(true).follow(FollowSymlinks::No);
  let source = parent.open_with(name, &options)?.into_std();
  let source_bytes = source.metadata()?.len();
  match detect_format(&source)? {
    DetectedArchive::Zip => {
      let mut archive = ZipArchive::new(source)?;
      ensure_entry_limit(archive.len() as u64)?;
      let mut bytes = 0_u64;
      for index in 0..archive.len() {
        bytes = bytes
          .checked_add(archive.by_index(index)?.size())
          .context("Archive expanded size overflow")?;
      }
      enforce_expansion_limits(bytes, source_bytes.max(1))?;
      Ok(WorkTotal {
        entries: archive.len() as u64,
        bytes,
      })
    }
    DetectedArchive::Tar | DetectedArchive::TarGz => Ok(WorkTotal {
      entries: 0,
      bytes: source_bytes,
    }),
    DetectedArchive::SevenZip => {
      let reader = ArchiveReader::new(source, Password::empty())?;
      ensure_entry_limit(reader.archive().files.len() as u64)?;
      let bytes = reader.archive().files.iter().try_fold(
        0_u64,
        |total, entry| {
          total
            .checked_add(entry.size())
            .context("Archive expanded size overflow")
        },
      )?;
      enforce_expansion_limits(bytes, source_bytes.max(1))?;
      Ok(WorkTotal {
        entries: reader.archive().files.len() as u64,
        bytes,
      })
    }
    DetectedArchive::Rar => {
      Err(anyhow!("RAR extraction is not enabled in this release"))
    }
  }
}

pub(super) fn extraction_capacity_bytes(
  root: &Dir,
  archive_path: &str,
) -> anyhow::Result<u64> {
  let archive_path = relative_path(archive_path, false)?;
  let (parent, name) = open_parent_nofollow(root, &archive_path)?;
  let mut options = OpenOptions::new();
  options.read(true).follow(FollowSymlinks::No);
  let source = parent.open_with(name, &options)?.into_std();
  let source_bytes = source.metadata()?.len();
  match detect_format(&source)? {
    DetectedArchive::Zip | DetectedArchive::SevenZip => {
      extraction_work(root, &path_string(&archive_path)?)
        .map(|work| work.bytes)
    }
    DetectedArchive::Tar | DetectedArchive::TarGz => {
      Ok(source_bytes.saturating_mul(MAX_ARCHIVE_EXPANSION_RATIO))
    }
    DetectedArchive::Rar => {
      Err(anyhow!("RAR extraction is not enabled in this release"))
    }
  }
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
  ensure_entry_limit(*count)?;
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
  ensure_entry_limit(*count)?;
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
  ensure_entry_limit(*count)?;
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
    if let Some(progress) = self.progress {
      progress.check_cancelled().map_err(std::io::Error::other)?;
    }
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

fn detect_format(file: &fs::File) -> anyhow::Result<DetectedArchive> {
  let mut file = file.try_clone()?;
  // Cloned descriptors can share a cursor with the caller. Preserve it so a
  // TAR reader does not begin after this signature probe.
  let position = file.stream_position()?;
  file.rewind()?;
  let mut header = [0_u8; 512];
  let read = file.read(&mut header)?;
  file.seek(std::io::SeekFrom::Start(position))?;
  if read >= 6 && header[..6] == [0x37, 0x7a, 0xbc, 0xaf, 0x27, 0x1c]
  {
    Ok(DetectedArchive::SevenZip)
  } else if read >= 4 && &header[..2] == b"PK" {
    Ok(DetectedArchive::Zip)
  } else if read >= 2 && header[..2] == [0x1f, 0x8b] {
    Ok(DetectedArchive::TarGz)
  } else if (read >= 7 && &header[..7] == b"Rar!\x1a\x07\x00")
    || (read >= 8 && &header[..8] == b"Rar!\x1a\x07\x01\x00")
  {
    Ok(DetectedArchive::Rar)
  } else if read >= 262 && &header[257..262] == b"ustar" {
    Ok(DetectedArchive::Tar)
  } else {
    Err(anyhow!("Archive signature is unsupported"))
  }
}

fn extract_zip(
  source: fs::File,
  destination: &Path,
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  let compressed_size = source.metadata()?.len().max(1);
  let mut archive = ZipArchive::new(source)?;
  let mut expanded = 0_u64;
  ensure_entry_limit(archive.len() as u64)?;
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
      ensure_free_space(
        destination,
        entry.size().saturating_add(MINIMUM_FREE_BYTES),
      )?;
      let copied = copy_extracted_entry(
        &mut entry,
        &mut file,
        destination,
        progress,
        true,
      )?;
      if copied != entry.size() {
        return Err(anyhow!(
          "Archive entry size changed during extraction"
        ));
      }
    }
    if let Some(progress) = progress {
      progress.add_entry();
    }
  }
  Ok(())
}

fn extract_tar(
  source: fs::File,
  destination: &Path,
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  let compressed_size = source.metadata()?.len().max(1);
  extract_tar_reader(
    ProgressReader::new(source, progress),
    compressed_size,
    destination,
    progress,
  )
}

fn extract_tar_gz(
  source: fs::File,
  destination: &Path,
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  let compressed_size = source.metadata()?.len().max(1);
  extract_tar_reader(
    GzDecoder::new(ProgressReader::new(source, progress)),
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
    ensure_entry_limit(count)?;
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
      ensure_free_space(
        destination,
        size.saturating_add(MINIMUM_FREE_BYTES),
      )?;
      let copied = copy_extracted_entry(
        &mut entry,
        &mut file,
        destination,
        progress,
        false,
      )?;
      if copied != size {
        return Err(anyhow!(
          "Archive entry size changed during extraction"
        ));
      }
    }
    if let Some(progress) = progress {
      progress.add_entry();
    }
  }
  Ok(())
}

fn extract_seven_zip(
  source: fs::File,
  destination: &Path,
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  let compressed_size = source.metadata()?.len().max(1);
  let mut count = 0_u64;
  let mut expanded = 0_u64;
  let mut archive = ArchiveReader::new(source, Password::empty())?;
  archive.set_thread_count(
    std::thread::available_parallelism()
      .map(|parallelism| parallelism.get())
      .unwrap_or(1)
      .min(2) as u32,
  );
  archive.for_each_entries(|entry, reader| {
    count += 1;
    ensure_entry_limit(count).map_err(|error| {
      sevenz_rust2::Error::Other(Cow::Owned(error.to_string()))
    })?;
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
    if let Some(progress) = progress {
      progress.check_cancelled().map_err(|error| {
        sevenz_rust2::Error::Other(Cow::Owned(error.to_string()))
      })?;
    }
    ensure_free_space(
      destination,
      entry.size().saturating_add(MINIMUM_FREE_BYTES),
    )
    .map_err(|error| {
      sevenz_rust2::Error::Other(Cow::Owned(error.to_string()))
    })?;
    let output = destination.join(
      archive_relative(entry.name()).map_err(|error| {
        sevenz_rust2::Error::Other(Cow::Owned(error.to_string()))
      })?,
    );
    if entry.is_directory() {
      fs::create_dir_all(&output).map_err(|error| {
        sevenz_rust2::Error::Other(Cow::Owned(error.to_string()))
      })?;
    } else {
      if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).map_err(|error| {
          sevenz_rust2::Error::Other(Cow::Owned(error.to_string()))
        })?;
      }
      let mut file = create_safe_file(&output).map_err(|error| {
        sevenz_rust2::Error::Other(Cow::Owned(error.to_string()))
      })?;
      let copied = copy_extracted_entry(
        reader,
        &mut file,
        destination,
        progress,
        true,
      )
      .map_err(|error| {
        sevenz_rust2::Error::Other(Cow::Owned(error.to_string()))
      })?;
      if copied != entry.size() {
        return Err(sevenz_rust2::Error::Other(Cow::Borrowed(
          "Archive entry size changed during extraction",
        )));
      }
    }
    if let Some(progress) = progress {
      progress.add_entry();
    }
    Ok(true)
  })?;
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
  if expanded
    > compressed
      .max(1)
      .saturating_mul(MAX_ARCHIVE_EXPANSION_RATIO)
  {
    return Err(anyhow!("Archive exceeds the expansion-ratio limit"));
  }
  Ok(())
}

fn copy_extracted_entry(
  source: &mut (impl Read + ?Sized),
  destination: &mut fs::File,
  staging: &Path,
  progress: Option<&OperationProgress>,
  count_bytes: bool,
) -> anyhow::Result<u64> {
  let mut copied = 0_u64;
  let mut buffer = [0_u8; 256 * 1024];
  loop {
    if let Some(progress) = progress {
      progress.check_cancelled()?;
    }
    let read = source.read(&mut buffer)?;
    if read == 0 {
      break;
    }
    ensure_free_space(
      staging,
      (read as u64).saturating_add(MINIMUM_FREE_BYTES),
    )?;
    destination.write_all(&buffer[..read])?;
    copied = copied.saturating_add(read as u64);
    if count_bytes && let Some(progress) = progress {
      progress.add_bytes(read as u64);
    }
  }
  Ok(copied)
}

fn host_bytes(path: &Path) -> anyhow::Result<u64> {
  let metadata = fs::symlink_metadata(path)?;
  if metadata.is_file() {
    return Ok(metadata.len());
  }
  let mut total = 0_u64;
  for entry in fs::read_dir(path)? {
    total = total.saturating_add(host_bytes(&entry?.path())?);
  }
  Ok(total)
}

fn sync_staged_tree(path: &Path) -> anyhow::Result<()> {
  #[cfg(unix)]
  {
    use std::os::fd::AsRawFd as _;
    let directory = fs::File::open(path)?;
    let result = unsafe { libc::syncfs(directory.as_raw_fd()) };
    if result == 0 {
      return Ok(());
    }
  }

  fn sync_fallback(path: &Path) -> anyhow::Result<()> {
    for entry in fs::read_dir(path)? {
      let entry = entry?;
      if entry.file_type()?.is_dir() {
        sync_fallback(&entry.path())?;
      } else {
        fs::File::open(entry.path())?.sync_all()?;
      }
    }
    fs::File::open(path)?.sync_all()?;
    Ok(())
  }

  sync_fallback(path)
}

fn validate_staged_tree(root: &Path) -> anyhow::Result<()> {
  fn visit(path: &Path, count: &mut u64) -> anyhow::Result<()> {
    for entry in fs::read_dir(path)? {
      *count += 1;
      ensure_entry_limit(*count)?;
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
      detect_format(&fs::File::open(&path).unwrap()).unwrap(),
      DetectedArchive::Rar
    ));
    fs::remove_dir_all(directory).unwrap();
  }

  #[test]
  fn signature_detection_preserves_the_source_position() {
    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    fs::create_dir(&directory).unwrap();
    let path = directory.join("archive.zip");
    let file = fs::File::create(&path).unwrap();
    let mut archive = ZipWriter::new(file);
    archive
      .start_file("file.txt", SimpleFileOptions::default())
      .unwrap();
    archive.write_all(b"contents").unwrap();
    archive.finish().unwrap().sync_all().unwrap();

    let mut source = fs::File::open(path).unwrap();
    source.seek(std::io::SeekFrom::Start(3)).unwrap();
    assert!(matches!(
      detect_format(&source),
      Ok(DetectedArchive::Zip)
    ));
    assert_eq!(source.stream_position().unwrap(), 3);

    fs::remove_dir_all(directory).unwrap();
  }

  #[test]
  fn extraction_publish_rolls_back_same_filesystem_renames() {
    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    fs::create_dir_all(directory.join("staging")).unwrap();
    fs::create_dir_all(directory.join("destination")).unwrap();
    fs::create_dir_all(directory.join("rollback")).unwrap();
    fs::write(directory.join("staging/a-new.txt"), b"new").unwrap();
    fs::write(directory.join("staging/z-conflict.txt"), b"incoming")
      .unwrap();
    fs::write(
      directory.join("destination/z-conflict.txt"),
      b"existing",
    )
    .unwrap();
    let root =
      Dir::open_ambient_dir(&directory, cap_std::ambient_authority())
        .unwrap();
    let rollback_dir = root.open_dir_nofollow("rollback").unwrap();
    let mut rollback = Vec::new();
    let mut backup_index = 0;

    let error = publish_staged_entry(
      &root,
      OsStr::new("staging"),
      &root,
      OsStr::new("destination"),
      "destination",
      &rollback_dir,
      &mut backup_index,
      &[],
      None,
      &mut rollback,
    )
    .unwrap_err();
    assert!(error.downcast_ref::<LatePublishConflict>().is_some());
    assert_eq!(
      fs::read(directory.join("destination/a-new.txt")).unwrap(),
      b"new"
    );

    rollback_publish(rollback).unwrap();
    assert!(!directory.join("destination/a-new.txt").exists());
    assert_eq!(
      fs::read(directory.join("staging/a-new.txt")).unwrap(),
      b"new"
    );
    assert_eq!(
      fs::read(directory.join("destination/z-conflict.txt")).unwrap(),
      b"existing"
    );

    fs::remove_dir_all(directory).unwrap();
  }

  #[test]
  fn extraction_publish_overwrites_only_the_selected_conflict() {
    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    fs::create_dir_all(directory.join("staging")).unwrap();
    fs::create_dir_all(directory.join("destination")).unwrap();
    fs::create_dir_all(directory.join("rollback")).unwrap();
    fs::write(directory.join("staging/overwrite.txt"), b"incoming")
      .unwrap();
    fs::write(directory.join("staging/skip.txt"), b"incoming")
      .unwrap();
    fs::write(
      directory.join("destination/overwrite.txt"),
      b"existing",
    )
    .unwrap();
    fs::write(directory.join("destination/skip.txt"), b"existing")
      .unwrap();
    let root =
      Dir::open_ambient_dir(&directory, cap_std::ambient_authority())
        .unwrap();
    let rollback_dir = root.open_dir_nofollow("rollback").unwrap();
    let decisions = [
      FileManagerConflictDecision {
        path: "destination/overwrite.txt".into(),
        action: FileManagerConflictAction::Overwrite,
      },
      FileManagerConflictDecision {
        path: "destination/skip.txt".into(),
        action: FileManagerConflictAction::Skip,
      },
    ];

    publish_staged_entry(
      &root,
      OsStr::new("staging"),
      &root,
      OsStr::new("destination"),
      "destination",
      &rollback_dir,
      &mut 0,
      &decisions,
      None,
      &mut Vec::new(),
    )
    .unwrap();

    assert_eq!(
      fs::read(directory.join("destination/overwrite.txt")).unwrap(),
      b"incoming"
    );
    assert_eq!(
      fs::read(directory.join("destination/skip.txt")).unwrap(),
      b"existing"
    );
    assert_eq!(
      fs::read(directory.join("staging/skip.txt")).unwrap(),
      b"incoming"
    );
    fs::remove_dir_all(directory).unwrap();
  }

  #[test]
  fn archive_formats_use_phase_specific_progress_denominators() {
    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    fs::create_dir(&directory).unwrap();
    let payload =
      (0_u8..=255).cycle().take(64 * 1024).collect::<Vec<_>>();
    fs::write(directory.join("payload.bin"), &payload).unwrap();

    let zip_path = directory.join("archive.zip");
    let mut zip =
      ZipWriter::new(fs::File::create(&zip_path).unwrap());
    zip
      .start_file(
        "payload.bin",
        SimpleFileOptions::default()
          .compression_method(CompressionMethod::Deflated),
      )
      .unwrap();
    zip.write_all(&payload).unwrap();
    zip.finish().unwrap().sync_all().unwrap();

    let tar_path = directory.join("archive.tar");
    let mut tar =
      TarBuilder::new(fs::File::create(&tar_path).unwrap());
    let mut header = Header::new_gnu();
    header.set_size(payload.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    tar
      .append_data(&mut header, "payload.bin", payload.as_slice())
      .unwrap();
    tar.into_inner().unwrap().sync_all().unwrap();

    let tar_gz_path = directory.join("archive.tar.gz");
    let encoder = GzEncoder::new(
      fs::File::create(&tar_gz_path).unwrap(),
      Compression::default(),
    );
    let mut tar_gz = TarBuilder::new(encoder);
    let mut header = Header::new_gnu();
    header.set_size(payload.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    tar_gz
      .append_data(&mut header, "payload.bin", payload.as_slice())
      .unwrap();
    tar_gz
      .into_inner()
      .unwrap()
      .finish()
      .unwrap()
      .sync_all()
      .unwrap();

    let root =
      Dir::open_ambient_dir(&directory, cap_std::ambient_authority())
        .unwrap();
    let mut options = OpenOptions::new();
    options
      .write(true)
      .read(true)
      .create_new(true)
      .follow(FollowSymlinks::No);
    let seven_zip_file =
      root.open_with("archive.7z", &options).unwrap();
    create_seven_zip(
      &root,
      &["payload.bin".into()],
      seven_zip_file,
      None,
    )
    .unwrap();

    let zip_work = extraction_work(&root, "archive.zip").unwrap();
    assert_eq!(zip_work.entries, 1);
    assert_eq!(zip_work.bytes, payload.len() as u64);

    let tar_work = extraction_work(&root, "archive.tar").unwrap();
    assert_eq!(tar_work.entries, 0);
    assert_eq!(
      tar_work.bytes,
      fs::metadata(&tar_path).unwrap().len()
    );

    let tar_gz_work =
      extraction_work(&root, "archive.tar.gz").unwrap();
    assert_eq!(tar_gz_work.entries, 0);
    assert_eq!(
      tar_gz_work.bytes,
      fs::metadata(&tar_gz_path).unwrap().len()
    );
    assert!(tar_gz_work.bytes < payload.len() as u64);

    let seven_zip_work =
      extraction_work(&root, "archive.7z").unwrap();
    assert_eq!(seven_zip_work.entries, 1);
    assert_eq!(seven_zip_work.bytes, payload.len() as u64);

    for (name, work) in [
      ("archive.zip", zip_work),
      ("archive.tar", tar_work),
      ("archive.tar.gz", tar_gz_work),
      ("archive.7z", seven_zip_work),
    ] {
      let destination = directory.join(format!("extract-{name}"));
      fs::create_dir(&destination).unwrap();
      let progress = OperationProgress::new(
        format!("progress-{name}"),
        "Extract archive".into(),
      );
      progress.phase(
        super::super::FileManagerOperationPhase::Applying,
        work,
      );
      let source = fs::File::open(directory.join(name)).unwrap();
      match name {
        "archive.zip" => {
          extract_zip(source, &destination, Some(&progress))
        }
        "archive.tar" => {
          extract_tar(source, &destination, Some(&progress))
        }
        "archive.tar.gz" => {
          extract_tar_gz(source, &destination, Some(&progress))
        }
        "archive.7z" => {
          extract_seven_zip(source, &destination, Some(&progress))
        }
        _ => unreachable!(),
      }
      .unwrap();
      let status = progress.snapshot();
      assert!(status.completed_bytes > 0, "no progress for {name}");
      assert!(
        status.completed_bytes <= status.total_bytes,
        "progress exceeded phase total for {name}"
      );
      assert_eq!(
        fs::read(destination.join("payload.bin")).unwrap(),
        payload
      );
    }

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

    extract_zip(
      fs::File::open(&archive_path).unwrap(),
      &extracted,
      None,
    )
    .unwrap();
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

    assert!(
      extract_zip(
        fs::File::open(&archive_path).unwrap(),
        &extracted,
        None,
      )
      .is_err()
    );
    assert!(!directory.join("escape.txt").exists());
    fs::remove_dir_all(directory).unwrap();
  }

  #[test]
  fn expansion_ratio_is_exact_and_has_no_fixed_byte_ceiling() {
    let compressed = 11_u64 * 1024 * 1024;
    let expanded = compressed * MAX_ARCHIVE_EXPANSION_RATIO;
    assert!(enforce_expansion_limits(expanded, compressed).is_ok());
    assert!(
      enforce_expansion_limits(expanded + 1, compressed).is_err()
    );
    assert!(expanded > 10 * 1024 * 1024 * 1024);
  }

  #[test]
  fn zip_progress_is_declared_expanded_work_and_staging_is_cleaned() {
    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    fs::create_dir_all(directory.join("destination")).unwrap();
    fs::write(directory.join("destination/existing.txt"), b"keep")
      .unwrap();
    let archive_path = directory.join("archive.zip");
    let file = fs::File::create(&archive_path).unwrap();
    let mut archive = ZipWriter::new(file);
    archive
      .start_file("folder/config.txt", SimpleFileOptions::default())
      .unwrap();
    archive.write_all(b"safe contents").unwrap();
    archive.finish().unwrap();

    let root =
      Dir::open_ambient_dir(&directory, cap_std::ambient_authority())
        .unwrap();
    let total = extraction_work(&root, "archive.zip").unwrap();
    assert_eq!(total.entries, 1);
    assert_eq!(total.bytes, b"safe contents".len() as u64);
    let progress = OperationProgress::new(
      "extract-test".into(),
      "Extract archive".into(),
    );
    progress.phase(
      super::super::FileManagerOperationPhase::Applying,
      total,
    );
    extract(
      &root,
      &directory,
      "archive.zip",
      "destination",
      &[],
      Some(&progress),
    )
    .unwrap();
    let status = progress.snapshot();
    assert_eq!(status.completed_bytes, status.total_bytes);
    assert!(status.completed_entries <= status.total_entries);
    assert_eq!(
      fs::read(directory.join("destination/folder/config.txt"))
        .unwrap(),
      b"safe contents"
    );
    assert_eq!(
      fs::read(directory.join("destination/existing.txt")).unwrap(),
      b"keep"
    );
    assert!(
      fs::read_dir(directory.join("destination")).unwrap().all(
        |entry| {
          !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".komodo-file-manager-staging-")
        }
      )
    );
    fs::remove_dir_all(directory).unwrap();
  }
}
