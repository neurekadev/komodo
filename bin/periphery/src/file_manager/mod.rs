use std::{
  collections::{HashMap, HashSet},
  fs,
  io::{Read as _, Write as _},
  path::{Component, Path, PathBuf},
  sync::{Arc, OnceLock},
  time::UNIX_EPOCH,
};

use anyhow::{Context, anyhow};
use cap_fs_ext::{
  DirExt as _, FollowSymlinks, MetadataExt as _,
  OpenOptionsFollowExt as _,
};
use cap_std::{
  ambient_authority,
  fs::{Dir, Metadata, OpenOptions},
};
use encoding::{Decode as _, WithChannel};
use komodo_client::entities::{
  file_manager::{
    FileManagerArchiveFormat, FileManagerCapabilities,
    FileManagerConflict, FileManagerConflictAction,
    FileManagerConflictDecision, FileManagerDirectory,
    FileManagerEntry, FileManagerEntryKind, FileManagerJournalStatus,
    FileManagerLimits, FileManagerOperation,
    FileManagerOperationState, FileManagerOperationStatus,
    FileManagerPreflight, FileManagerRevision, FileManagerTextFile,
  },
  komodo_timestamp, to_path_compatible_name,
};
use periphery_client::api::file_manager::{
  PeripheryFileManagerTarget, StartFileManagerUpload,
};
use periphery_client::{
  api::file_manager::StartFileManagerDownloadResponse,
  transport::{EncodedFileTransferMessage, FileTransferMessage},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
  config::periphery_config,
  state::{core_connections, docker_client, file_transfer_channels},
};

mod archive;
mod path;

use path::{
  open_dir_nofollow, open_parent_nofollow, relative_path, single_name,
};

pub const MAX_TEXT_BYTES: u64 = 4 * 1024 * 1024;
pub const MAX_ENTRIES: u64 = 100_000;
pub const MAX_ARCHIVE_EXPANDED_BYTES: u64 = 10 * 1024 * 1024 * 1024;
pub const MAX_ARCHIVE_EXPANSION_RATIO: u64 = 1_000;
pub const MINIMUM_FREE_BYTES: u64 = 256 * 1024 * 1024;
const PLAN_TTL_MS: i64 = 5 * 60 * 1_000;
const JOURNAL_TTL_MS: i64 = 24 * 60 * 60 * 1_000;

#[derive(Debug, Clone)]
pub struct ResolvedRoot {
  pub path: PathBuf,
  pub key: String,
  pub read_only: bool,
  pub managed_file: Option<String>,
  pub create_if_missing: bool,
}

#[derive(Debug, Clone)]
struct OperationPlan {
  actor: String,
  root_key: String,
  operation: FileManagerOperation,
  expires_at: i64,
  conflicts: Vec<FileManagerConflict>,
  confirmation_required: bool,
  revisions: Vec<(String, Option<FileManagerRevision>)>,
  copy_targets: Vec<CopyTarget>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CopyTarget {
  source: String,
  destination: String,
}

#[derive(Debug, Clone)]
struct OperationStatusRecord {
  actor: String,
  root_key: String,
  expires_at: i64,
  status: FileManagerOperationStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JournalRecord {
  id: String,
  actor: String,
  root_key: String,
  root_path: PathBuf,
  #[serde(default)]
  managed: bool,
  created_at: i64,
  expires_at: i64,
  description: String,
  snapshots: Vec<JournalSnapshot>,
  #[serde(default)]
  before_revisions: Vec<(String, Option<FileManagerRevision>)>,
  after_revisions: Vec<(String, Option<FileManagerRevision>)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JournalSnapshot {
  path: String,
  existed: bool,
  backup_name: String,
}

#[derive(Debug, Default)]
struct JournalHistory {
  undo: Vec<JournalRecord>,
  redo: Vec<JournalRecord>,
}

struct TemporaryUpload<'a> {
  parent: &'a Dir,
  name: String,
  file: Option<cap_std::fs::File>,
  committed: bool,
}

impl Drop for TemporaryUpload<'_> {
  fn drop(&mut self) {
    self.file.take();
    if !self.committed {
      let _ = self.parent.remove_file(&self.name);
    }
  }
}

fn plans() -> &'static Mutex<HashMap<String, OperationPlan>> {
  static PLANS: OnceLock<Mutex<HashMap<String, OperationPlan>>> =
    OnceLock::new();
  PLANS.get_or_init(Default::default)
}

fn statuses() -> &'static Mutex<HashMap<String, OperationStatusRecord>>
{
  static STATUSES: OnceLock<
    Mutex<HashMap<String, OperationStatusRecord>>,
  > = OnceLock::new();
  STATUSES.get_or_init(Default::default)
}

fn histories() -> &'static Mutex<HashMap<String, JournalHistory>> {
  static HISTORIES: OnceLock<Mutex<HashMap<String, JournalHistory>>> =
    OnceLock::new();
  HISTORIES.get_or_init(Default::default)
}

fn root_locks() -> &'static Mutex<HashMap<String, Arc<Mutex<()>>>> {
  static LOCKS: OnceLock<Mutex<HashMap<String, Arc<Mutex<()>>>>> =
    OnceLock::new();
  LOCKS.get_or_init(Default::default)
}

async fn root_lock(key: &str) -> Arc<Mutex<()>> {
  let mut locks = root_locks().lock().await;
  locks
    .entry(key.to_string())
    .or_insert_with(|| Arc::new(Mutex::new(())))
    .clone()
}

pub async fn initialize() -> anyhow::Result<()> {
  let root = journal_root();
  fs::create_dir_all(&root)?;
  let _ = fs::remove_dir_all(root.join("transfers"));
  let mut records = Vec::new();
  for entry in fs::read_dir(&root)? {
    let entry = entry?;
    if !entry.file_type()?.is_dir() {
      continue;
    }
    let manifest = entry.path().join("manifest.json");
    let record = fs::read(&manifest).ok().and_then(|manifest| {
      serde_json::from_slice::<JournalRecord>(&manifest).ok()
    });
    match record {
      Some(record)
        if !record.managed
          && record.expires_at > komodo_timestamp() =>
      {
        records.push(record);
      }
      _ => {
        let _ = fs::remove_dir_all(entry.path());
      }
    }
  }
  records.sort_by_key(|record| record.created_at);
  let mut loaded = histories().lock().await;
  for record in records {
    loaded
      .entry(history_key(&record.root_key, &record.actor))
      .or_default()
      .undo
      .push(record);
  }
  drop(loaded);

  tokio::spawn(async {
    let mut interval =
      tokio::time::interval(std::time::Duration::from_secs(60 * 60));
    interval.tick().await;
    loop {
      interval.tick().await;
      let mut histories = histories().lock().await;
      for history in histories.values_mut() {
        prune_history(history);
      }
    }
  });
  Ok(())
}

pub fn limits() -> FileManagerLimits {
  FileManagerLimits {
    max_text_bytes: MAX_TEXT_BYTES,
    max_entries: MAX_ENTRIES,
    max_depth: path::MAX_DEPTH as u64,
    max_archive_expanded_bytes: MAX_ARCHIVE_EXPANDED_BYTES,
    max_archive_expansion_ratio: MAX_ARCHIVE_EXPANSION_RATIO,
    minimum_free_bytes: MINIMUM_FREE_BYTES,
  }
}

fn configured_path(base: impl AsRef<Path>, value: &str) -> PathBuf {
  let path = base.as_ref().join(value);
  let mut normalized = PathBuf::new();
  for component in path.components() {
    match component {
      Component::CurDir => {}
      Component::Normal(value) => normalized.push(value),
      Component::ParentDir => {
        normalized.pop();
      }
      Component::RootDir | Component::Prefix(_) => {
        normalized.push(component.as_os_str());
      }
    }
  }
  normalized
}

pub async fn resolve_root(
  target: &PeripheryFileManagerTarget,
) -> anyhow::Result<ResolvedRoot> {
  let (path, read_only, managed_file, create_if_missing) =
    match target {
      PeripheryFileManagerTarget::Stack { stack, repo } => {
        if !stack.config.swarm_id.is_empty() {
          return Err(anyhow!(
            "File Manager is unavailable for Swarm stacks"
          ));
        }

        let stack_name = to_path_compatible_name(&stack.name);
        let (run_root, read_only, managed) =
          if stack.config.files_on_host {
            (
              configured_path(
                periphery_config().stack_dir().join(stack_name),
                &stack.config.run_directory,
              ),
              false,
              false,
            )
          } else if let Some(repo) = repo {
            (
              configured_path(
                configured_path(
                  periphery_config()
                    .repo_dir()
                    .join(to_path_compatible_name(&repo.name)),
                  &repo.config.path,
                ),
                &stack.config.run_directory,
              ),
              true,
              false,
            )
          } else if !stack.config.repo.is_empty() {
            (
              configured_path(
                configured_path(
                  periphery_config().stack_dir().join(stack_name),
                  &stack.config.clone_path,
                ),
                &stack.config.run_directory,
              ),
              true,
              false,
            )
          } else {
            (
              periphery_config().stack_dir().join(stack_name),
              false,
              true,
            )
          };

        let compose = stack
          .config
          .file_paths
          .first()
          .map(String::as_str)
          .unwrap_or("compose.yaml");
        let compose = configured_path(&run_root, compose);
        let name = compose
          .file_name()
          .and_then(|name| name.to_str())
          .context("Compose path must end in a UTF-8 filename")?
          .to_string();
        let root = compose
          .parent()
          .context("Compose path must have a parent directory")?
          .to_path_buf();
        (root, read_only, managed.then_some(name), managed)
      }
      PeripheryFileManagerTarget::Volume { volume } => {
        let client = docker_client().load();
        let client = client
          .iter()
          .next()
          .context("Could not connect to Docker")?;
        let volume = client.inspect_volume(volume).await?;
        if volume.mountpoint.is_empty() {
          return Err(anyhow!(
            "Docker did not report a volume mountpoint"
          ));
        }
        (PathBuf::from(volume.mountpoint), false, None, false)
      }
    };

  let private_journal = journal_root();
  if path.starts_with(&private_journal)
    || private_journal.starts_with(&path)
  {
    return Err(anyhow!(
      "File Manager root overlaps its private journal"
    ));
  }

  let mut hash = Sha256::new();
  hash.update(path.as_os_str().as_encoded_bytes());
  let key = hex::encode(hash.finalize());
  Ok(ResolvedRoot {
    path,
    key,
    read_only,
    managed_file,
    create_if_missing,
  })
}

fn journal_root() -> PathBuf {
  periphery_config()
    .root_directory
    .join("file-manager-journal")
}

fn open_root(
  root: &ResolvedRoot,
  create: bool,
) -> anyhow::Result<Dir> {
  if create && root.create_if_missing && !root.path.exists() {
    fs::create_dir_all(&root.path)
      .context("Failed to create managed File Manager root")?;
  }
  Dir::open_ambient_dir(&root.path, ambient_authority())
    .context("File Manager root is inaccessible")
}

pub async fn capabilities(
  target: &PeripheryFileManagerTarget,
) -> FileManagerCapabilities {
  match resolve_root(target).await {
    Ok(root)
      if root.path.is_dir()
        || (root.create_if_missing && !root.path.exists()) =>
    {
      FileManagerCapabilities {
        available: true,
        read_only: root.read_only,
        reason: None,
        managed_file: root.managed_file,
        limits: limits(),
      }
    }
    Ok(_) => FileManagerCapabilities {
      available: false,
      read_only: true,
      reason: Some(
        "File Manager root is not an accessible directory".into(),
      ),
      managed_file: None,
      limits: limits(),
    },
    Err(error) => FileManagerCapabilities {
      available: false,
      read_only: true,
      reason: Some(error.to_string()),
      managed_file: None,
      limits: limits(),
    },
  }
}

pub async fn list_directory(
  target: &PeripheryFileManagerTarget,
  path: &str,
) -> anyhow::Result<FileManagerDirectory> {
  let root = resolve_root(target).await?;
  let relative = relative_path(path, true)?;
  let lock = root_lock(&root.key).await;
  let _guard = lock.lock().await;
  if root.create_if_missing && !root.path.exists() {
    return Ok(FileManagerDirectory {
      path: path.to_string(),
      entries: Vec::new(),
    });
  }
  let root_dir = open_root(&root, false)?;
  let dir = open_dir_nofollow(&root_dir, &relative)?;
  let mut entries = Vec::new();
  for entry in dir.entries()? {
    if entries.len() as u64 >= MAX_ENTRIES {
      return Err(anyhow!("Directory exceeds the entry limit"));
    }
    let entry = entry?;
    let name = entry
      .file_name()
      .into_string()
      .map_err(|_| anyhow!("Non-UTF-8 filenames are unsupported"))?;
    let metadata = dir.symlink_metadata(&name)?;
    let kind = entry_kind(&metadata);
    let entry_path = if path.is_empty() {
      name.clone()
    } else {
      format!("{path}/{name}")
    };
    entries.push(FileManagerEntry {
      managed: root.managed_file.as_deref()
        == Some(entry_path.as_str()),
      path: entry_path,
      name,
      kind,
      size: metadata.len(),
      modified_at: modified_at(&metadata),
      revision: revision(&metadata),
    });
  }
  entries.sort_by(|a, b| {
    (
      a.kind != FileManagerEntryKind::Directory,
      a.name.to_lowercase(),
    )
      .cmp(&(
        b.kind != FileManagerEntryKind::Directory,
        b.name.to_lowercase(),
      ))
  });
  Ok(FileManagerDirectory {
    path: path.to_string(),
    entries,
  })
}

pub async fn read_text(
  target: &PeripheryFileManagerTarget,
  path: &str,
) -> anyhow::Result<FileManagerTextFile> {
  let root = resolve_root(target).await?;
  let relative = relative_path(path, false)?;
  let lock = root_lock(&root.key).await;
  let _guard = lock.lock().await;
  let root_dir = open_root(&root, false)?;
  let (parent, name) = open_parent_nofollow(&root_dir, &relative)?;
  let metadata = parent.symlink_metadata(&name)?;
  if !metadata.is_file() {
    return Err(anyhow!("Path is not a regular file"));
  }
  if metadata.len() > MAX_TEXT_BYTES {
    return Err(anyhow!(
      "File is too large for the editor; download it instead"
    ));
  }
  let mut options = OpenOptions::new();
  options.read(true).follow(FollowSymlinks::No);
  let mut file = parent.open_with(&name, &options)?;
  let mut bytes = Vec::with_capacity(metadata.len() as usize);
  file.read_to_end(&mut bytes)?;
  if bytes.contains(&0) {
    return Err(anyhow!(
      "Binary files cannot be opened in the editor"
    ));
  }
  let contents = String::from_utf8(bytes)
    .context("File is not valid UTF-8; download it instead")?;
  Ok(FileManagerTextFile {
    path: path.to_string(),
    revision: content_revision(&metadata, contents.as_bytes()),
    contents,
  })
}

pub async fn preflight(
  target: &PeripheryFileManagerTarget,
  actor: String,
  operation: FileManagerOperation,
) -> anyhow::Result<FileManagerPreflight> {
  let root = resolve_root(target).await?;
  if root.read_only {
    return Err(anyhow!("This File Manager root is read-only"));
  }
  let operation = normalize_operation(operation);
  validate_operation(&root, &operation)?;
  let lock = root_lock(&root.key).await;
  let _guard = lock.lock().await;
  let root_dir = match open_root(&root, false) {
    Ok(root) => Some(root),
    Err(_) if root.create_if_missing => None,
    Err(error) => return Err(error),
  };
  let root_dir = root_dir.as_ref();
  let copy_targets = resolve_copy_targets(root_dir, &operation)?;
  let conflicts =
    find_conflicts_planned(root_dir, &operation, &copy_targets)?;
  let mut watched =
    revision_paths_planned(&operation, &copy_targets)?;
  watched.sort();
  watched.dedup();
  let revisions = watched
    .into_iter()
    .map(|path| {
      let rev = root_dir
        .map(|root| tree_revision(root, &path))
        .transpose()?
        .flatten();
      anyhow::Ok((path, rev))
    })
    .collect::<anyhow::Result<Vec<_>>>()?;
  let plan_id = Uuid::new_v4().to_string();
  let expires_at = komodo_timestamp() + PLAN_TTL_MS;
  let confirmation_required =
    operation.requires_confirmation() || !conflicts.is_empty();
  let mut plans = plans().lock().await;
  plans.retain(|_, plan| plan.expires_at > komodo_timestamp());
  plans.insert(
    plan_id.clone(),
    OperationPlan {
      actor,
      root_key: root.key,
      operation,
      expires_at,
      conflicts: conflicts.clone(),
      confirmation_required,
      revisions,
      copy_targets,
    },
  );
  Ok(FileManagerPreflight {
    plan_id,
    expires_at,
    conflicts,
    confirmation_required,
  })
}

pub async fn commit(
  target: &PeripheryFileManagerTarget,
  actor: &str,
  plan_id: &str,
  decisions: &[FileManagerConflictDecision],
  confirmed: bool,
) -> anyhow::Result<
  periphery_client::api::file_manager::FileManagerCommitResponse,
> {
  let plan = plans().lock().await.remove(plan_id).context(
    "Preflight plan is missing, expired, or already consumed",
  )?;
  if plan.actor != actor {
    return Err(anyhow!("Preflight plan belongs to another user"));
  }
  if plan.expires_at < komodo_timestamp() {
    return Err(anyhow!("Preflight plan has expired"));
  }
  if plan.confirmation_required && !confirmed {
    return Err(anyhow!("Explicit confirmation is required"));
  }
  validate_conflict_decisions(&plan.conflicts, decisions)?;

  let root = resolve_root(target).await?;
  if root.key != plan.root_key || root.read_only {
    return Err(anyhow!(
      "File Manager target changed after preflight"
    ));
  }
  let lock = root_lock(&root.key).await;
  let _guard = lock.lock().await;
  let root_dir = open_root(&root, true)?;
  for (path, expected) in &plan.revisions {
    let actual = tree_revision(&root_dir, path)?;
    if &actual != expected {
      return Err(anyhow!(
        "File Manager contents changed after preflight; retry the operation"
      ));
    }
  }

  let operation_id = Uuid::new_v4().to_string();
  let now = komodo_timestamp();
  let mut operation_statuses = statuses().lock().await;
  operation_statuses.retain(|_, status| status.expires_at > now);
  operation_statuses.insert(
    operation_id.clone(),
    OperationStatusRecord {
      actor: actor.to_string(),
      root_key: root.key.clone(),
      expires_at: now + JOURNAL_TTL_MS,
      status: FileManagerOperationStatus {
        operation_id: operation_id.clone(),
        state: FileManagerOperationState::Running,
        ..Default::default()
      },
    },
  );
  drop(operation_statuses);

  let journal = create_journal(
    &root,
    actor,
    &operation_id,
    &plan.operation,
    &plan.copy_targets,
  )?;
  let result = apply_operation_planned(
    &root_dir,
    &plan.operation,
    &plan.copy_targets,
    decisions,
  );
  match result {
    Ok(()) => {
      let journal = match finish_journal(&root_dir, journal.clone()) {
        Ok(journal) => journal,
        Err(error) => {
          let _ = restore_journal(&root_dir, &journal);
          let _ =
            fs::remove_dir_all(journal_root().join(&journal.id));
          if let Some(status) =
            statuses().lock().await.get_mut(&operation_id)
          {
            status.status.state = FileManagerOperationState::Failed;
            status.status.error = Some(error.to_string());
          }
          return Err(error);
        }
      };
      push_journal(journal).await?;
      if let Some(status) =
        statuses().lock().await.get_mut(&operation_id)
      {
        status.status.state = FileManagerOperationState::Complete;
      }
      Ok(
        periphery_client::api::file_manager::FileManagerCommitResponse {
          operation_id,
          affected_paths: affected_paths_planned(
            &plan.operation,
            &plan.copy_targets,
          ),
        },
      )
    }
    Err(error) => {
      let _ = restore_journal(&root_dir, &journal);
      let _ = fs::remove_dir_all(journal_root().join(&journal.id));
      if let Some(status) =
        statuses().lock().await.get_mut(&operation_id)
      {
        status.status.state = FileManagerOperationState::Failed;
        status.status.error = Some(error.to_string());
      }
      Err(error)
    }
  }
}

pub async fn operation_status(
  target: &PeripheryFileManagerTarget,
  actor: &str,
  operation_id: &str,
) -> anyhow::Result<FileManagerOperationStatus> {
  let root = resolve_root(target).await?;
  let mut statuses = statuses().lock().await;
  statuses.retain(|_, status| status.expires_at > komodo_timestamp());
  let record = statuses
    .get(operation_id)
    .cloned()
    .context("File Manager operation was not found")?;
  if record.actor != actor || record.root_key != root.key {
    return Err(anyhow!("File Manager operation was not found"));
  }
  Ok(record.status)
}

pub async fn journal_status(
  target: &PeripheryFileManagerTarget,
  actor: &str,
) -> anyhow::Result<FileManagerJournalStatus> {
  let root = resolve_root(target).await?;
  let key = history_key(&root.key, actor);
  let mut histories = histories().lock().await;
  let history = histories.entry(key).or_default();
  prune_history(history);
  Ok(FileManagerJournalStatus {
    can_undo: !history.undo.is_empty(),
    can_redo: !history.redo.is_empty(),
    undo_description: history
      .undo
      .last()
      .map(|r| r.description.clone()),
    redo_description: history
      .redo
      .last()
      .map(|record| record.description.clone()),
    expires_at: history.undo.last().map(|r| r.expires_at),
  })
}

pub async fn undo(
  target: &PeripheryFileManagerTarget,
  actor: &str,
  confirmed: bool,
) -> anyhow::Result<
  periphery_client::api::file_manager::FileManagerCommitResponse,
> {
  if !confirmed {
    return Err(anyhow!("Explicit confirmation is required"));
  }
  let root = resolve_root(target).await?;
  if root.read_only {
    return Err(anyhow!("This File Manager root is read-only"));
  }
  let key = history_key(&root.key, actor);
  let mut record = {
    let mut histories = histories().lock().await;
    let history = histories.entry(key.clone()).or_default();
    prune_history(history);
    history.undo.pop().context("Nothing is available to undo")?
  };
  let lock = root_lock(&root.key).await;
  let _guard = lock.lock().await;
  let result = (|| {
    let root_dir = open_root(&root, true)?;
    verify_revisions(
      &root_dir,
      &record.after_revisions,
      "Undo is unsafe because files changed after the operation",
    )?;
    restore_journal(&root_dir, &record)?;
    record.before_revisions =
      capture_snapshot_revisions(&root_dir, &record.snapshots)?;
    Ok(())
  })();
  if let Err(error) = result {
    histories()
      .lock()
      .await
      .entry(key)
      .or_default()
      .undo
      .push(record);
    return Err(error);
  }
  histories()
    .lock()
    .await
    .entry(key)
    .or_default()
    .redo
    .push(record.clone());
  Ok(
    periphery_client::api::file_manager::FileManagerCommitResponse {
      operation_id: Uuid::new_v4().to_string(),
      affected_paths: record
        .snapshots
        .iter()
        .map(|snapshot| snapshot.path.clone())
        .collect(),
    },
  )
}

pub async fn redo(
  target: &PeripheryFileManagerTarget,
  actor: &str,
  confirmed: bool,
) -> anyhow::Result<
  periphery_client::api::file_manager::FileManagerCommitResponse,
> {
  if !confirmed {
    return Err(anyhow!("Explicit confirmation is required"));
  }
  let root = resolve_root(target).await?;
  if root.read_only {
    return Err(anyhow!("This File Manager root is read-only"));
  }
  let key = history_key(&root.key, actor);
  let mut record = histories()
    .lock()
    .await
    .entry(key.clone())
    .or_default()
    .redo
    .pop()
    .context("Nothing is available to redo")?;
  let lock = root_lock(&root.key).await;
  let _guard = lock.lock().await;
  let result = (|| {
    let root_dir = open_root(&root, true)?;
    verify_revisions(
      &root_dir,
      &record.before_revisions,
      "Redo is unsafe because files changed after undo",
    )?;
    restore_after_journal(&root_dir, &record)?;
    record.after_revisions =
      capture_snapshot_revisions(&root_dir, &record.snapshots)?;
    Ok(())
  })();
  if let Err(error) = result {
    histories()
      .lock()
      .await
      .entry(key)
      .or_default()
      .redo
      .push(record);
    return Err(error);
  }
  histories()
    .lock()
    .await
    .entry(key)
    .or_default()
    .undo
    .push(record.clone());
  Ok(
    periphery_client::api::file_manager::FileManagerCommitResponse {
      operation_id: Uuid::new_v4().to_string(),
      affected_paths: record
        .snapshots
        .iter()
        .map(|snapshot| snapshot.path.clone())
        .collect(),
    },
  )
}

pub async fn handle_transfer_message(
  message: EncodedFileTransferMessage,
) {
  let WithChannel { channel, data } = match message.decode() {
    Ok(message) => message,
    Err(error) => {
      warn!("Invalid file-transfer channel message: {error:#}");
      return;
    }
  };
  let message = data.and_then(FileTransferMessage::from_raw);
  let Some(sender) = file_transfer_channels().get(&channel).await
  else {
    warn!("No file-transfer channel for {channel}");
    return;
  };
  if sender.send(message).await.is_err() {
    file_transfer_channels().remove(&channel).await;
  }
}

pub async fn start_upload(
  core: &str,
  request: StartFileManagerUpload,
) -> anyhow::Result<Uuid> {
  let StartFileManagerUpload {
    target,
    actor,
    destination,
    file_name,
    total_bytes,
    overwrite,
    expected_revision,
  } = request;
  let root = resolve_root(&target).await?;
  if root.read_only {
    return Err(anyhow!("This File Manager root is read-only"));
  }
  let destination = relative_path(&destination, true)?;
  let file_name = single_name(&file_name)?;
  let relative = destination.join(&file_name);
  let path = path_string(&relative)?;
  if root.managed_file.as_deref() == Some(path.as_str()) {
    return Err(anyhow!(
      "The managed compose file can only be changed in the editor"
    ));
  }
  let core_key = core.to_string();
  let connection =
    core_connections().get(&core_key).await.with_context(|| {
      format!("Core connection {core} is unavailable")
    })?;
  let channel = Uuid::new_v4();
  let (sender, mut receiver) = tokio::sync::mpsc::channel(32);
  file_transfer_channels().insert(channel, sender).await;
  tokio::spawn(async move {
    let result = async {
      let begin = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        receiver.recv(),
      )
      .await
      .context("Upload did not begin before timeout")?
      .context("Upload channel closed")??;
      if begin != FileTransferMessage::Begin {
        return Err(anyhow!("Upload did not begin correctly"));
      }
      let lock = root_lock(&root.key).await;
      let _guard = lock.lock().await;
      let root_dir = open_root(&root, true)?;
      let (parent, name) =
        open_parent_nofollow(&root_dir, &relative)?;
      let initial_revision = tree_revision(&root_dir, &path)?;
      if overwrite && initial_revision != expected_revision {
        return Err(anyhow!(
          "Upload destination changed after overwrite confirmation"
        ));
      }
      if !overwrite && initial_revision.is_some() {
        return Err(anyhow!("Upload destination already exists"));
      }
      let temporary =
        format!(".komodo-upload-{}.tmp", Uuid::new_v4());
      let mut options = OpenOptions::new();
      options
        .write(true)
        .create_new(true)
        .follow(FollowSymlinks::No);
      let file = parent.open_with(&temporary, &options)?;
      let mut temporary_upload = TemporaryUpload {
        parent: &parent,
        name: temporary.clone(),
        file: Some(file),
        committed: false,
      };
      let mut received = 0_u64;
      let mut hasher = Sha256::new();
      loop {
        let message = tokio::time::timeout(
          std::time::Duration::from_secs(60),
          receiver.recv(),
        )
        .await
        .context("Upload stalled")?
        .context("Upload channel closed")??;
        match message {
          FileTransferMessage::Chunk(bytes) => {
            received = received
              .checked_add(bytes.len() as u64)
              .context("Upload size overflow")?;
            if received > total_bytes {
              return Err(anyhow!(
                "Upload exceeded its declared size"
              ));
            }
            hasher.update(&bytes);
            temporary_upload
              .file
              .as_mut()
              .context("Upload staging file is unavailable")?
              .write_all(&bytes)?;
          }
          FileTransferMessage::Complete { bytes, sha256 } => {
            let actual: [u8; 32] = hasher.finalize().into();
            if received != total_bytes
              || bytes != received
              || sha256 != actual
            {
              return Err(anyhow!(
                "Upload byte count or checksum verification failed"
              ));
            }
            temporary_upload
              .file
              .take()
              .context("Upload staging file is unavailable")?
              .sync_all()?;
            let operation =
              FileManagerOperation::CreateFile { path: path.clone() };
            let journal = create_journal(
              &root,
              &actor,
              &Uuid::new_v4().to_string(),
              &operation,
              &[],
            )?;
            let commit = (|| {
              if tree_revision(&root_dir, &path)? != initial_revision
              {
                return Err(anyhow!(
                  "Upload destination changed while streaming"
                ));
              }
              if parent.symlink_metadata(&name).is_ok() {
                if !overwrite {
                  return Err(anyhow!(
                    "Upload destination changed while streaming"
                  ));
                }
                remove_entry(&parent, &name)?;
              }
              parent.rename(&temporary, &parent, &name)?;
              anyhow::Ok(())
            })();
            match commit {
              Ok(()) => {
                temporary_upload.committed = true;
                let journal =
                  match finish_journal(&root_dir, journal.clone()) {
                    Ok(journal) => journal,
                    Err(error) => {
                      let _ = restore_journal(&root_dir, &journal);
                      let _ = fs::remove_dir_all(
                        journal_root().join(&journal.id),
                      );
                      return Err(error);
                    }
                  };
                push_journal(journal).await?;
                return Ok(FileTransferMessage::Complete {
                  bytes: received,
                  sha256: actual,
                });
              }
              Err(error) => {
                let _ = restore_journal(&root_dir, &journal);
                return Err(error);
              }
            }
          }
          FileTransferMessage::Cancel => {
            return Err(anyhow!("Upload was cancelled"));
          }
          FileTransferMessage::Begin => {
            return Err(anyhow!("Duplicate upload begin message"));
          }
        }
      }
    }
    .await;
    file_transfer_channels().remove(&channel).await;
    let _ = connection
      .sender
      .send_file_transfer(
        channel,
        result.map(FileTransferMessage::into_raw),
      )
      .await;
  });
  Ok(channel)
}

pub async fn start_download(
  core: &str,
  target: PeripheryFileManagerTarget,
  paths: Vec<String>,
) -> anyhow::Result<StartFileManagerDownloadResponse> {
  if paths.is_empty() {
    return Err(anyhow!("Select at least one entry to download"));
  }
  let root = resolve_root(&target).await?;
  if root
    .managed_file
    .as_ref()
    .is_some_and(|managed| paths.iter().any(|path| path == managed))
  {
    return Err(anyhow!(
      "The managed compose file is available only through the editor"
    ));
  }
  let core_key = core.to_string();
  let connection =
    core_connections().get(&core_key).await.with_context(|| {
      format!("Core connection {core} is unavailable")
    })?;
  let lock = root_lock(&root.key).await;
  let _guard = lock.lock().await;
  let root_dir = open_root(&root, false)?;
  let staging = journal_root()
    .join("transfers")
    .join(Uuid::new_v4().to_string());
  fs::create_dir_all(&staging)?;
  let prepared = (|| {
    let (file_name, staged) = if paths.len() == 1 {
      let relative = relative_path(&paths[0], false)?;
      let (parent, name) =
        open_parent_nofollow(&root_dir, &relative)?;
      let metadata = parent.symlink_metadata(&name)?;
      if metadata.is_file() && !metadata.file_type().is_symlink() {
        let download_name = name
          .to_str()
          .context("Download filename is not UTF-8")?
          .to_string();
        let staged = staging.join("download");
        copy_capability_to_host(&parent, &name, &staged)?;
        (download_name, staged)
      } else {
        let staged = staging.join("download.zip");
        archive::create_download_zip(&root_dir, &paths, &staged)?;
        (format!("{}.zip", name.to_string_lossy()), staged)
      }
    } else {
      let staged = staging.join("download.zip");
      archive::create_download_zip(&root_dir, &paths, &staged)?;
      ("komodo-download.zip".to_string(), staged)
    };
    let total_bytes = fs::metadata(&staged)?.len();
    let sha256 = hash_host_file(&staged)?;
    anyhow::Ok((file_name, staged, total_bytes, sha256))
  })();
  let (file_name, staged, total_bytes, sha256) = match prepared {
    Ok(prepared) => prepared,
    Err(error) => {
      let _ = fs::remove_dir_all(&staging);
      return Err(error);
    }
  };
  drop(_guard);
  let channel = Uuid::new_v4();
  let (sender, mut receiver) = tokio::sync::mpsc::channel(4);
  file_transfer_channels().insert(channel, sender).await;
  tokio::spawn(async move {
    let result = async {
      let begin = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        receiver.recv(),
      )
      .await
      .context("Download did not begin before timeout")?
      .context("Download channel closed")??;
      if begin != FileTransferMessage::Begin {
        return Err(anyhow!("Download did not begin correctly"));
      }
      let mut file = tokio::fs::File::open(&staged).await?;
      let mut buffer = vec![0_u8; 256 * 1024];
      let mut sent = 0_u64;
      use tokio::io::AsyncReadExt as _;
      loop {
        match receiver.try_recv() {
          Ok(Ok(FileTransferMessage::Cancel)) => {
            return Err(anyhow!("Download was cancelled"));
          }
          Ok(Ok(_)) => {
            return Err(anyhow!(
              "Unexpected download control message"
            ));
          }
          Ok(Err(error)) => return Err(error),
          Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
          Err(
            tokio::sync::mpsc::error::TryRecvError::Disconnected,
          ) => {
            return Err(anyhow!("Download channel closed"));
          }
        }
        let read = file.read(&mut buffer).await?;
        if read == 0 {
          break;
        }
        sent += read as u64;
        connection
          .sender
          .send_file_transfer(
            channel,
            Ok(
              FileTransferMessage::Chunk(buffer[..read].to_vec())
                .into_raw(),
            ),
          )
          .await?;
      }
      if sent != total_bytes {
        return Err(anyhow!(
          "Download byte count changed while streaming"
        ));
      }
      connection
        .sender
        .send_file_transfer(
          channel,
          Ok(
            FileTransferMessage::Complete {
              bytes: sent,
              sha256,
            }
            .into_raw(),
          ),
        )
        .await?;
      anyhow::Ok(())
    }
    .await;
    if let Err(error) = result {
      let _ = connection
        .sender
        .send_file_transfer(channel, Err(error))
        .await;
    }
    file_transfer_channels().remove(&channel).await;
    let _ = fs::remove_dir_all(staging);
  });
  Ok(StartFileManagerDownloadResponse {
    channel,
    file_name,
    total_bytes,
    sha256: hex::encode(sha256),
  })
}

fn hash_host_file(path: &Path) -> anyhow::Result<[u8; 32]> {
  let mut file = fs::File::open(path)?;
  let mut hasher = Sha256::new();
  let mut buffer = [0_u8; 256 * 1024];
  loop {
    let read = file.read(&mut buffer)?;
    if read == 0 {
      break;
    }
    hasher.update(&buffer[..read]);
  }
  Ok(hasher.finalize().into())
}

fn archive_extension(
  format: FileManagerArchiveFormat,
) -> &'static str {
  match format {
    FileManagerArchiveFormat::Zip => ".zip",
    FileManagerArchiveFormat::Tar => ".tar",
    FileManagerArchiveFormat::TarGz => ".tar.gz",
    FileManagerArchiveFormat::SevenZip => ".7z",
  }
}

fn ensure_archive_extension(
  destination: &str,
  format: FileManagerArchiveFormat,
) -> String {
  let extension = archive_extension(format);
  if destination.to_lowercase().ends_with(extension) {
    destination.to_string()
  } else {
    format!("{destination}{extension}")
  }
}

fn normalize_operation(
  mut operation: FileManagerOperation,
) -> FileManagerOperation {
  if let FileManagerOperation::CreateArchive {
    destination,
    format,
    ..
  } = &mut operation
  {
    *destination = ensure_archive_extension(destination, *format);
  }
  operation
}

fn duplicate_name(
  name: &str,
  index: usize,
  preserve_extension: bool,
) -> String {
  const COMPOUND_EXTENSIONS: [&str; 4] =
    [".tar.gz", ".tar.bz2", ".tar.xz", ".tar.zst"];
  let lower = name.to_lowercase();
  let split = if preserve_extension {
    COMPOUND_EXTENSIONS
      .iter()
      .find(|extension| lower.ends_with(**extension))
      .map(|extension| name.len() - extension.len())
      .or_else(|| name.rfind('.').filter(|position| *position > 0))
      .unwrap_or(name.len())
  } else {
    name.len()
  };
  let (stem, extension) = name.split_at(split);
  format!("{stem} ({index}){extension}")
}

#[cfg(test)]
fn direct_copy_targets(
  operation: &FileManagerOperation,
) -> anyhow::Result<Vec<CopyTarget>> {
  let FileManagerOperation::Copy { paths, destination } = operation
  else {
    return Ok(Vec::new());
  };
  let destination = relative_path(destination, true)?;
  paths
    .iter()
    .map(|source| {
      let source = relative_path(source, false)?;
      let destination = destination.join(
        source.file_name().context("Source path has no filename")?,
      );
      Ok(CopyTarget {
        source: path_string(&source)?,
        destination: path_string(&destination)?,
      })
    })
    .collect()
}

fn resolve_copy_targets(
  root: Option<&Dir>,
  operation: &FileManagerOperation,
) -> anyhow::Result<Vec<CopyTarget>> {
  let FileManagerOperation::Copy { paths, destination } = operation
  else {
    return Ok(Vec::new());
  };
  let destination = relative_path(destination, true)?;
  let mut pending = Vec::with_capacity(paths.len());
  for source in paths {
    let source = relative_path(source, false)?;
    let source_parent =
      source.parent().unwrap_or_else(|| Path::new(""));
    let target = destination.join(
      source.file_name().context("Source path has no filename")?,
    );
    let same_parent = source_parent == destination;
    pending.push((source, target, same_parent));
  }

  let mut reserved = pending
    .iter()
    .filter(|(_, _, same_parent)| !same_parent)
    .map(|(_, target, _)| path_string(target))
    .collect::<anyhow::Result<HashSet<_>>>()?;
  let mut targets = Vec::with_capacity(pending.len());
  for (source, default_target, same_parent) in pending {
    let target = if same_parent {
      let root = root.context(
        "The source directory is unavailable for duplicate-name planning",
      )?;
      let name = source
        .file_name()
        .and_then(|name| name.to_str())
        .context("Source path is not valid UTF-8")?;
      let source_string = path_string(&source)?;
      let metadata = entry_metadata(root, &source_string)?
        .context("Source path does not exist")?;
      let preserve_extension =
        !metadata.is_dir() || metadata.file_type().is_symlink();
      let mut index = 1;
      loop {
        let candidate = destination.join(duplicate_name(
          name,
          index,
          preserve_extension,
        ));
        let candidate_string = path_string(&candidate)?;
        if !reserved.contains(&candidate_string)
          && entry_metadata(root, &candidate_string)?.is_none()
        {
          reserved.insert(candidate_string);
          break candidate;
        }
        index += 1;
      }
    } else {
      default_target
    };
    targets.push(CopyTarget {
      source: path_string(&source)?,
      destination: path_string(&target)?,
    });
  }
  Ok(targets)
}

fn validate_operation(
  root: &ResolvedRoot,
  operation: &FileManagerOperation,
) -> anyhow::Result<()> {
  match operation {
    FileManagerOperation::CreateFile { path }
    | FileManagerOperation::CreateDirectory { path }
    | FileManagerOperation::Rename { path, .. }
    | FileManagerOperation::WriteText { path, .. } => {
      relative_path(path, false)?;
    }
    FileManagerOperation::Move { paths, destination }
    | FileManagerOperation::Copy { paths, destination } => {
      let moving =
        matches!(operation, FileManagerOperation::Move { .. });
      if paths.is_empty() {
        return Err(anyhow!("Select at least one entry"));
      }
      let destination = relative_path(destination, true)?;
      let mut destination_names = std::collections::HashSet::new();
      for path in paths {
        let source = relative_path(path, false)?;
        let source_name = source
          .file_name()
          .context("Source path has no filename")?;
        if !destination_names.insert(source_name.to_os_string()) {
          return Err(anyhow!(
            "Multiple selected entries have the same destination name"
          ));
        }
        if moving && destination.join(source_name) == source {
          return Err(anyhow!("Source and destination are the same"));
        }
        if destination.starts_with(&source) {
          return Err(anyhow!(
            "An entry cannot be copied or moved inside itself"
          ));
        }
      }
    }
    FileManagerOperation::Delete { paths }
    | FileManagerOperation::CreateArchive { paths, .. } => {
      if paths.is_empty() {
        return Err(anyhow!("Select at least one entry"));
      }
      for path in paths {
        relative_path(path, false)?;
      }
      if let FileManagerOperation::CreateArchive {
        destination, ..
      } = operation
      {
        let destination_path = relative_path(destination, false)?;
        if paths.iter().any(|path| path == destination) {
          return Err(anyhow!(
            "Archive destination cannot replace a selected source"
          ));
        }
        for source in paths {
          if destination_path
            .starts_with(relative_path(source, false)?)
          {
            return Err(anyhow!(
              "Archive destination cannot be inside a selected source"
            ));
          }
        }
      }
    }
    FileManagerOperation::ExtractArchive { path, destination } => {
      relative_path(path, false)?;
      relative_path(destination, false)?;
      if path == destination {
        return Err(anyhow!(
          "Extraction destination cannot replace its archive"
        ));
      }
    }
  }
  if let FileManagerOperation::Rename { new_name, .. } = operation {
    single_name(new_name)?;
  }
  if let FileManagerOperation::WriteText {
    path: _, contents, ..
  } = operation
    && contents.len() as u64 > MAX_TEXT_BYTES
  {
    return Err(anyhow!("Text exceeds the editor size limit"));
  }
  if let Some(managed) = root.managed_file.as_deref() {
    let touches_managed =
      watched_paths(operation)?.iter().any(|path| path == managed);
    if touches_managed
      && !matches!(operation, FileManagerOperation::WriteText { path, .. } if path == managed)
    {
      return Err(anyhow!(
        "The managed compose file can only be changed in the editor"
      ));
    }
  }
  Ok(())
}

#[cfg(test)]
fn find_conflicts(
  root: Option<&Dir>,
  operation: &FileManagerOperation,
) -> anyhow::Result<Vec<FileManagerConflict>> {
  let copy_targets = direct_copy_targets(operation)?;
  find_conflicts_planned(root, operation, &copy_targets)
}

fn find_conflicts_planned(
  root: Option<&Dir>,
  operation: &FileManagerOperation,
  copy_targets: &[CopyTarget],
) -> anyhow::Result<Vec<FileManagerConflict>> {
  let Some(root) = root else {
    return Ok(Vec::new());
  };
  let mut paths = Vec::new();
  let mut conflicts = Vec::new();
  match operation {
    FileManagerOperation::CreateFile { path } => {
      paths.push((path.clone(), FileManagerEntryKind::File));
    }
    FileManagerOperation::CreateDirectory { path } => {
      paths.push((path.clone(), FileManagerEntryKind::Directory));
    }
    FileManagerOperation::Rename { path, new_name } => {
      let source = relative_path(path, false)?;
      let destination = source
        .parent()
        .unwrap_or_else(|| Path::new(""))
        .join(single_name(new_name)?);
      paths.push((
        path_string(&destination)?,
        entry_metadata(root, path)?
          .as_ref()
          .map(entry_kind)
          .context("Source path does not exist")?,
      ));
    }
    FileManagerOperation::Move {
      paths: sources,
      destination,
    } => {
      let destination = relative_path(destination, true)?;
      for source in sources {
        let source_path = relative_path(source, false)?;
        let target = destination.join(
          source_path
            .file_name()
            .context("Source path has no filename")?,
        );
        collect_merge_conflicts(
          root,
          &source_path,
          &target,
          &mut conflicts,
        )?;
      }
    }
    FileManagerOperation::Copy { .. } => {
      for target in copy_targets {
        collect_merge_conflicts(
          root,
          &relative_path(&target.source, false)?,
          &relative_path(&target.destination, false)?,
          &mut conflicts,
        )?;
      }
    }
    FileManagerOperation::CreateArchive { destination, .. } => {
      paths.push((destination.clone(), FileManagerEntryKind::File));
    }
    FileManagerOperation::ExtractArchive { destination, .. } => {
      paths
        .push((destination.clone(), FileManagerEntryKind::Directory));
    }
    FileManagerOperation::Delete { .. }
    | FileManagerOperation::WriteText { .. } => {}
  }
  for (path, incoming_kind) in paths {
    if let Some(metadata) = entry_metadata(root, &path)? {
      conflicts.push(FileManagerConflict {
        path,
        existing_kind: entry_kind(&metadata),
        incoming_kind,
      });
    }
  }
  Ok(conflicts)
}

fn collect_merge_conflicts(
  root: &Dir,
  source: &Path,
  target: &Path,
  conflicts: &mut Vec<FileManagerConflict>,
) -> anyhow::Result<()> {
  let (source_parent, source_name) =
    open_parent_nofollow(root, source)?;
  let source_metadata =
    source_parent.symlink_metadata(&source_name)?;
  let (target_parent, target_name) =
    open_parent_nofollow(root, target)?;
  let target_metadata =
    match target_parent.symlink_metadata(&target_name) {
      Ok(metadata) => metadata,
      Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
        return Ok(());
      }
      Err(error) => return Err(error.into()),
    };
  if source_metadata.is_dir()
    && !source_metadata.file_type().is_symlink()
    && target_metadata.is_dir()
    && !target_metadata.file_type().is_symlink()
  {
    let source_dir = source_parent.open_dir_nofollow(&source_name)?;
    let mut children = source_dir
      .entries()?
      .map(|entry| {
        entry.and_then(|entry| {
          entry.file_name().into_string().map_err(|_| {
            std::io::Error::new(
              std::io::ErrorKind::InvalidData,
              "Non-UTF-8 filenames are unsupported",
            )
          })
        })
      })
      .collect::<std::io::Result<Vec<_>>>()?;
    children.sort();
    for child in children {
      collect_merge_conflicts(
        root,
        &source.join(&child),
        &target.join(&child),
        conflicts,
      )?;
    }
  } else {
    if conflicts.len() as u64 >= MAX_ENTRIES {
      return Err(anyhow!("Conflict list exceeds the entry limit"));
    }
    conflicts.push(FileManagerConflict {
      path: path_string(target)?,
      existing_kind: entry_kind(&target_metadata),
      incoming_kind: entry_kind(&source_metadata),
    });
  }
  Ok(())
}

fn validate_conflict_decisions(
  conflicts: &[FileManagerConflict],
  decisions: &[FileManagerConflictDecision],
) -> anyhow::Result<()> {
  for conflict in conflicts {
    if !decisions
      .iter()
      .any(|decision| decision.path == conflict.path)
    {
      return Err(anyhow!(
        "Every conflict must have an explicit overwrite or skip decision"
      ));
    }
  }
  Ok(())
}

fn decision_for(
  path: &str,
  decisions: &[FileManagerConflictDecision],
) -> Option<FileManagerConflictAction> {
  decisions
    .iter()
    .find(|decision| decision.path == path)
    .map(|decision| decision.action)
}

#[cfg(test)]
fn apply_operation(
  root: &Dir,
  operation: &FileManagerOperation,
  decisions: &[FileManagerConflictDecision],
) -> anyhow::Result<()> {
  let copy_targets = direct_copy_targets(operation)?;
  apply_operation_planned(root, operation, &copy_targets, decisions)
}

fn apply_operation_planned(
  root: &Dir,
  operation: &FileManagerOperation,
  copy_targets: &[CopyTarget],
  decisions: &[FileManagerConflictDecision],
) -> anyhow::Result<()> {
  match operation {
    FileManagerOperation::CreateFile { path } => {
      let path = relative_path(path, false)?;
      let (parent, name) = open_parent_nofollow(root, &path)?;
      let normalized = path_string(&path)?;
      if parent.symlink_metadata(&name).is_ok() {
        match decision_for(&normalized, decisions) {
          Some(FileManagerConflictAction::Skip) => return Ok(()),
          Some(FileManagerConflictAction::Overwrite) => {
            remove_entry(&parent, &name)?
          }
          None => return Err(anyhow!("Destination already exists")),
        }
      }
      let mut options = OpenOptions::new();
      options
        .write(true)
        .create_new(true)
        .follow(FollowSymlinks::No);
      parent.open_with(name, &options)?.sync_all()?;
    }
    FileManagerOperation::CreateDirectory { path } => {
      let path = relative_path(path, false)?;
      let (parent, name) = open_parent_nofollow(root, &path)?;
      let normalized = path_string(&path)?;
      if parent.symlink_metadata(&name).is_ok() {
        match decision_for(&normalized, decisions) {
          Some(FileManagerConflictAction::Skip) => return Ok(()),
          Some(FileManagerConflictAction::Overwrite) => {
            remove_entry(&parent, &name)?
          }
          None => return Err(anyhow!("Destination already exists")),
        }
      }
      parent.create_dir(name)?;
    }
    FileManagerOperation::Rename { path, new_name } => {
      let source = relative_path(path, false)?;
      let (parent, old_name) = open_parent_nofollow(root, &source)?;
      let new_name = single_name(new_name)?;
      let destination = source
        .parent()
        .unwrap_or_else(|| Path::new(""))
        .join(&new_name);
      let destination_string = path_string(&destination)?;
      if parent.symlink_metadata(&new_name).is_ok() {
        match decision_for(&destination_string, decisions) {
          Some(FileManagerConflictAction::Skip) => return Ok(()),
          Some(FileManagerConflictAction::Overwrite) => {
            remove_entry(&parent, &new_name)?
          }
          None => return Err(anyhow!("Destination already exists")),
        }
      }
      parent.rename(old_name, &parent, new_name)?;
    }
    FileManagerOperation::Move { paths, destination } => {
      let destination = relative_path(destination, true)?;
      let destination_dir = open_dir_nofollow(root, &destination)?;
      for source in paths {
        let source = relative_path(source, false)?;
        let (source_parent, source_name) =
          open_parent_nofollow(root, &source)?;
        let target = destination.join(&source_name);
        merge_entry(
          &source_parent,
          &source_name,
          &destination_dir,
          &source_name,
          &path_string(&target)?,
          true,
          decisions,
        )?;
      }
    }
    FileManagerOperation::Copy { .. } => {
      for target in copy_targets {
        let source = relative_path(&target.source, false)?;
        let destination = relative_path(&target.destination, false)?;
        let (source_parent, source_name) =
          open_parent_nofollow(root, &source)?;
        let (destination_parent, destination_name) =
          open_parent_nofollow(root, &destination)?;
        merge_entry(
          &source_parent,
          &source_name,
          &destination_parent,
          &destination_name,
          &target.destination,
          false,
          decisions,
        )?;
      }
    }
    FileManagerOperation::Delete { paths } => {
      for path in paths {
        let path = relative_path(path, false)?;
        let (parent, name) = open_parent_nofollow(root, &path)?;
        remove_entry(&parent, name)?;
      }
    }
    FileManagerOperation::WriteText {
      path,
      contents,
      expected_revision,
    } => {
      let path = relative_path(path, false)?;
      let (parent, name) = open_parent_nofollow(root, &path)?;
      match parent.symlink_metadata(&name) {
        Ok(metadata) => {
          if !metadata.is_file() || metadata.file_type().is_symlink()
          {
            return Err(anyhow!("Path is not a regular file"));
          }
          let mut read_options = OpenOptions::new();
          read_options.read(true).follow(FollowSymlinks::No);
          let mut current = parent.open_with(&name, &read_options)?;
          let mut current_bytes = Vec::new();
          current.read_to_end(&mut current_bytes)?;
          if content_revision(&metadata, &current_bytes)
            != *expected_revision
          {
            return Err(anyhow!(
              "File changed since it was opened; reload before saving"
            ));
          }
        }
        Err(error)
          if error.kind() == std::io::ErrorKind::NotFound
            && expected_revision.id.is_empty() => {}
        Err(error) => return Err(error.into()),
      }

      let temporary =
        format!(".komodo-file-manager-{}.tmp", Uuid::new_v4());
      let mut options = OpenOptions::new();
      options
        .write(true)
        .create_new(true)
        .follow(FollowSymlinks::No);
      let file = parent.open_with(&temporary, &options)?;
      let mut temporary_write = TemporaryUpload {
        parent: &parent,
        name: temporary.clone(),
        file: Some(file),
        committed: false,
      };
      temporary_write
        .file
        .as_mut()
        .context("Text staging file is unavailable")?
        .write_all(contents.as_bytes())?;
      temporary_write
        .file
        .take()
        .context("Text staging file is unavailable")?
        .sync_all()?;
      parent.rename(&temporary, &parent, name)?;
      temporary_write.committed = true;
    }
    FileManagerOperation::CreateArchive {
      paths,
      destination,
      format,
    } => {
      archive::create(root, paths, destination, *format, decisions)?
    }
    FileManagerOperation::ExtractArchive { path, destination } => {
      archive::extract(root, path, destination, decisions)?
    }
  }
  Ok(())
}

fn merge_entry(
  source_parent: &Dir,
  source_name: &std::ffi::OsStr,
  destination_parent: &Dir,
  destination_name: &std::ffi::OsStr,
  destination_path: &str,
  move_entry: bool,
  decisions: &[FileManagerConflictDecision],
) -> anyhow::Result<()> {
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
      let children = source_dir
        .entries()?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<std::io::Result<Vec<_>>>()?;
      for child in children {
        let child_name = child
          .to_str()
          .context("Non-UTF-8 filenames are unsupported")?;
        merge_entry(
          &source_dir,
          &child,
          &destination_dir,
          &child,
          &format!("{destination_path}/{child_name}"),
          move_entry,
          decisions,
        )?;
      }
      if move_entry && source_dir.entries()?.next().is_none() {
        source_parent.remove_dir(source_name)?;
      }
      return Ok(());
    }
    match decision_for(destination_path, decisions) {
      Some(FileManagerConflictAction::Skip) => return Ok(()),
      Some(FileManagerConflictAction::Overwrite) => {
        remove_entry(destination_parent, destination_name)?
      }
      None => return Err(anyhow!("Destination already exists")),
    }
  }

  if move_entry {
    match source_parent.rename(
      source_name,
      destination_parent,
      destination_name,
    ) {
      Ok(()) => {}
      Err(error) if error.raw_os_error() == Some(libc::EXDEV) => {
        copy_entry(
          source_parent,
          source_name,
          destination_parent,
          destination_name,
        )?;
        remove_entry(source_parent, source_name)?;
      }
      Err(error) => return Err(error.into()),
    }
  } else {
    copy_entry(
      source_parent,
      source_name,
      destination_parent,
      destination_name,
    )?;
  }
  Ok(())
}

fn copy_entry(
  source_parent: &Dir,
  source_name: &std::ffi::OsStr,
  destination_parent: &Dir,
  destination_name: &std::ffi::OsStr,
) -> anyhow::Result<()> {
  let metadata = source_parent.symlink_metadata(source_name)?;
  if metadata.file_type().is_symlink() {
    return Err(anyhow!("Symbolic links cannot be copied"));
  }
  if metadata.is_file() {
    let mut read_options = OpenOptions::new();
    read_options.read(true).follow(FollowSymlinks::No);
    let mut source =
      source_parent.open_with(source_name, &read_options)?;
    let mut write_options = OpenOptions::new();
    write_options
      .write(true)
      .create_new(true)
      .follow(FollowSymlinks::No);
    let mut destination = destination_parent
      .open_with(destination_name, &write_options)?;
    let mut source_hash = Sha256::new();
    let mut buffer = [0_u8; 128 * 1024];
    loop {
      let read = source.read(&mut buffer)?;
      if read == 0 {
        break;
      }
      source_hash.update(&buffer[..read]);
      destination.write_all(&buffer[..read])?;
    }
    destination.sync_all()?;
    let mut destination = destination_parent
      .open_with(destination_name, &read_options)?;
    let mut destination_hash = Sha256::new();
    loop {
      let read = destination.read(&mut buffer)?;
      if read == 0 {
        break;
      }
      destination_hash.update(&buffer[..read]);
    }
    if source_hash.finalize() != destination_hash.finalize() {
      let _ = destination_parent.remove_file(destination_name);
      return Err(anyhow!(
        "Copied file checksum verification failed"
      ));
    }
  } else if metadata.is_dir() {
    destination_parent.create_dir(destination_name)?;
    let source = source_parent.open_dir_nofollow(source_name)?;
    let destination =
      destination_parent.open_dir_nofollow(destination_name)?;
    for entry in source.entries()? {
      let entry = entry?;
      let name = entry.file_name();
      copy_entry(&source, &name, &destination, &name)?;
    }
  } else {
    return Err(anyhow!(
      "Special filesystem entries cannot be copied"
    ));
  }
  Ok(())
}

fn remove_entry(
  parent: &Dir,
  name: impl AsRef<Path>,
) -> anyhow::Result<()> {
  let name = name.as_ref();
  let metadata = parent.symlink_metadata(name)?;
  if metadata.is_dir() && !metadata.file_type().is_symlink() {
    parent.remove_dir_all(name)?;
  } else {
    parent.remove_file_or_symlink(name)?;
  }
  Ok(())
}

fn watched_paths(
  operation: &FileManagerOperation,
) -> anyhow::Result<Vec<String>> {
  let paths = match operation {
    FileManagerOperation::CreateFile { path }
    | FileManagerOperation::CreateDirectory { path }
    | FileManagerOperation::WriteText { path, .. } => {
      vec![path.clone()]
    }
    FileManagerOperation::Delete { paths } => paths.clone(),
    FileManagerOperation::Rename { path, new_name } => {
      let source = relative_path(path, false)?;
      vec![
        path.clone(),
        path_string(
          &source
            .parent()
            .unwrap_or_else(|| Path::new(""))
            .join(single_name(new_name)?),
        )?,
      ]
    }
    FileManagerOperation::Move {
      paths: sources,
      destination,
    } => {
      let destination = relative_path(destination, true)?;
      let mut paths = sources.clone();
      for source in sources {
        let source = relative_path(source, false)?;
        paths.push(path_string(&destination.join(
          source.file_name().context("Source has no filename")?,
        ))?);
      }
      paths
    }
    FileManagerOperation::Copy {
      paths: sources,
      destination,
    } => {
      let destination = relative_path(destination, true)?;
      let mut paths = Vec::with_capacity(sources.len());
      for source in sources {
        let source = relative_path(source, false)?;
        paths.push(path_string(&destination.join(
          source.file_name().context("Source has no filename")?,
        ))?);
      }
      paths
    }
    FileManagerOperation::CreateArchive { destination, .. }
    | FileManagerOperation::ExtractArchive { destination, .. } => {
      vec![destination.clone()]
    }
  };
  Ok(paths)
}

fn watched_paths_planned(
  operation: &FileManagerOperation,
  copy_targets: &[CopyTarget],
) -> anyhow::Result<Vec<String>> {
  if matches!(operation, FileManagerOperation::Copy { .. }) {
    Ok(
      copy_targets
        .iter()
        .map(|target| target.destination.clone())
        .collect(),
    )
  } else {
    watched_paths(operation)
  }
}

fn revision_paths_planned(
  operation: &FileManagerOperation,
  copy_targets: &[CopyTarget],
) -> anyhow::Result<Vec<String>> {
  let mut paths = watched_paths_planned(operation, copy_targets)?;
  match operation {
    FileManagerOperation::Copy { paths: sources, .. }
    | FileManagerOperation::CreateArchive {
      paths: sources, ..
    } => paths.extend(sources.iter().cloned()),
    FileManagerOperation::ExtractArchive { path, .. } => {
      paths.push(path.clone());
    }
    _ => {}
  }
  Ok(paths)
}

fn affected_paths_planned(
  operation: &FileManagerOperation,
  copy_targets: &[CopyTarget],
) -> Vec<String> {
  if matches!(operation, FileManagerOperation::Copy { .. }) {
    copy_targets
      .iter()
      .map(|target| target.destination.clone())
      .collect()
  } else {
    operation
      .affected_paths()
      .into_iter()
      .map(str::to_string)
      .collect()
  }
}

fn create_journal(
  root: &ResolvedRoot,
  actor: &str,
  id: &str,
  operation: &FileManagerOperation,
  copy_targets: &[CopyTarget],
) -> anyhow::Result<JournalRecord> {
  let directory = journal_root().join(id).join("before");
  fs::create_dir_all(&directory)?;
  let root_dir = open_root(root, true)?;
  let mut snapshots = Vec::new();
  let mut before_revisions = Vec::new();
  let mut watched = watched_paths_planned(operation, copy_targets)?;
  watched.sort();
  watched.dedup();
  for (index, path) in watched.into_iter().enumerate() {
    let relative = relative_path(&path, false)?;
    let backup_name = index.to_string();
    let backup = directory.join(&backup_name);
    let before_revision = tree_revision(&root_dir, &path)?;
    let existed = before_revision.is_some();
    if existed {
      backup_from_capability(&root_dir, &relative, &backup)?;
    }
    snapshots.push(JournalSnapshot {
      path: path.clone(),
      existed,
      backup_name,
    });
    before_revisions.push((path, before_revision));
  }
  let created_at = komodo_timestamp();
  Ok(JournalRecord {
    id: id.to_string(),
    actor: actor.to_string(),
    root_key: root.key.clone(),
    root_path: root.path.clone(),
    managed: root.managed_file.is_some(),
    created_at,
    expires_at: created_at + JOURNAL_TTL_MS,
    description: operation_description(operation),
    snapshots,
    before_revisions,
    after_revisions: Vec::new(),
  })
}

fn finish_journal(
  root: &Dir,
  mut record: JournalRecord,
) -> anyhow::Result<JournalRecord> {
  let directory = journal_root().join(&record.id);
  let after_directory = directory.join("after");
  fs::create_dir_all(&after_directory)?;
  let mut after_revisions = Vec::new();
  for snapshot in &record.snapshots {
    let revision = tree_revision(root, &snapshot.path)?;
    if revision.is_some() {
      backup_from_capability(
        root,
        &relative_path(&snapshot.path, false)?,
        &after_directory.join(&snapshot.backup_name),
      )?;
    }
    after_revisions.push((snapshot.path.clone(), revision));
  }
  record.after_revisions = after_revisions;
  let manifest = serde_json::to_vec_pretty(&record)?;
  let temporary = directory.join("manifest.json.tmp");
  fs::write(&temporary, manifest)?;
  fs::rename(temporary, directory.join("manifest.json"))?;
  Ok(record)
}

async fn push_journal(record: JournalRecord) -> anyhow::Result<()> {
  let key = history_key(&record.root_key, &record.actor);
  let mut histories = histories().lock().await;
  let history = histories.entry(key).or_default();
  for redo in history.redo.drain(..) {
    let _ = fs::remove_dir_all(journal_root().join(redo.id));
  }
  history.undo.push(record);
  prune_history(history);
  Ok(())
}

fn restore_journal(
  root: &Dir,
  record: &JournalRecord,
) -> anyhow::Result<()> {
  for snapshot in record.snapshots.iter().rev() {
    let relative = relative_path(&snapshot.path, false)?;
    if let Ok((parent, name)) = open_parent_nofollow(root, &relative)
      && parent.symlink_metadata(&name).is_ok()
    {
      remove_entry(&parent, &name)?;
    }
    if snapshot.existed {
      let source = journal_root()
        .join(&record.id)
        .join("before")
        .join(&snapshot.backup_name);
      restore_to_capability(root, &relative, &source)?;
    }
  }
  Ok(())
}

fn restore_after_journal(
  root: &Dir,
  record: &JournalRecord,
) -> anyhow::Result<()> {
  for snapshot in record.snapshots.iter().rev() {
    let relative = relative_path(&snapshot.path, false)?;
    if let Ok((parent, name)) = open_parent_nofollow(root, &relative)
      && parent.symlink_metadata(&name).is_ok()
    {
      remove_entry(&parent, &name)?;
    }
    let exists_after = record
      .after_revisions
      .iter()
      .find(|(path, _)| path == &snapshot.path)
      .is_some_and(|(_, revision)| revision.is_some());
    if exists_after {
      let source = journal_root()
        .join(&record.id)
        .join("after")
        .join(&snapshot.backup_name);
      restore_to_capability(root, &relative, &source)?;
    }
  }
  Ok(())
}

fn verify_revisions(
  root: &Dir,
  revisions: &[(String, Option<FileManagerRevision>)],
  unsafe_message: &str,
) -> anyhow::Result<()> {
  for (path, expected) in revisions {
    if &tree_revision(root, path)? != expected {
      return Err(anyhow!("{unsafe_message}"));
    }
  }
  Ok(())
}

fn capture_snapshot_revisions(
  root: &Dir,
  snapshots: &[JournalSnapshot],
) -> anyhow::Result<Vec<(String, Option<FileManagerRevision>)>> {
  snapshots
    .iter()
    .map(|snapshot| {
      Ok((
        snapshot.path.clone(),
        tree_revision(root, &snapshot.path)?,
      ))
    })
    .collect()
}

fn backup_from_capability(
  root: &Dir,
  relative: &Path,
  destination: &Path,
) -> anyhow::Result<()> {
  let (parent, name) = open_parent_nofollow(root, relative)?;
  copy_capability_to_host(&parent, &name, destination)
}

fn copy_capability_to_host(
  parent: &Dir,
  name: &std::ffi::OsStr,
  destination: &Path,
) -> anyhow::Result<()> {
  let metadata = parent.symlink_metadata(name)?;
  if metadata.file_type().is_symlink() {
    let target = parent.read_link(name)?;
    create_host_symlink(&target, destination, false)?;
  } else if metadata.is_file() {
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    let mut source = parent.open_with(name, &options)?;
    let mut destination = fs::File::create(destination)?;
    std::io::copy(&mut source, &mut destination)?;
    destination.sync_all()?;
  } else if metadata.is_dir() {
    fs::create_dir(destination)?;
    let source = parent.open_dir_nofollow(name)?;
    for entry in source.entries()? {
      let entry = entry?;
      let child = entry.file_name();
      copy_capability_to_host(
        &source,
        &child,
        &destination.join(&child),
      )?;
    }
  } else {
    return Err(anyhow!("Special entries cannot be journaled"));
  }
  Ok(())
}

fn restore_to_capability(
  root: &Dir,
  relative: &Path,
  source: &Path,
) -> anyhow::Result<()> {
  let (parent, name) = open_parent_nofollow(root, relative)?;
  copy_host_to_capability(source, &parent, &name)
}

fn copy_host_to_capability(
  source: &Path,
  parent: &Dir,
  name: &std::ffi::OsStr,
) -> anyhow::Result<()> {
  let metadata = fs::symlink_metadata(source)?;
  if metadata.file_type().is_symlink() {
    let target = fs::read_link(source)?;
    parent.symlink(target, name)?;
  } else if metadata.is_file() {
    let mut source = fs::File::open(source)?;
    let mut options = OpenOptions::new();
    options
      .write(true)
      .create_new(true)
      .follow(FollowSymlinks::No);
    let mut destination = parent.open_with(name, &options)?;
    std::io::copy(&mut source, &mut destination)?;
    destination.sync_all()?;
  } else if metadata.is_dir() {
    parent.create_dir(name)?;
    let destination = parent.open_dir_nofollow(name)?;
    for entry in fs::read_dir(source)? {
      let entry = entry?;
      let child = entry.file_name();
      copy_host_to_capability(&entry.path(), &destination, &child)?;
    }
  } else {
    return Err(anyhow!(
      "Special journal entries cannot be restored"
    ));
  }
  Ok(())
}

#[cfg(unix)]
fn create_host_symlink(
  target: &Path,
  destination: &Path,
  _directory: bool,
) -> std::io::Result<()> {
  std::os::unix::fs::symlink(target, destination)
}

#[cfg(windows)]
fn create_host_symlink(
  target: &Path,
  destination: &Path,
  directory: bool,
) -> std::io::Result<()> {
  if directory {
    std::os::windows::fs::symlink_dir(target, destination)
  } else {
    std::os::windows::fs::symlink_file(target, destination)
  }
}

fn history_key(root: &str, actor: &str) -> String {
  format!("{root}:{actor}")
}

fn prune_history(history: &mut JournalHistory) {
  let now = komodo_timestamp();
  history.undo.retain(|record| {
    let keep = record.expires_at > now;
    if !keep {
      let _ = fs::remove_dir_all(journal_root().join(&record.id));
    }
    keep
  });
  history.redo.retain(|record| {
    let keep = record.expires_at > now;
    if !keep {
      let _ = fs::remove_dir_all(journal_root().join(&record.id));
    }
    keep
  });
}

fn operation_description(operation: &FileManagerOperation) -> String {
  match operation {
    FileManagerOperation::CreateFile { .. } => "Create file",
    FileManagerOperation::CreateDirectory { .. } => "Create folder",
    FileManagerOperation::Rename { .. } => "Rename",
    FileManagerOperation::Move { .. } => "Move",
    FileManagerOperation::Copy { .. } => "Copy",
    FileManagerOperation::Delete { .. } => "Delete",
    FileManagerOperation::WriteText { .. } => "Save text file",
    FileManagerOperation::CreateArchive { .. } => "Create archive",
    FileManagerOperation::ExtractArchive { .. } => "Extract archive",
  }
  .to_string()
}

fn entry_metadata(
  root: &Dir,
  path: &str,
) -> anyhow::Result<Option<Metadata>> {
  let relative = relative_path(path, false)?;
  let (parent, name) = open_parent_nofollow(root, &relative)?;
  match parent.symlink_metadata(name) {
    Ok(metadata) => Ok(Some(metadata)),
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
      Ok(None)
    }
    Err(error) => Err(error.into()),
  }
}

fn tree_revision(
  root: &Dir,
  path: &str,
) -> anyhow::Result<Option<FileManagerRevision>> {
  let relative = relative_path(path, false)?;
  let (parent, name) = open_parent_nofollow(root, &relative)?;
  let metadata = match parent.symlink_metadata(&name) {
    Ok(metadata) => metadata,
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
      return Ok(None);
    }
    Err(error) => return Err(error.into()),
  };
  let mut hasher = Sha256::new();
  let mut entries = 0_u64;
  hash_tree_entry(
    &parent,
    &name,
    &metadata,
    &mut hasher,
    &mut entries,
    0,
  )?;
  Ok(Some(FileManagerRevision {
    id: hex::encode(hasher.finalize()),
  }))
}

fn hash_tree_entry(
  parent: &Dir,
  name: &std::ffi::OsStr,
  metadata: &Metadata,
  hasher: &mut Sha256,
  entries: &mut u64,
  depth: usize,
) -> anyhow::Result<()> {
  *entries += 1;
  if *entries > MAX_ENTRIES || depth > path::MAX_DEPTH {
    return Err(anyhow!("Entry exceeds File Manager tree limits"));
  }
  hasher.update(name.as_encoded_bytes());
  hasher.update(revision(metadata).id.as_bytes());
  if metadata.is_file() && !metadata.file_type().is_symlink() {
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    let mut file = parent.open_with(name, &options)?;
    let mut buffer = [0_u8; 128 * 1024];
    loop {
      let read = file.read(&mut buffer)?;
      if read == 0 {
        break;
      }
      hasher.update(&buffer[..read]);
    }
  } else if metadata.is_dir() && !metadata.file_type().is_symlink() {
    let directory = parent.open_dir_nofollow(name)?;
    let mut children = directory
      .entries()?
      .map(|entry| entry.map(|entry| entry.file_name()))
      .collect::<std::io::Result<Vec<_>>>()?;
    children.sort();
    for child in children {
      let metadata = directory.symlink_metadata(&child)?;
      hash_tree_entry(
        &directory,
        &child,
        &metadata,
        hasher,
        entries,
        depth + 1,
      )?;
    }
  }
  Ok(())
}

fn entry_kind(metadata: &Metadata) -> FileManagerEntryKind {
  if metadata.file_type().is_symlink() {
    FileManagerEntryKind::Symlink
  } else if metadata.is_dir() {
    FileManagerEntryKind::Directory
  } else if metadata.is_file() {
    FileManagerEntryKind::File
  } else {
    FileManagerEntryKind::Special
  }
}

fn modified_at(metadata: &Metadata) -> i64 {
  metadata
    .modified()
    .ok()
    .and_then(|time| time.into_std().duration_since(UNIX_EPOCH).ok())
    .map(|duration| duration.as_millis() as i64)
    .unwrap_or_default()
}

fn revision(metadata: &Metadata) -> FileManagerRevision {
  let mut hash = Sha256::new();
  hash.update(metadata.dev().to_le_bytes());
  hash.update(metadata.ino().to_le_bytes());
  hash.update(metadata.len().to_le_bytes());
  hash.update(modified_at(metadata).to_le_bytes());
  hash.update([entry_kind(metadata) as u8]);
  FileManagerRevision {
    id: hex::encode(hash.finalize()),
  }
}

fn content_revision(
  metadata: &Metadata,
  contents: &[u8],
) -> FileManagerRevision {
  let mut hash = Sha256::new();
  hash.update(revision(metadata).id.as_bytes());
  hash.update(contents);
  FileManagerRevision {
    id: hex::encode(hash.finalize()),
  }
}

fn path_string(path: &Path) -> anyhow::Result<String> {
  path
    .to_str()
    .map(|path| path.replace('\\', "/"))
    .context("Path is not valid UTF-8")
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn configured_paths_match_stack_execution_roots() {
    assert_eq!(
      configured_path("/srv/komodo/stacks/demo", "./config"),
      PathBuf::from("/srv/komodo/stacks/demo/config")
    );
    assert_eq!(
      configured_path("/srv/komodo/stacks/demo", "../shared"),
      PathBuf::from("/srv/komodo/stacks/shared")
    );
    assert_eq!(
      configured_path("/srv/komodo/stacks/demo", "/opt/stack"),
      PathBuf::from("/opt/stack")
    );
  }

  #[test]
  fn operation_audit_description_never_contains_text() {
    let operation = FileManagerOperation::WriteText {
      path: "compose.yaml".into(),
      contents: "super-secret".into(),
      expected_revision: Default::default(),
    };
    assert_eq!(operation_description(&operation), "Save text file");
  }

  #[test]
  fn managed_compose_is_editor_only() {
    let root = ResolvedRoot {
      path: PathBuf::from("/unused"),
      key: "test".into(),
      read_only: false,
      managed_file: Some("compose.yaml".into()),
      create_if_missing: false,
    };
    assert!(
      validate_operation(
        &root,
        &FileManagerOperation::Delete {
          paths: vec!["compose.yaml".into()]
        }
      )
      .is_err()
    );
    assert!(
      validate_operation(
        &root,
        &FileManagerOperation::Rename {
          path: "other.yaml".into(),
          new_name: "compose.yaml".into(),
        }
      )
      .is_err()
    );
    assert!(
      validate_operation(
        &root,
        &FileManagerOperation::WriteText {
          path: "compose.yaml".into(),
          contents: "services: {}".into(),
          expected_revision: Default::default(),
        }
      )
      .is_ok()
    );
  }

  #[test]
  fn text_write_rejects_a_stale_content_revision() {
    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    fs::create_dir(&directory).unwrap();
    let path = directory.join("config.txt");
    fs::write(&path, "first").unwrap();
    let root =
      Dir::open_ambient_dir(&directory, ambient_authority()).unwrap();
    let metadata = root.symlink_metadata("config.txt").unwrap();
    let expected = content_revision(&metadata, b"first");
    fs::write(&path, "changed externally").unwrap();

    let result = apply_operation(
      &root,
      &FileManagerOperation::WriteText {
        path: "config.txt".into(),
        contents: "editor change".into(),
        expected_revision: expected,
      },
      &[],
    );
    assert!(result.is_err());
    assert_eq!(
      fs::read_to_string(&path).unwrap(),
      "changed externally"
    );
    fs::remove_dir_all(directory).unwrap();
  }

  #[test]
  fn tree_revision_detects_nested_content_changes() {
    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    fs::create_dir_all(directory.join("nested")).unwrap();
    let child = directory.join("nested/file.txt");
    fs::write(&child, "one").unwrap();
    let root =
      Dir::open_ambient_dir(&directory, ambient_authority()).unwrap();
    let before = tree_revision(&root, "nested").unwrap();
    fs::write(&child, "two").unwrap();
    let after = tree_revision(&root, "nested").unwrap();

    assert_ne!(before, after);
    fs::remove_dir_all(directory).unwrap();
  }

  #[test]
  fn archive_names_receive_the_selected_extension() {
    assert_eq!(
      ensure_archive_extension(
        "backup",
        FileManagerArchiveFormat::Zip
      ),
      "backup.zip"
    );
    assert_eq!(
      ensure_archive_extension(
        "backup.ZIP",
        FileManagerArchiveFormat::Zip
      ),
      "backup.ZIP"
    );
    assert_eq!(
      ensure_archive_extension(
        "backup.tar",
        FileManagerArchiveFormat::Zip
      ),
      "backup.tar.zip"
    );
    assert_eq!(
      ensure_archive_extension(
        "backup",
        FileManagerArchiveFormat::TarGz
      ),
      "backup.tar.gz"
    );
  }

  #[test]
  fn duplicate_names_preserve_extensions() {
    assert_eq!(
      duplicate_name("report.txt", 1, true),
      "report (1).txt"
    );
    assert_eq!(
      duplicate_name("archive.tar.gz", 2, true),
      "archive (2).tar.gz"
    );
    assert_eq!(duplicate_name("folder", 1, false), "folder (1)");
    assert_eq!(
      duplicate_name("folder.name", 1, false),
      "folder.name (1)"
    );
    assert_eq!(duplicate_name(".env", 1, true), ".env (1)");
  }

  #[test]
  fn same_parent_copies_receive_the_first_available_name() {
    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    fs::create_dir_all(directory.join("folder")).unwrap();
    fs::create_dir_all(directory.join("folder.name")).unwrap();
    fs::write(directory.join("report.txt"), "source").unwrap();
    fs::write(directory.join("report (1).txt"), "existing").unwrap();
    fs::write(directory.join("archive.tar.gz"), "archive").unwrap();
    fs::write(directory.join(".env"), "VALUE=1").unwrap();
    let root =
      Dir::open_ambient_dir(&directory, ambient_authority()).unwrap();

    for (source, expected) in [
      ("report.txt", "report (2).txt"),
      ("archive.tar.gz", "archive (1).tar.gz"),
      (".env", ".env (1)"),
      ("folder", "folder (1)"),
      ("folder.name", "folder.name (1)"),
    ] {
      let operation = FileManagerOperation::Copy {
        paths: vec![source.into()],
        destination: String::new(),
      };
      let targets =
        resolve_copy_targets(Some(&root), &operation).unwrap();
      assert_eq!(targets[0].destination, expected);
      assert!(
        find_conflicts_planned(Some(&root), &operation, &targets)
          .unwrap()
          .is_empty()
      );
      apply_operation_planned(&root, &operation, &targets, &[])
        .unwrap();
      assert!(directory.join(expected).exists());
    }

    assert_eq!(
      fs::read_to_string(directory.join("report (2).txt")).unwrap(),
      "source"
    );
    fs::remove_dir_all(directory).unwrap();
  }

  #[test]
  fn duplicate_planning_reserves_other_batch_destinations() {
    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    fs::create_dir_all(directory.join("other")).unwrap();
    fs::write(directory.join("foo.txt"), "same parent").unwrap();
    fs::write(directory.join("other/foo (1).txt"), "incoming")
      .unwrap();
    let root =
      Dir::open_ambient_dir(&directory, ambient_authority()).unwrap();
    let operation = FileManagerOperation::Copy {
      paths: vec!["foo.txt".into(), "other/foo (1).txt".into()],
      destination: String::new(),
    };

    let targets =
      resolve_copy_targets(Some(&root), &operation).unwrap();
    assert_eq!(targets[0].destination, "foo (2).txt");
    assert_eq!(targets[1].destination, "foo (1).txt");
    fs::remove_dir_all(directory).unwrap();
  }

  #[test]
  fn refreshed_redo_revisions_still_reject_external_changes() {
    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    fs::create_dir(&directory).unwrap();
    fs::write(directory.join("config.txt"), "restored").unwrap();
    let root =
      Dir::open_ambient_dir(&directory, ambient_authority()).unwrap();
    let snapshots = vec![JournalSnapshot {
      path: "config.txt".into(),
      existed: true,
      backup_name: "0".into(),
    }];
    let refreshed =
      capture_snapshot_revisions(&root, &snapshots).unwrap();

    verify_revisions(&root, &refreshed, "Redo is unsafe").unwrap();
    fs::write(directory.join("config.txt"), "changed externally")
      .unwrap();
    let error = verify_revisions(
      &root,
      &refreshed,
      "Redo is unsafe because files changed after undo",
    )
    .unwrap_err();
    assert_eq!(
      error.to_string(),
      "Redo is unsafe because files changed after undo"
    );
    fs::remove_dir_all(directory).unwrap();
  }

  #[test]
  fn recursive_copy_applies_per_file_conflict_decisions() {
    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    fs::create_dir_all(directory.join("source")).unwrap();
    fs::create_dir_all(directory.join("destination/source")).unwrap();
    fs::write(directory.join("source/conflict.txt"), "incoming")
      .unwrap();
    fs::write(directory.join("source/new.txt"), "new").unwrap();
    fs::write(
      directory.join("destination/source/conflict.txt"),
      "existing",
    )
    .unwrap();
    let root =
      Dir::open_ambient_dir(&directory, ambient_authority()).unwrap();
    let operation = FileManagerOperation::Copy {
      paths: vec!["source".into()],
      destination: "destination".into(),
    };
    let conflicts = find_conflicts(Some(&root), &operation).unwrap();
    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0].path, "destination/source/conflict.txt");

    apply_operation(
      &root,
      &operation,
      &[FileManagerConflictDecision {
        path: conflicts[0].path.clone(),
        action: FileManagerConflictAction::Skip,
      }],
    )
    .unwrap();
    assert_eq!(
      fs::read_to_string(
        directory.join("destination/source/conflict.txt")
      )
      .unwrap(),
      "existing"
    );
    assert_eq!(
      fs::read_to_string(
        directory.join("destination/source/new.txt")
      )
      .unwrap(),
      "new"
    );
    fs::remove_dir_all(directory).unwrap();
  }
}
