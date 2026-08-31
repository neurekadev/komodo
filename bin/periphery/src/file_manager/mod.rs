use std::{
  collections::{HashMap, HashSet, VecDeque},
  ffi::OsString,
  fs,
  io::{Read as _, Write as _},
  path::{Component, Path, PathBuf},
  sync::{
    Arc, Mutex as StdMutex, OnceLock, RwLock as StdRwLock,
    atomic::{AtomicBool, Ordering},
  },
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
    FileManagerActiveOperations, FileManagerArchiveFormat,
    FileManagerCapabilities, FileManagerConflict,
    FileManagerConflictAction, FileManagerConflictDecision,
    FileManagerDirectory, FileManagerEntry, FileManagerEntryKind,
    FileManagerJournalStatus, FileManagerLimits,
    FileManagerOperation, FileManagerOperationPhase,
    FileManagerOperationState, FileManagerOperationStatus,
    FileManagerPendingConflict, FileManagerPreflight,
    FileManagerRevision, FileManagerTextFile,
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
use tokio::sync::{Mutex, Notify, Semaphore};
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
/// Kept in the public limits response for wire compatibility. A value of zero
/// means archive expansion is capacity-limited instead of fixed-size-limited.
pub const MAX_ARCHIVE_EXPANDED_BYTES: u64 = 0;
pub const MAX_ARCHIVE_EXPANSION_RATIO: u64 = 1_000;
pub const MINIMUM_FREE_BYTES: u64 = 256 * 1024 * 1024;
const PLAN_TTL_MS: i64 = 5 * 60 * 1_000;
const JOURNAL_TTL_MS: i64 = 24 * 60 * 60 * 1_000;
const MAX_HEAVY_JOBS: usize = 2;
const MAX_READ_JOBS: usize = 4;

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
  progress: OperationProgress,
}

#[derive(Debug, Clone, Copy)]
struct ConflictResolution {
  action: FileManagerConflictAction,
  apply_to_all: bool,
}

#[derive(Debug, Default)]
struct OperationControl {
  cancelled: AtomicBool,
  decisions: StdMutex<VecDeque<(String, ConflictResolution)>>,
  decision_notify: Notify,
}

#[derive(Debug, Clone, Copy, Default)]
struct WorkTotal {
  entries: u64,
  bytes: u64,
}

impl WorkTotal {
  fn add(&mut self, other: Self) {
    self.entries = self.entries.saturating_add(other.entries);
    self.bytes = self.bytes.saturating_add(other.bytes);
  }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct SpaceRequirements {
  journal_bytes: u64,
  target_bytes: u64,
}

fn heavy_job_permits() -> &'static Arc<Semaphore> {
  static PERMITS: OnceLock<Arc<Semaphore>> = OnceLock::new();
  PERMITS.get_or_init(|| {
    let permits = std::thread::available_parallelism()
      .map(|parallelism| parallelism.get())
      .unwrap_or(1)
      .clamp(1, MAX_HEAVY_JOBS);
    Arc::new(Semaphore::new(permits))
  })
}

fn archive_transform_permits() -> &'static Arc<Semaphore> {
  static PERMITS: OnceLock<Arc<Semaphore>> = OnceLock::new();
  PERMITS.get_or_init(|| Arc::new(Semaphore::new(1)))
}

fn read_job_permits() -> &'static Arc<Semaphore> {
  static PERMITS: OnceLock<Arc<Semaphore>> = OnceLock::new();
  PERMITS.get_or_init(|| {
    let permits = std::thread::available_parallelism()
      .map(|parallelism| parallelism.get())
      .unwrap_or(1)
      .clamp(1, MAX_READ_JOBS);
    Arc::new(Semaphore::new(permits))
  })
}

async fn run_heavy_blocking<T, F>(task: F) -> anyhow::Result<T>
where
  T: Send + 'static,
  F: FnOnce() -> anyhow::Result<T> + Send + 'static,
{
  let permit = heavy_job_permits()
    .clone()
    .acquire_owned()
    .await
    .context("File Manager job queue is unavailable")?;
  tokio::task::spawn_blocking(move || {
    let _permit = permit;
    task()
  })
  .await
  .context("File Manager blocking job stopped unexpectedly")?
}

async fn run_read_blocking<T, F>(task: F) -> anyhow::Result<T>
where
  T: Send + 'static,
  F: FnOnce() -> anyhow::Result<T> + Send + 'static,
{
  let permit = read_job_permits()
    .clone()
    .acquire_owned()
    .await
    .context("File Manager read queue is unavailable")?;
  tokio::task::spawn_blocking(move || {
    let _permit = permit;
    task()
  })
  .await
  .context("File Manager read job stopped unexpectedly")?
}

async fn run_root_blocking<T, F>(
  root_key: &str,
  task: F,
) -> anyhow::Result<T>
where
  T: Send + 'static,
  F: FnOnce() -> anyhow::Result<T> + Send + 'static,
{
  let lock = root_lock(root_key).await;
  let guard = lock.lock_owned().await;
  run_heavy_blocking(move || {
    let _guard = guard;
    task()
  })
  .await
}

#[derive(Debug, Clone)]
pub(super) struct OperationProgress {
  status: Arc<StdRwLock<FileManagerOperationStatus>>,
  control: Arc<OperationControl>,
}

impl OperationProgress {
  fn new(operation_id: String, description: String) -> Self {
    let now = komodo_timestamp();
    Self {
      status: Arc::new(StdRwLock::new(FileManagerOperationStatus {
        operation_id,
        state: FileManagerOperationState::Pending,
        phase: FileManagerOperationPhase::Queued,
        description,
        started_at: now,
        updated_at: now,
        phase_started_at: now,
        cancellable: true,
        ..Default::default()
      })),
      control: Arc::new(OperationControl::default()),
    }
  }

  fn snapshot(&self) -> FileManagerOperationStatus {
    self.status.read().unwrap().clone()
  }

  fn phase(
    &self,
    phase: FileManagerOperationPhase,
    total: WorkTotal,
  ) {
    let mut status = self.status.write().unwrap();
    status.state = FileManagerOperationState::Running;
    status.phase = phase;
    let now = komodo_timestamp();
    status.updated_at = now;
    status.phase_started_at = now;
    status.pending_conflict = None;
    status.completed_entries = 0;
    status.total_entries = total.entries;
    status.completed_bytes = 0;
    status.total_bytes = total.bytes;
  }

  pub(super) fn add_entry(&self) {
    let mut status = self.status.write().unwrap();
    status.completed_entries = status
      .completed_entries
      .saturating_add(1)
      .min(if status.total_entries == 0 {
        u64::MAX
      } else {
        status.total_entries
      });
    status.updated_at = komodo_timestamp();
  }

  pub(super) fn add_bytes(&self, bytes: u64) {
    let mut status = self.status.write().unwrap();
    status.completed_bytes = status
      .completed_bytes
      .saturating_add(bytes)
      .min(if status.total_bytes == 0 {
        u64::MAX
      } else {
        status.total_bytes
      });
    status.updated_at = komodo_timestamp();
  }

  pub(super) fn check_cancelled(&self) -> anyhow::Result<()> {
    if self.control.cancelled.load(Ordering::Acquire) {
      Err(anyhow!("File Manager operation was cancelled"))
    } else {
      Ok(())
    }
  }

  fn request_cancel(&self) {
    self.control.cancelled.store(true, Ordering::Release);
    self.control.decision_notify.notify_waiters();
    let mut status = self.status.write().unwrap();
    status.cancellable = false;
    status.updated_at = komodo_timestamp();
    status.description = "Cancelling file operation".into();
  }

  fn add_temporary_storage_bytes(&self, bytes: u64) {
    let mut status = self.status.write().unwrap();
    status.temporary_storage_bytes =
      status.temporary_storage_bytes.saturating_add(bytes);
    status.updated_at = komodo_timestamp();
  }

  async fn wait_for_conflict(
    &self,
    conflict: FileManagerConflict,
  ) -> anyhow::Result<ConflictResolution> {
    let decision_id = Uuid::new_v4().to_string();
    {
      let mut status = self.status.write().unwrap();
      status.state = FileManagerOperationState::WaitingForInput;
      status.pending_conflict = Some(FileManagerPendingConflict {
        decision_id: decision_id.clone(),
        conflict,
      });
      status.updated_at = komodo_timestamp();
    }
    let wait = async {
      loop {
        self.check_cancelled()?;
        let notified = self.control.decision_notify.notified();
        let resolution = {
          let mut decisions = self.control.decisions.lock().unwrap();
          decisions
            .iter()
            .position(|(id, _)| id == &decision_id)
            .and_then(|position| decisions.remove(position))
        };
        if let Some((_, resolution)) = resolution {
          return Ok::<ConflictResolution, anyhow::Error>(resolution);
        }
        notified.await;
      }
    };
    let resolution = tokio::time::timeout(
      std::time::Duration::from_secs(30 * 60),
      wait,
    )
    .await
    .context("Conflict decision timed out after 30 minutes")??;
    {
      let mut status = self.status.write().unwrap();
      status.state = FileManagerOperationState::Running;
      status.pending_conflict = None;
      status.updated_at = komodo_timestamp();
    }
    Ok(resolution)
  }

  fn complete(&self) {
    let mut status = self.status.write().unwrap();
    status.state = FileManagerOperationState::Complete;
    status.phase = FileManagerOperationPhase::Finalizing;
    status.completed_entries = status.total_entries;
    status.completed_bytes = status.total_bytes;
    status.error = None;
    status.cancellable = false;
    status.pending_conflict = None;
    status.temporary_storage_bytes = 0;
    status.updated_at = komodo_timestamp();
  }

  fn fail(&self, error: &anyhow::Error) {
    let mut status = self.status.write().unwrap();
    status.state = if self.control.cancelled.load(Ordering::Acquire) {
      FileManagerOperationState::Cancelled
    } else {
      FileManagerOperationState::Failed
    };
    status.error = Some(format!("{error:#}"));
    status.cancellable = false;
    status.pending_conflict = None;
    status.temporary_storage_bytes = 0;
    status.updated_at = komodo_timestamp();
  }

  fn cancel(&self, message: impl Into<String>) {
    let mut status = self.status.write().unwrap();
    status.state = FileManagerOperationState::Cancelled;
    status.error = Some(message.into());
    status.cancellable = false;
    status.pending_conflict = None;
    status.temporary_storage_bytes = 0;
    status.updated_at = komodo_timestamp();
  }
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

struct StreamingUpload {
  parent: Dir,
  destination_name: OsString,
  temporary_name: String,
  file: Option<tokio::fs::File>,
  initial_revision: Option<FileManagerRevision>,
  committed: bool,
}

impl Drop for StreamingUpload {
  fn drop(&mut self) {
    self.file.take();
    if !self.committed {
      let _ = self.parent.remove_file(&self.temporary_name);
    }
  }
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

async fn register_status(
  operation_id: String,
  actor: &str,
  root_key: &str,
  description: impl Into<String>,
) -> OperationProgress {
  let progress =
    OperationProgress::new(operation_id.clone(), description.into());
  let now = komodo_timestamp();
  let mut statuses = statuses().lock().await;
  statuses.retain(|_, status| status.expires_at > now);
  statuses.insert(
    operation_id,
    OperationStatusRecord {
      actor: actor.to_string(),
      root_key: root_key.to_string(),
      expires_at: now + JOURNAL_TTL_MS,
      progress: progress.clone(),
    },
  );
  progress
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
  let mut records = run_heavy_blocking(move || {
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
    Ok(records)
  })
  .await?;
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
      let expired = {
        let mut histories = histories().lock().await;
        histories.values_mut().flat_map(prune_history).collect()
      };
      schedule_journal_cleanup(expired);
    }
  });
  Ok(())
}

pub fn limits() -> FileManagerLimits {
  FileManagerLimits {
    max_text_bytes: MAX_TEXT_BYTES,
    max_entries: max_entries(),
    max_depth: path::MAX_DEPTH as u64,
    max_archive_expanded_bytes: MAX_ARCHIVE_EXPANDED_BYTES,
    max_archive_expansion_ratio: MAX_ARCHIVE_EXPANSION_RATIO,
    minimum_free_bytes: MINIMUM_FREE_BYTES,
  }
}

pub(super) fn max_entries() -> u64 {
  periphery_config().file_manager_max_entries.get()
}

pub(super) fn ensure_entry_limit(entries: u64) -> anyhow::Result<()> {
  ensure_entry_limit_with_max(entries, max_entries())
}

fn ensure_entry_limit_with_max(
  entries: u64,
  maximum: u64,
) -> anyhow::Result<()> {
  if entries > maximum {
    return Err(anyhow!(
      "File Manager entry limit exceeded (maximum {maximum})"
    ));
  }
  Ok(())
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

  ensure_outside_private_journal(&path, &journal_root())?;

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

const PRIVATE_JOURNAL_OVERLAP_REASON: &str = "File Manager target overlaps protected Periphery private journal data and cannot be opened";

fn ensure_outside_private_journal(
  path: &Path,
  private_journal: &Path,
) -> anyhow::Result<()> {
  if paths_overlap(path, private_journal)? {
    return Err(anyhow!(PRIVATE_JOURNAL_OVERLAP_REASON));
  }
  Ok(())
}

fn paths_overlap(left: &Path, right: &Path) -> anyhow::Result<bool> {
  if left.starts_with(right) || right.starts_with(left) {
    return Ok(true);
  }

  Ok(
    path_matches_ancestor(left, right)?
      || path_matches_ancestor(right, left)?,
  )
}

fn path_matches_ancestor(
  path: &Path,
  other: &Path,
) -> anyhow::Result<bool> {
  let Some(identity) = path_identity(path)? else {
    return Ok(false);
  };
  for ancestor in other.ancestors() {
    if path_identity(ancestor)?.is_some_and(|other| other == identity)
    {
      return Ok(true);
    }
  }
  Ok(false)
}

fn path_identity(path: &Path) -> anyhow::Result<Option<(u64, u64)>> {
  let metadata = match fs::metadata(path) {
    Ok(metadata) => metadata,
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
      return Ok(None);
    }
    Err(error) => {
      return Err(error).with_context(|| {
        format!("Failed to inspect File Manager path {path:?}")
      });
    }
  };
  Ok(Some((
    cap_fs_ext::MetadataExt::dev(&metadata),
    cap_fs_ext::MetadataExt::ino(&metadata),
  )))
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
        if matches!(target, PeripheryFileManagerTarget::Volume { .. })
        {
          "Docker reported a volume path that Periphery cannot access. When Periphery runs in a container, mount the Docker volume root into it read/write at the identical path (PERIPHERY_DOCKER_VOLUME_ROOT).".into()
        } else {
          "File Manager root is not an accessible directory".into()
        },
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
  let path = path.to_string();
  run_read_blocking(move || {
    let relative = relative_path(&path, true)?;
    if root.create_if_missing && !root.path.exists() {
      return Ok(FileManagerDirectory {
        path,
        entries: Vec::new(),
      });
    }
    let root_dir = open_root(&root, false)?;
    let dir = open_dir_nofollow(&root_dir, &relative)?;
    let mut entries = Vec::new();
    for entry in dir.entries()? {
      let entry = entry?;
      let name = entry.file_name().into_string().map_err(|_| {
        anyhow!("Non-UTF-8 filenames are unsupported")
      })?;
      if name.starts_with(".komodo-file-manager-staging-") {
        continue;
      }
      ensure_entry_limit(entries.len() as u64 + 1)?;
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
    Ok(FileManagerDirectory { path, entries })
  })
  .await
}

pub async fn read_text(
  target: &PeripheryFileManagerTarget,
  path: &str,
) -> anyhow::Result<FileManagerTextFile> {
  let root = resolve_root(target).await?;
  let path = path.to_string();
  run_read_blocking(move || {
    let relative = relative_path(&path, false)?;
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
      path,
      revision: content_revision(&metadata, contents.as_bytes()),
      contents,
    })
  })
  .await
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
  let root_key = root.key.clone();
  let plan_root_key = root.key.clone();
  let (operation, conflicts, revisions, copy_targets) =
    run_root_blocking(&root_key, move || {
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
            .map(|root| metadata_tree_revision(root, &path))
            .transpose()?
            .flatten();
          anyhow::Ok((path, rev))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
      Ok((operation, conflicts, revisions, copy_targets))
    })
    .await?;
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
      root_key: plan_root_key,
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
  operation_id: &str,
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
  let progress = register_status(
    operation_id.to_string(),
    actor,
    &root.key,
    operation_description(&plan.operation),
  )
  .await;
  let root_key = root.key.clone();
  let actor = actor.to_string();
  let operation_id = operation_id.to_string();
  let decisions = decisions.to_vec();
  let job_progress = progress.clone();
  let response =
    periphery_client::api::file_manager::FileManagerCommitResponse {
      operation_id: operation_id.clone(),
      affected_paths: affected_paths_planned(
        &plan.operation,
        &plan.copy_targets,
      ),
      undoable: plan.operation.is_undoable(),
    };
  tokio::spawn(async move {
    let _archive_permit = if matches!(
      &plan.operation,
      FileManagerOperation::CreateArchive { .. }
        | FileManagerOperation::ExtractArchive { .. }
    ) {
      match archive_transform_permits().clone().acquire_owned().await
      {
        Ok(permit) => Some(permit),
        Err(error) => {
          progress.fail(
            &anyhow!(error)
              .context("Archive transform queue is unavailable"),
          );
          return;
        }
      }
    } else {
      None
    };
    let lock = root_lock(&root_key).await;
    let guard = lock.lock_owned().await;
    let result = run_heavy_blocking(move || {
      job_progress.check_cancelled()?;
      job_progress.phase(
        FileManagerOperationPhase::Preparing,
        WorkTotal::default(),
      );
      let root_dir = open_root(&root, true)?;
      for (path, expected) in &plan.revisions {
        job_progress.check_cancelled()?;
        let actual = metadata_tree_revision(&root_dir, path)?;
        if &actual != expected {
          return Err(anyhow!(
            "File Manager contents changed after preflight; retry the operation"
          ));
        }
      }
      ensure_operation_capacity(
        &root,
        &root_dir,
        &plan.operation,
        &plan.copy_targets,
      )?;

      let snapshot_total = work_for_paths(
        &root_dir,
        &journal_paths_planned(&plan.operation, &plan.copy_targets)?,
      )?;
      job_progress.phase(
        FileManagerOperationPhase::Snapshotting,
        snapshot_total,
      );
      let journal = create_journal(
        &root,
        &actor,
        &operation_id,
        &plan.operation,
        &plan.copy_targets,
        Some(&job_progress),
      )?;
      job_progress.add_temporary_storage_bytes(snapshot_total.bytes);
      job_progress.check_cancelled()?;
      job_progress.phase(
        FileManagerOperationPhase::Applying,
        operation_work(
          &root_dir,
          &plan.operation,
          &plan.copy_targets,
        )?,
      );
      let apply_result = apply_operation_planned(
        &root_dir,
        Some(&root.path),
        &plan.operation,
        &plan.copy_targets,
        &decisions,
        Some(&job_progress),
      );
      match apply_result {
        Ok(()) => {
          job_progress.check_cancelled()?;
          if plan.operation.is_undoable() || journal.managed {
            job_progress.phase(
              FileManagerOperationPhase::Finalizing,
              work_for_paths(
                &root_dir,
                &watched_paths_planned(
                  &plan.operation,
                  &plan.copy_targets,
                )?,
              )?,
            );
            let journal = match finish_journal(
              &root_dir,
              journal.clone(),
              Some(&job_progress),
            ) {
              Ok(journal) => journal,
              Err(error) => {
                job_progress.phase(
                  FileManagerOperationPhase::RollingBack,
                  WorkTotal::default(),
                );
                let _ = restore_journal(&root_dir, &journal, None);
                let _ = fs::remove_dir_all(
                  journal_root().join(&journal.id),
                );
                return Err(error);
              }
            };
            Ok((Some(journal), guard))
          } else {
            let _ =
              fs::remove_dir_all(journal_root().join(&journal.id));
            Ok((None, guard))
          }
        }
        Err(error) => {
          job_progress.phase(
            FileManagerOperationPhase::RollingBack,
            snapshot_total,
          );
          let _ = restore_journal(
            &root_dir,
            &journal,
            Some(&job_progress),
          );
          let _ =
            fs::remove_dir_all(journal_root().join(&journal.id));
          Err(error)
        }
      }
    })
    .await;
    let result = match result {
      Ok((Some(journal), _guard)) => push_journal(journal).await,
      Ok((None, _guard)) => Ok(()),
      Err(error) => Err(error),
    };
    match &result {
      Ok(()) => progress.complete(),
      Err(error) => progress.fail(error),
    }
  });
  Ok(response)
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
  Ok(record.progress.snapshot())
}

pub async fn list_active_operations(
  target: &PeripheryFileManagerTarget,
  actor: &str,
) -> anyhow::Result<FileManagerActiveOperations> {
  let root = resolve_root(target).await?;
  let mut statuses = statuses().lock().await;
  statuses.retain(|_, status| status.expires_at > komodo_timestamp());
  let mut operations = statuses
    .values()
    .filter(|record| {
      record.actor == actor && record.root_key == root.key
    })
    .map(|record| record.progress.snapshot())
    .filter(|status| {
      !matches!(
        status.state,
        FileManagerOperationState::Complete
          | FileManagerOperationState::Failed
          | FileManagerOperationState::Cancelled
      )
    })
    .collect::<Vec<_>>();
  operations.sort_by_key(|status| status.started_at);
  Ok(FileManagerActiveOperations { operations })
}

pub async fn resolve_operation_conflict(
  target: &PeripheryFileManagerTarget,
  actor: &str,
  operation_id: &str,
  decision_id: String,
  action: FileManagerConflictAction,
  apply_to_all: bool,
) -> anyhow::Result<FileManagerOperationStatus> {
  let root = resolve_root(target).await?;
  let statuses = statuses().lock().await;
  let record = statuses
    .get(operation_id)
    .context("File Manager operation was not found")?;
  if record.actor != actor || record.root_key != root.key {
    return Err(anyhow!("File Manager operation was not found"));
  }
  let status = record.progress.snapshot();
  if status
    .pending_conflict
    .as_ref()
    .is_none_or(|pending| pending.decision_id != decision_id)
  {
    return Err(anyhow!(
      "Conflict decision is stale; refresh operation status"
    ));
  }
  record
    .progress
    .control
    .decisions
    .lock()
    .unwrap()
    .push_back((
      decision_id,
      ConflictResolution {
        action,
        apply_to_all,
      },
    ));
  record.progress.control.decision_notify.notify_waiters();
  Ok(status)
}

pub async fn cancel_file_manager_operation(
  target: &PeripheryFileManagerTarget,
  actor: &str,
  operation_id: &str,
) -> anyhow::Result<FileManagerOperationStatus> {
  let root = resolve_root(target).await?;
  let statuses = statuses().lock().await;
  let record = statuses
    .get(operation_id)
    .context("File Manager operation was not found")?;
  if record.actor != actor || record.root_key != root.key {
    return Err(anyhow!("File Manager operation was not found"));
  }
  let status = record.progress.snapshot();
  if matches!(
    status.state,
    FileManagerOperationState::Complete
      | FileManagerOperationState::Failed
      | FileManagerOperationState::Cancelled
  ) {
    return Err(anyhow!(
      "File Manager operation has already finished"
    ));
  }
  record.progress.request_cancel();
  Ok(record.progress.snapshot())
}

pub async fn journal_status(
  target: &PeripheryFileManagerTarget,
  actor: &str,
) -> anyhow::Result<FileManagerJournalStatus> {
  let root = resolve_root(target).await?;
  let key = history_key(&root.key, actor);
  let (mut status, expired, retained_ids) = {
    let mut histories = histories().lock().await;
    let history = histories.entry(key).or_default();
    let expired = prune_history(history);
    let visible_undo =
      history.undo.iter().rev().find(|record| !record.managed);
    let visible_redo =
      history.redo.iter().rev().find(|record| !record.managed);
    let status = FileManagerJournalStatus {
      can_undo: visible_undo.is_some(),
      can_redo: visible_redo.is_some(),
      undo_description: history
        .undo
        .iter()
        .rev()
        .find(|record| !record.managed)
        .map(|r| r.description.clone()),
      redo_description: history
        .redo
        .iter()
        .rev()
        .find(|record| !record.managed)
        .map(|record| record.description.clone()),
      expires_at: visible_undo.map(|record| record.expires_at),
      retained_storage_bytes: 0,
      storage_description: match target {
        PeripheryFileManagerTarget::Stack { .. } => "Stack files resolve beneath PERIPHERY_STACK_DIR, or PERIPHERY_ROOT_DIRECTORY/stacks by default. Recovery records use private File Manager journal storage. Absolute host paths remain server-side.".into(),
        PeripheryFileManagerTarget::Volume { .. } => "Volume files use Docker's reported mountpoint (commonly Docker's volumes directory). Recovery records use private File Manager journal storage. Absolute host paths remain server-side.".into(),
      },
    };
    let retained_ids = history
      .undo
      .iter()
      .chain(&history.redo)
      .map(|record| record.id.clone())
      .collect::<Vec<_>>();
    (status, expired, retained_ids)
  };
  schedule_journal_cleanup(expired);
  status.retained_storage_bytes = run_read_blocking(move || {
    Ok(retained_ids.into_iter().fold(0_u64, |total, id| {
      total.saturating_add(
        host_work(&journal_root().join(id))
          .map(|work| work.bytes)
          .unwrap_or_default(),
      )
    }))
  })
  .await?;
  Ok(status)
}

pub async fn undo(
  target: &PeripheryFileManagerTarget,
  actor: &str,
  operation_id: &str,
  confirmed: bool,
  rollback_operation_id: Option<&str>,
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
  let progress = register_status(
    operation_id.to_string(),
    actor,
    &root.key,
    "Undo file operation",
  )
  .await;
  let key = history_key(&root.key, actor);
  let (record, expired) = {
    let mut histories = histories().lock().await;
    let history = histories.entry(key.clone()).or_default();
    let expired = prune_history(history);
    let position = rollback_operation_id
      .and_then(|id| {
        history.undo.iter().rposition(|record| record.id == id)
      })
      .or_else(|| {
        history.undo.iter().rposition(|record| !record.managed)
      });
    let record = position
      .map(|position| history.undo.remove(position))
      .context("Nothing is available to undo");
    (record, expired)
  };
  schedule_journal_cleanup(expired);
  let mut record = match record {
    Ok(record) => record,
    Err(error) => {
      progress.fail(&error);
      return Err(error);
    }
  };
  let root_key = root.key.clone();
  let fallback_record = record.clone();
  let job_progress = progress.clone();
  let response =
    periphery_client::api::file_manager::FileManagerCommitResponse {
      operation_id: operation_id.to_string(),
      affected_paths: record
        .snapshots
        .iter()
        .map(|snapshot| snapshot.path.clone())
        .collect(),
      undoable: true,
    };
  tokio::spawn(async move {
    let lock = root_lock(&root_key).await;
    let guard = lock.lock_owned().await;
    let outcome = run_heavy_blocking(move || {
      job_progress.check_cancelled()?;
      let restore_total =
        host_work(&journal_root().join(&record.id).join("before"))
          .unwrap_or_default();
      let result = (|| {
        let root_dir = open_root(&root, true)?;
        let redo_total = work_for_paths(
          &root_dir,
          &record
            .snapshots
            .iter()
            .map(|snapshot| snapshot.path.clone())
            .collect::<Vec<_>>(),
        )?;
        job_progress
          .phase(FileManagerOperationPhase::Snapshotting, redo_total);
        ensure_free_space(
          &journal_root(),
          redo_total.bytes.saturating_add(MINIMUM_FREE_BYTES),
        )?;
        verify_revisions(
          &root_dir,
          &record.after_revisions,
          "Undo is unsafe because files changed after the operation",
        )?;
        capture_redo_journal(
          &root_dir,
          &record,
          Some(&job_progress),
        )?;
        job_progress
          .phase(FileManagerOperationPhase::Applying, restore_total);
        ensure_free_space(
          &root.path,
          restore_total.bytes.saturating_add(MINIMUM_FREE_BYTES),
        )?;
        restore_journal(&root_dir, &record, Some(&job_progress))?;
        record.before_revisions =
          capture_snapshot_revisions(&root_dir, &record.snapshots)?;
        Ok(())
      })();
      Ok((record, result, guard))
    })
    .await;
    let (record, result, _guard) = match outcome {
      Ok(outcome) => outcome,
      Err(error) => {
        histories()
          .lock()
          .await
          .entry(key)
          .or_default()
          .undo
          .push(fallback_record);
        progress.fail(&error);
        return;
      }
    };
    if let Err(error) = result {
      histories()
        .lock()
        .await
        .entry(key)
        .or_default()
        .undo
        .push(record);
      progress.fail(&error);
      return;
    }
    histories()
      .lock()
      .await
      .entry(key)
      .or_default()
      .redo
      .push(record);
    progress.complete();
  });
  Ok(response)
}

pub async fn redo(
  target: &PeripheryFileManagerTarget,
  actor: &str,
  operation_id: &str,
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
  let progress = register_status(
    operation_id.to_string(),
    actor,
    &root.key,
    "Redo file operation",
  )
  .await;
  let key = history_key(&root.key, actor);
  let (record, expired) = {
    let mut histories = histories().lock().await;
    let history = histories.entry(key.clone()).or_default();
    let expired = prune_history(history);
    let record = history
      .redo
      .iter()
      .rposition(|record| !record.managed)
      .map(|position| history.redo.remove(position))
      .context("Nothing is available to redo");
    (record, expired)
  };
  schedule_journal_cleanup(expired);
  let mut record = match record {
    Ok(record) => record,
    Err(error) => {
      progress.fail(&error);
      return Err(error);
    }
  };
  let root_key = root.key.clone();
  let fallback_record = record.clone();
  let job_progress = progress.clone();
  let response =
    periphery_client::api::file_manager::FileManagerCommitResponse {
      operation_id: operation_id.to_string(),
      affected_paths: record
        .snapshots
        .iter()
        .map(|snapshot| snapshot.path.clone())
        .collect(),
      undoable: true,
    };
  tokio::spawn(async move {
    let lock = root_lock(&root_key).await;
    let guard = lock.lock_owned().await;
    let outcome = run_heavy_blocking(move || {
      job_progress.check_cancelled()?;
      let total =
        host_work(&journal_root().join(&record.id).join("after"))
          .unwrap_or_default();
      job_progress.phase(FileManagerOperationPhase::Applying, total);
      let result = (|| {
        ensure_free_space(
          &root.path,
          total.bytes.saturating_add(MINIMUM_FREE_BYTES),
        )?;
        let root_dir = open_root(&root, true)?;
        verify_revisions(
          &root_dir,
          &record.before_revisions,
          "Redo is unsafe because files changed after undo",
        )?;
        restore_after_journal(
          &root_dir,
          &record,
          Some(&job_progress),
        )?;
        record.after_revisions =
          capture_snapshot_revisions(&root_dir, &record.snapshots)?;
        Ok(())
      })();
      Ok((record, result, guard))
    })
    .await;
    let (record, result, _guard) = match outcome {
      Ok(outcome) => outcome,
      Err(error) => {
        histories()
          .lock()
          .await
          .entry(key)
          .or_default()
          .redo
          .push(fallback_record);
        progress.fail(&error);
        return;
      }
    };
    if let Err(error) = result {
      histories()
        .lock()
        .await
        .entry(key)
        .or_default()
        .redo
        .push(record);
      progress.fail(&error);
      return;
    }
    histories()
      .lock()
      .await
      .entry(key)
      .or_default()
      .undo
      .push(record);
    progress.complete();
  });
  Ok(response)
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
    operation_id,
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
  let progress =
    register_status(operation_id, &actor, &root.key, "Upload file")
      .await;
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
      let guard = lock.lock_owned().await;
      let prepare_root = root.clone();
      let prepare_relative = relative.clone();
      let prepare_path = path.clone();
      let prepare_expected_revision = expected_revision.clone();
      let (guard, mut upload) = run_heavy_blocking(move || {
        let root_dir = open_root(&prepare_root, true)?;
        let (parent, destination_name) =
          open_parent_nofollow(&root_dir, &prepare_relative)?;
        let initial_revision =
          metadata_tree_revision(&root_dir, &prepare_path)?;
        if overwrite && initial_revision != prepare_expected_revision
        {
          return Err(anyhow!(
            "Upload destination changed after overwrite confirmation"
          ));
        }
        if !overwrite && initial_revision.is_some() {
          return Err(anyhow!("Upload destination already exists"));
        }
        let before_bytes = work_for_paths(
          &root_dir,
          std::slice::from_ref(&prepare_path),
        )?
        .bytes;
        ensure_capacity_requirements(
          &prepare_root,
          SpaceRequirements {
            journal_bytes: before_bytes,
            target_bytes: total_bytes,
          },
        )?;
        let temporary_name =
          format!(".komodo-upload-{}.tmp", Uuid::new_v4());
        let mut options = OpenOptions::new();
        options
          .write(true)
          .create_new(true)
          .follow(FollowSymlinks::No);
        let file = parent.open_with(&temporary_name, &options)?;
        Ok((
          guard,
          StreamingUpload {
            parent,
            destination_name,
            temporary_name,
            file: Some(tokio::fs::File::from_std(file.into_std())),
            initial_revision,
            committed: false,
          },
        ))
      })
      .await?;
      progress.phase(
        FileManagerOperationPhase::Transferring,
        WorkTotal {
          entries: 1,
          bytes: total_bytes,
        },
      );
      let mut received = 0_u64;
      let mut hasher = Sha256::new();
      loop {
        progress.check_cancelled()?;
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
            use tokio::io::AsyncWriteExt as _;
            upload
              .file
              .as_mut()
              .context("Upload staging file is unavailable")?
              .write_all(&bytes)
              .await?;
            progress.add_bytes(bytes.len() as u64);
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
            use tokio::io::AsyncWriteExt as _;
            let mut file = upload
              .file
              .take()
              .context("Upload staging file is unavailable")?;
            file.flush().await?;
            file.sync_all().await?;
            drop(file);
            progress.phase(
              FileManagerOperationPhase::Finalizing,
              WorkTotal {
                entries: 1,
                bytes: 0,
              },
            );
            let finalize_root = root.clone();
            let finalize_actor = actor.clone();
            let finalize_path = path.clone();
            let (message, _guard) = run_heavy_blocking(move || {
              let root_dir = open_root(&finalize_root, true)?;
              if metadata_tree_revision(&root_dir, &finalize_path)?
                != upload.initial_revision
              {
                return Err(anyhow!(
                  "Upload destination changed while streaming"
                ));
              }
              let operation = FileManagerOperation::CreateFile {
                path: finalize_path.clone(),
              };
              let journal = create_journal(
                &finalize_root,
                &finalize_actor,
                &Uuid::new_v4().to_string(),
                &operation,
                &[],
                None,
              )?;
              let commit = (|| {
                if upload
                  .parent
                  .symlink_metadata(&upload.destination_name)
                  .is_ok()
                {
                  if !overwrite {
                    return Err(anyhow!(
                      "Upload destination changed while streaming"
                    ));
                  }
                  remove_entry(
                    &upload.parent,
                    &upload.destination_name,
                  )?;
                }
                upload.parent.rename(
                  &upload.temporary_name,
                  &upload.parent,
                  &upload.destination_name,
                )?;
                upload.committed = true;
                anyhow::Ok(())
              })();
              if let Err(error) = commit {
                let _ = restore_journal(&root_dir, &journal, None);
                let _ = fs::remove_dir_all(
                  journal_root().join(&journal.id),
                );
                return Err(error);
              }
              let _ =
                fs::remove_dir_all(journal_root().join(&journal.id));
              Ok((
                FileTransferMessage::Complete {
                  bytes: received,
                  sha256: actual,
                },
                guard,
              ))
            })
            .await?;
            progress.add_entry();
            return Ok(message);
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
    match &result {
      Ok(_) => progress.complete(),
      Err(error) if error.to_string().contains("cancel") => {
        progress.cancel(error.to_string())
      }
      Err(error) => progress.fail(error),
    }
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
  actor: String,
  operation_id: String,
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
  let progress = register_status(
    operation_id,
    &actor,
    &root.key,
    "Download files",
  )
  .await;
  let root_key = root.key.clone();
  let job_progress = progress.clone();
  let prepared = run_root_blocking(&root_key, move || {
    let root_dir = open_root(&root, false)?;
    let total = work_for_paths(&root_dir, &paths)?;
    job_progress.phase(FileManagerOperationPhase::Preparing, total);
    let staging = journal_root()
      .join("transfers")
      .join(Uuid::new_v4().to_string());
    fs::create_dir_all(&staging)?;
    let result = (|| {
      ensure_free_space(
        &staging,
        total.bytes.saturating_add(MINIMUM_FREE_BYTES),
      )?;
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
          copy_capability_to_host(
            &parent,
            &name,
            &staged,
            Some(&job_progress),
          )?;
          (download_name, staged)
        } else {
          let staged = staging.join("download.zip");
          archive::create_download_zip(
            &root_dir,
            &paths,
            &staged,
            Some(&job_progress),
          )?;
          (format!("{}.zip", name.to_string_lossy()), staged)
        }
      } else {
        let staged = staging.join("download.zip");
        archive::create_download_zip(
          &root_dir,
          &paths,
          &staged,
          Some(&job_progress),
        )?;
        ("komodo-download.zip".to_string(), staged)
      };
      let total_bytes = fs::metadata(&staged)?.len();
      let sha256 = hash_host_file(&staged)?;
      anyhow::Ok((
        file_name,
        staged,
        staging.clone(),
        total_bytes,
        sha256,
      ))
    })();
    if result.is_err() {
      let _ = fs::remove_dir_all(&staging);
    }
    result
  })
  .await;
  let (file_name, staged, staging, total_bytes, sha256) =
    match prepared {
      Ok(prepared) => prepared,
      Err(error) => {
        progress.fail(&error);
        return Err(error);
      }
    };
  progress.phase(
    FileManagerOperationPhase::Transferring,
    WorkTotal {
      entries: 1,
      bytes: total_bytes,
    },
  );
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
        progress.add_bytes(read as u64);
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
    match &result {
      Ok(_) => progress.complete(),
      Err(error) if error.to_string().contains("cancel") => {
        progress.cancel(error.to_string())
      }
      Err(error) => progress.fail(error),
    }
    if let Err(error) = result {
      let _ = connection
        .sender
        .send_file_transfer(channel, Err(error))
        .await;
    }
    file_transfer_channels().remove(&channel).await;
    let _ = run_heavy_blocking(move || {
      let _ = fs::remove_dir_all(staging);
      Ok(())
    })
    .await;
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
    // Extraction conflicts are discovered from the validated staging tree,
    // immediately before publish. Preflight must not prompt merely because
    // the merge destination already exists.
    FileManagerOperation::ExtractArchive { .. } => {}
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
    ensure_entry_limit(conflicts.len() as u64 + 1)?;
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
  apply_operation_planned(
    root,
    None,
    operation,
    &copy_targets,
    decisions,
    None,
  )
}

fn apply_operation_planned(
  root: &Dir,
  root_path: Option<&Path>,
  operation: &FileManagerOperation,
  copy_targets: &[CopyTarget],
  decisions: &[FileManagerConflictDecision],
  progress: Option<&OperationProgress>,
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
      if let Some(progress) = progress {
        progress.add_entry();
      }
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
      if let Some(progress) = progress {
        progress.add_entry();
      }
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
      if let Some(progress) = progress {
        progress.add_entry();
      }
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
          MergeContext {
            move_entry: true,
            decisions,
            progress,
          },
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
          MergeContext {
            move_entry: false,
            decisions,
            progress,
          },
        )?;
      }
    }
    FileManagerOperation::Delete { paths } => {
      for path in paths {
        let path = relative_path(path, false)?;
        let (parent, name) = open_parent_nofollow(root, &path)?;
        remove_entry_progress(&parent, name, progress)?;
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
      if let Some(progress) = progress {
        progress.add_bytes(contents.len() as u64);
        progress.add_entry();
      }
    }
    FileManagerOperation::CreateArchive {
      paths,
      destination,
      format,
    } => archive::create(
      root,
      paths,
      destination,
      *format,
      decisions,
      progress,
    )?,
    FileManagerOperation::ExtractArchive { path, destination } => {
      archive::extract(
        root,
        root_path.context("Extraction root path is unavailable")?,
        path,
        destination,
        decisions,
        progress,
      )?
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
  context: MergeContext<'_>,
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
          context,
        )?;
      }
      if context.move_entry && source_dir.entries()?.next().is_none()
      {
        source_parent.remove_dir(source_name)?;
      }
      return Ok(());
    }
    match decision_for(destination_path, context.decisions) {
      Some(FileManagerConflictAction::Skip) => return Ok(()),
      Some(FileManagerConflictAction::Overwrite) => {
        remove_entry(destination_parent, destination_name)?
      }
      None => return Err(anyhow!("Destination already exists")),
    }
  }

  if context.move_entry {
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
          context.progress,
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
      context.progress,
    )?;
  }
  if context.move_entry
    && let Some(progress) = context.progress
  {
    progress.add_entry();
  }
  Ok(())
}

#[derive(Clone, Copy)]
struct MergeContext<'a> {
  move_entry: bool,
  decisions: &'a [FileManagerConflictDecision],
  progress: Option<&'a OperationProgress>,
}

fn copy_entry(
  source_parent: &Dir,
  source_name: &std::ffi::OsStr,
  destination_parent: &Dir,
  destination_name: &std::ffi::OsStr,
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  let metadata = source_parent.symlink_metadata(source_name)?;
  if let Some(progress) = progress {
    progress.check_cancelled()?;
  }
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
      if let Some(progress) = progress {
        progress.add_bytes(read as u64);
      }
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
      copy_entry(&source, &name, &destination, &name, progress)?;
    }
  } else {
    return Err(anyhow!(
      "Special filesystem entries cannot be copied"
    ));
  }
  if let Some(progress) = progress {
    progress.add_entry();
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

fn remove_entry_progress(
  parent: &Dir,
  name: impl AsRef<Path>,
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  let name = name.as_ref();
  if let Some(progress) = progress {
    progress.check_cancelled()?;
  }
  let metadata = parent.symlink_metadata(name)?;
  if metadata.is_dir() && !metadata.file_type().is_symlink() {
    let directory = parent.open_dir_nofollow(name)?;
    let children = directory
      .entries()?
      .map(|entry| entry.map(|entry| entry.file_name()))
      .collect::<std::io::Result<Vec<_>>>()?;
    for child in children {
      remove_entry_progress(&directory, child, progress)?;
    }
    parent.remove_dir(name)?;
  } else {
    parent.remove_file_or_symlink(name)?;
    if let Some(progress) = progress {
      progress.add_bytes(metadata.len());
    }
  }
  if let Some(progress) = progress {
    progress.add_entry();
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

fn journal_paths_planned(
  operation: &FileManagerOperation,
  copy_targets: &[CopyTarget],
) -> anyhow::Result<Vec<String>> {
  if matches!(operation, FileManagerOperation::ExtractArchive { .. })
  {
    Ok(Vec::new())
  } else {
    watched_paths_planned(operation, copy_targets)
  }
}

fn operation_work(
  root: &Dir,
  operation: &FileManagerOperation,
  copy_targets: &[CopyTarget],
) -> anyhow::Result<WorkTotal> {
  match operation {
    FileManagerOperation::CreateFile { .. }
    | FileManagerOperation::CreateDirectory { .. }
    | FileManagerOperation::Rename { .. } => Ok(WorkTotal {
      entries: 1,
      bytes: 0,
    }),
    FileManagerOperation::WriteText { contents, .. } => {
      Ok(WorkTotal {
        entries: 1,
        bytes: contents.len() as u64,
      })
    }
    FileManagerOperation::Move { paths, .. } => {
      let mut work = work_for_paths(root, paths)?;
      work.bytes = 0;
      Ok(work)
    }
    FileManagerOperation::Delete { paths }
    | FileManagerOperation::CreateArchive { paths, .. } => {
      work_for_paths(root, paths)
    }
    FileManagerOperation::Copy { .. } => work_for_paths(
      root,
      &copy_targets
        .iter()
        .map(|target| target.source.clone())
        .collect::<Vec<_>>(),
    ),
    FileManagerOperation::ExtractArchive { path, .. } => {
      archive::extraction_work(root, path)
    }
  }
}

fn operation_space_requirements(
  root: &Dir,
  operation: &FileManagerOperation,
  copy_targets: &[CopyTarget],
) -> anyhow::Result<SpaceRequirements> {
  let watched = journal_paths_planned(operation, copy_targets)?;
  let before = work_for_paths(root, &watched)?.bytes;
  let operation_bytes =
    operation_work(root, operation, copy_targets)?.bytes;

  let target = match operation {
    FileManagerOperation::Delete { .. } => 0,
    FileManagerOperation::Rename { .. }
    | FileManagerOperation::Move { .. } => 0,
    FileManagerOperation::Copy { .. } => operation_bytes,
    FileManagerOperation::CreateArchive { .. } => operation_bytes
      .saturating_add(operation_bytes / 10)
      .saturating_add(1024 * 1024),
    FileManagerOperation::ExtractArchive { path, .. } => {
      archive::extraction_capacity_bytes(root, path)?
    }
    FileManagerOperation::WriteText { contents, .. } => {
      contents.len() as u64
    }
    FileManagerOperation::CreateFile { .. }
    | FileManagerOperation::CreateDirectory { .. } => 0,
  };

  Ok(SpaceRequirements {
    journal_bytes: before,
    target_bytes: target,
  })
}

fn capacity_thresholds(
  requirements: SpaceRequirements,
  shared_filesystem: bool,
) -> (u64, Option<u64>) {
  if shared_filesystem {
    (
      requirements
        .journal_bytes
        .saturating_add(requirements.target_bytes)
        .saturating_add(MINIMUM_FREE_BYTES),
      None,
    )
  } else {
    (
      requirements
        .journal_bytes
        .saturating_add(MINIMUM_FREE_BYTES),
      (requirements.target_bytes > 0).then(|| {
        requirements.target_bytes.saturating_add(MINIMUM_FREE_BYTES)
      }),
    )
  }
}

fn ensure_operation_capacity(
  root: &ResolvedRoot,
  root_dir: &Dir,
  operation: &FileManagerOperation,
  copy_targets: &[CopyTarget],
) -> anyhow::Result<()> {
  let requirements =
    operation_space_requirements(root_dir, operation, copy_targets)?;
  ensure_capacity_requirements(root, requirements)
}

fn ensure_capacity_requirements(
  root: &ResolvedRoot,
  requirements: SpaceRequirements,
) -> anyhow::Result<()> {
  let journal = journal_root();
  let shared_filesystem = same_filesystem(&journal, &root.path)?;
  let (journal_required, target_required) =
    capacity_thresholds(requirements, shared_filesystem);
  ensure_free_space(&journal, journal_required)?;
  if let Some(target_required) = target_required {
    ensure_free_space(&root.path, target_required)?;
  }
  Ok(())
}

fn work_for_paths(
  root: &Dir,
  paths: &[String],
) -> anyhow::Result<WorkTotal> {
  let mut total = WorkTotal::default();
  for path in paths {
    let relative = relative_path(path, false)?;
    let (parent, name) = open_parent_nofollow(root, &relative)?;
    let metadata = match parent.symlink_metadata(&name) {
      Ok(metadata) => metadata,
      Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
        continue;
      }
      Err(error) => return Err(error.into()),
    };
    total.add(work_for_entry(&parent, &name, &metadata, 0)?);
  }
  Ok(total)
}

fn work_for_entry(
  parent: &Dir,
  name: &std::ffi::OsStr,
  metadata: &Metadata,
  depth: usize,
) -> anyhow::Result<WorkTotal> {
  if depth > path::MAX_DEPTH {
    return Err(anyhow!("Entry exceeds File Manager tree limits"));
  }
  let mut total = WorkTotal {
    entries: 1,
    bytes: if metadata.is_file() {
      metadata.len()
    } else {
      0
    },
  };
  if metadata.is_dir() && !metadata.file_type().is_symlink() {
    let directory = parent.open_dir_nofollow(name)?;
    for entry in directory.entries()? {
      let entry = entry?;
      let child = entry.file_name();
      let metadata = directory.symlink_metadata(&child)?;
      total.add(work_for_entry(
        &directory,
        &child,
        &metadata,
        depth + 1,
      )?);
      ensure_entry_limit(total.entries)?;
    }
  }
  Ok(total)
}

fn host_work(path: &Path) -> anyhow::Result<WorkTotal> {
  let metadata = fs::symlink_metadata(path)?;
  let mut total = WorkTotal {
    entries: 1,
    bytes: if metadata.is_file() {
      metadata.len()
    } else {
      0
    },
  };
  if metadata.is_dir() && !metadata.file_type().is_symlink() {
    for entry in fs::read_dir(path)? {
      total.add(host_work(&entry?.path())?);
      ensure_entry_limit(total.entries)?;
    }
  }
  Ok(total)
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
  progress: Option<&OperationProgress>,
) -> anyhow::Result<JournalRecord> {
  let directory = journal_root().join(id).join("before");
  fs::create_dir_all(&directory)?;
  let root_dir = open_root(root, true)?;
  let mut snapshots = Vec::new();
  let mut before_revisions = Vec::new();
  let mut watched = journal_paths_planned(operation, copy_targets)?;
  watched.sort();
  watched.dedup();
  for (index, path) in watched.into_iter().enumerate() {
    let relative = relative_path(&path, false)?;
    let backup_name = index.to_string();
    let backup = directory.join(&backup_name);
    let before_revision = tree_revision(&root_dir, &path)?;
    let existed = before_revision.is_some();
    if existed {
      backup_from_capability(
        &root_dir, &relative, &backup, progress,
      )?;
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
  _progress: Option<&OperationProgress>,
) -> anyhow::Result<JournalRecord> {
  let directory = journal_root().join(&record.id);
  let mut after_revisions = Vec::new();
  for snapshot in &record.snapshots {
    let revision = tree_revision(root, &snapshot.path)?;
    after_revisions.push((snapshot.path.clone(), revision));
  }
  record.after_revisions = after_revisions;
  let manifest = serde_json::to_vec_pretty(&record)?;
  let temporary = directory.join("manifest.json.tmp");
  fs::write(&temporary, manifest)?;
  fs::rename(temporary, directory.join("manifest.json"))?;
  Ok(record)
}

fn capture_redo_journal(
  root: &Dir,
  record: &JournalRecord,
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  let after_directory = journal_root().join(&record.id).join("after");
  if after_directory.exists() {
    fs::remove_dir_all(&after_directory)?;
  }
  fs::create_dir(&after_directory)?;
  let result = (|| {
    for snapshot in &record.snapshots {
      let has_after = record
        .after_revisions
        .iter()
        .find(|(path, _)| path == &snapshot.path)
        .is_some_and(|(_, revision)| revision.is_some());
      if has_after {
        backup_from_capability(
          root,
          &relative_path(&snapshot.path, false)?,
          &after_directory.join(&snapshot.backup_name),
          progress,
        )?;
      }
    }
    anyhow::Ok(())
  })();
  if result.is_err() {
    let _ = fs::remove_dir_all(after_directory);
  }
  result
}

async fn push_journal(record: JournalRecord) -> anyhow::Result<()> {
  let key = history_key(&record.root_key, &record.actor);
  let expired = {
    let mut histories = histories().lock().await;
    let history = histories.entry(key).or_default();
    let mut expired = history
      .redo
      .drain(..)
      .map(|record| record.id)
      .collect::<Vec<_>>();
    history.undo.push(record);
    expired.extend(prune_history(history));
    expired
  };
  schedule_journal_cleanup(expired);
  Ok(())
}

fn restore_journal(
  root: &Dir,
  record: &JournalRecord,
  progress: Option<&OperationProgress>,
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
      restore_to_capability(root, &relative, &source, progress)?;
    }
  }
  Ok(())
}

fn restore_after_journal(
  root: &Dir,
  record: &JournalRecord,
  progress: Option<&OperationProgress>,
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
      restore_to_capability(root, &relative, &source, progress)?;
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
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  let (parent, name) = open_parent_nofollow(root, relative)?;
  copy_capability_to_host(&parent, &name, destination, progress)
}

fn copy_capability_to_host(
  parent: &Dir,
  name: &std::ffi::OsStr,
  destination: &Path,
  progress: Option<&OperationProgress>,
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
    copy_with_progress(&mut source, &mut destination, progress)?;
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
        progress,
      )?;
    }
  } else {
    return Err(anyhow!("Special entries cannot be journaled"));
  }
  if let Some(progress) = progress {
    progress.add_entry();
  }
  Ok(())
}

fn restore_to_capability(
  root: &Dir,
  relative: &Path,
  source: &Path,
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  let (parent, name) = open_parent_nofollow(root, relative)?;
  copy_host_to_capability(source, &parent, &name, progress)
}

pub(super) fn copy_host_to_capability(
  source: &Path,
  parent: &Dir,
  name: &std::ffi::OsStr,
  progress: Option<&OperationProgress>,
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
    copy_with_progress(&mut source, &mut destination, progress)?;
    destination.sync_all()?;
  } else if metadata.is_dir() {
    parent.create_dir(name)?;
    let destination = parent.open_dir_nofollow(name)?;
    for entry in fs::read_dir(source)? {
      let entry = entry?;
      let child = entry.file_name();
      copy_host_to_capability(
        &entry.path(),
        &destination,
        &child,
        progress,
      )?;
    }
  } else {
    return Err(anyhow!(
      "Special journal entries cannot be restored"
    ));
  }
  if let Some(progress) = progress {
    progress.add_entry();
  }
  Ok(())
}

pub(super) fn copy_with_progress<
  R: std::io::Read,
  W: std::io::Write,
>(
  reader: &mut R,
  writer: &mut W,
  progress: Option<&OperationProgress>,
) -> std::io::Result<u64> {
  let mut copied = 0_u64;
  let mut buffer = [0_u8; 128 * 1024];
  loop {
    if let Some(progress) = progress {
      progress.check_cancelled().map_err(std::io::Error::other)?;
    }
    let read = reader.read(&mut buffer)?;
    if read == 0 {
      break;
    }
    writer.write_all(&buffer[..read])?;
    copied = copied.saturating_add(read as u64);
    if let Some(progress) = progress {
      progress.add_bytes(read as u64);
    }
  }
  Ok(copied)
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

fn prune_history(history: &mut JournalHistory) -> Vec<String> {
  let now = komodo_timestamp();
  let mut expired = Vec::new();
  history.undo.retain(|record| {
    let keep = record.expires_at > now;
    if !keep {
      expired.push(record.id.clone());
    }
    keep
  });
  history.redo.retain(|record| {
    let keep = record.expires_at > now;
    if !keep {
      expired.push(record.id.clone());
    }
    keep
  });
  expired
}

fn schedule_journal_cleanup(ids: Vec<String>) {
  if ids.is_empty() {
    return;
  }
  tokio::spawn(async move {
    let result = run_heavy_blocking(move || {
      for id in ids {
        let _ = fs::remove_dir_all(journal_root().join(id));
      }
      Ok(())
    })
    .await;
    if let Err(error) = result {
      warn!("Failed to clean up File Manager journals: {error:#}");
    }
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

fn metadata_tree_revision(
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
  let mut entries = 0;
  metadata_tree_entry(&parent, &name, &metadata, &mut entries, 0)
    .map(Some)
}

fn metadata_tree_entry(
  parent: &Dir,
  name: &std::ffi::OsStr,
  metadata: &Metadata,
  entries: &mut u64,
  depth: usize,
) -> anyhow::Result<FileManagerRevision> {
  *entries = entries.saturating_add(1);
  ensure_entry_limit(*entries)?;
  if depth > path::MAX_DEPTH {
    return Err(anyhow!("Entry exceeds File Manager tree limits"));
  }
  if !metadata.is_dir() || metadata.file_type().is_symlink() {
    return Ok(revision(metadata));
  }

  let directory = parent.open_dir_nofollow(name)?;
  let mut children = directory
    .entries()?
    .map(|entry| entry.map(|entry| entry.file_name()))
    .collect::<std::io::Result<Vec<_>>>()?;
  children.sort();

  let mut hasher = Sha256::new();
  hasher.update(revision(metadata).id.as_bytes());
  for child in children {
    let metadata = directory.symlink_metadata(&child)?;
    hasher.update(child.as_encoded_bytes());
    hasher.update(
      metadata_tree_entry(
        &directory,
        &child,
        &metadata,
        entries,
        depth + 1,
      )?
      .id
      .as_bytes(),
    );
  }
  Ok(FileManagerRevision {
    id: hex::encode(hasher.finalize()),
  })
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
  ensure_entry_limit(*entries)?;
  if depth > path::MAX_DEPTH {
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
  match metadata
    .modified()
    .ok()
    .and_then(|time| time.into_std().duration_since(UNIX_EPOCH).ok())
  {
    Some(modified) => {
      hash.update([1]);
      hash.update(modified.as_secs().to_le_bytes());
      hash.update(modified.subsec_nanos().to_le_bytes());
    }
    None => hash.update([0]),
  }
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

fn same_filesystem(
  left: &Path,
  right: &Path,
) -> anyhow::Result<bool> {
  Ok(
    cap_fs_ext::MetadataExt::dev(&fs::metadata(left)?)
      == cap_fs_ext::MetadataExt::dev(&fs::metadata(right)?),
  )
}

#[cfg(unix)]
pub(super) fn ensure_free_space(
  path: &Path,
  required: u64,
) -> anyhow::Result<()> {
  use std::{
    ffi::CString, mem::MaybeUninit, os::unix::ffi::OsStrExt as _,
  };
  let path = CString::new(path.as_os_str().as_bytes())?;
  let mut stats = MaybeUninit::<libc::statvfs>::uninit();
  // SAFETY: `path` is NUL terminated and `stats` points to writable memory.
  let result =
    unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) };
  if result != 0 {
    return Err(std::io::Error::last_os_error().into());
  }
  // SAFETY: statvfs initialized the structure after returning success.
  let stats = unsafe { stats.assume_init() };
  let available = stats.f_bavail.saturating_mul(stats.f_frsize);
  validate_free_space(available, required)
}

fn validate_free_space(
  available: u64,
  required: u64,
) -> anyhow::Result<()> {
  if available < required {
    return Err(anyhow!(
      "Insufficient free space for File Manager operation (required {required} bytes, available {available} bytes)"
    ));
  }
  Ok(())
}

#[cfg(not(unix))]
pub(super) fn ensure_free_space(
  _path: &Path,
  _required: u64,
) -> anyhow::Result<()> {
  Ok(())
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
  fn file_manager_entry_limit_accepts_exact_boundary() {
    assert!(ensure_entry_limit_with_max(5, 5).is_ok());
    assert!(ensure_entry_limit_with_max(6, 5).is_err());
  }

  #[test]
  fn operation_progress_tracks_phase_work_and_completion() {
    let progress = OperationProgress::new(
      "operation-1".into(),
      "Copy files".into(),
    );

    let queued = progress.snapshot();
    assert_eq!(queued.state, FileManagerOperationState::Pending);
    assert_eq!(queued.phase, FileManagerOperationPhase::Queued);
    assert_eq!(queued.description, "Copy files");

    progress.phase(
      FileManagerOperationPhase::Applying,
      WorkTotal {
        entries: 2,
        bytes: 12,
      },
    );
    progress.add_entry();
    progress.add_bytes(5);
    progress.add_entry();
    progress.add_entry();
    progress.add_bytes(50);
    let running = progress.snapshot();
    assert_eq!(running.state, FileManagerOperationState::Running);
    assert_eq!(running.phase, FileManagerOperationPhase::Applying);
    assert_eq!(running.completed_entries, 2);
    assert_eq!(running.total_entries, 2);
    assert_eq!(running.completed_bytes, 12);
    assert_eq!(running.total_bytes, 12);

    progress.complete();
    let complete = progress.snapshot();
    assert_eq!(complete.state, FileManagerOperationState::Complete);
    assert_eq!(complete.phase, FileManagerOperationPhase::Finalizing);
    assert_eq!(complete.completed_entries, 2);
    assert_eq!(complete.completed_bytes, 12);
    assert!(complete.error.is_none());
  }

  #[test]
  fn operation_progress_preserves_terminal_error_states() {
    let failed = OperationProgress::new(
      "operation-2".into(),
      "Extract archive".into(),
    );
    failed.fail(
      &anyhow!("archive is corrupt").context("ZIP extraction failed"),
    );
    let failed = failed.snapshot();
    assert_eq!(failed.state, FileManagerOperationState::Failed);
    assert_eq!(
      failed.error.as_deref(),
      Some("ZIP extraction failed: archive is corrupt")
    );

    let cancelled = OperationProgress::new(
      "operation-3".into(),
      "Upload file".into(),
    );
    cancelled.cancel("Upload cancelled");
    let cancelled = cancelled.snapshot();
    assert_eq!(cancelled.state, FileManagerOperationState::Cancelled);
    assert_eq!(cancelled.error.as_deref(), Some("Upload cancelled"));
  }

  #[tokio::test]
  async fn conflict_wait_is_recoverable_and_requires_an_opaque_id() {
    let progress = OperationProgress::new(
      "operation-conflict".into(),
      "Extract archive".into(),
    );
    let waiting = progress.clone();
    let task = tokio::spawn(async move {
      waiting
        .wait_for_conflict(FileManagerConflict {
          path: "output/config.txt".into(),
          existing_kind: FileManagerEntryKind::File,
          incoming_kind: FileManagerEntryKind::File,
        })
        .await
    });
    tokio::task::yield_now().await;
    let pending = progress
      .snapshot()
      .pending_conflict
      .expect("conflict should be visible in operation status");
    progress.control.decisions.lock().unwrap().push_back((
      pending.decision_id,
      ConflictResolution {
        action: FileManagerConflictAction::Skip,
        apply_to_all: true,
      },
    ));
    progress.control.decision_notify.notify_waiters();
    let resolution = task.await.unwrap().unwrap();
    assert_eq!(resolution.action, FileManagerConflictAction::Skip);
    assert!(resolution.apply_to_all);
    assert_eq!(
      progress.snapshot().state,
      FileManagerOperationState::Running
    );
  }

  #[tokio::test]
  async fn read_admission_does_not_wait_for_the_root_write_lock() {
    let key = format!("read-admission-{}", Uuid::new_v4());
    let write_lock = root_lock(&key).await.lock_owned().await;

    let value = tokio::time::timeout(
      std::time::Duration::from_secs(1),
      run_read_blocking(|| Ok(42_u8)),
    )
    .await
    .expect("bounded reads must remain responsive during a write")
    .unwrap();

    assert_eq!(value, 42);
    drop(write_lock);
  }

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

  #[cfg(unix)]
  #[test]
  fn private_journal_overlap_detects_aliased_parent() {
    use std::os::unix::fs::symlink;

    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    let private_data = directory.join("private-data");
    let private_journal = private_data.join("file-manager-journal");
    let aliased_volume = directory.join("docker-volume-data");
    let unrelated_volume = directory.join("application-volume");
    fs::create_dir_all(&private_journal).unwrap();
    fs::create_dir(&unrelated_volume).unwrap();
    symlink(&private_data, &aliased_volume).unwrap();

    let error = ensure_outside_private_journal(
      &aliased_volume,
      &private_journal,
    )
    .unwrap_err();
    assert_eq!(error.to_string(), PRIVATE_JOURNAL_OVERLAP_REASON);
    ensure_outside_private_journal(
      &unrelated_volume,
      &private_journal,
    )
    .unwrap();

    fs::remove_dir_all(directory).unwrap();
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
  fn metadata_file_revision_is_content_independent() {
    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    fs::create_dir(&directory).unwrap();
    fs::write(directory.join("config.txt"), "contents").unwrap();
    let root =
      Dir::open_ambient_dir(&directory, ambient_authority()).unwrap();
    let metadata = root.symlink_metadata("config.txt").unwrap();

    assert_eq!(
      metadata_tree_revision(&root, "config.txt").unwrap(),
      Some(revision(&metadata))
    );
    fs::remove_dir_all(directory).unwrap();
  }

  #[test]
  fn metadata_tree_revision_detects_nested_metadata_changes() {
    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    fs::create_dir_all(directory.join("nested/child")).unwrap();
    let child = directory.join("nested/child/file.txt");
    fs::write(&child, "one").unwrap();
    let root =
      Dir::open_ambient_dir(&directory, ambient_authority()).unwrap();
    let before = metadata_tree_revision(&root, "nested").unwrap();

    fs::write(&child, "longer contents").unwrap();
    let changed_file =
      metadata_tree_revision(&root, "nested").unwrap();
    fs::write(directory.join("nested/added.txt"), "added").unwrap();
    let changed_structure =
      metadata_tree_revision(&root, "nested").unwrap();

    assert_ne!(before, changed_file);
    assert_ne!(changed_file, changed_structure);
    fs::remove_dir_all(directory).unwrap();
  }

  #[test]
  fn capacity_thresholds_cover_shared_and_separate_filesystems() {
    let requirements = SpaceRequirements {
      journal_bytes: 10,
      target_bytes: 20,
    };

    assert_eq!(
      capacity_thresholds(requirements, true),
      (MINIMUM_FREE_BYTES + 30, None)
    );
    assert_eq!(
      capacity_thresholds(requirements, false),
      (MINIMUM_FREE_BYTES + 10, Some(MINIMUM_FREE_BYTES + 20))
    );
  }

  #[test]
  fn capacity_calculations_saturate_and_preserve_the_reserve() {
    let requirements = SpaceRequirements {
      journal_bytes: u64::MAX - 1,
      target_bytes: u64::MAX - 1,
    };

    assert_eq!(
      capacity_thresholds(requirements, true),
      (u64::MAX, None)
    );
    assert_eq!(
      capacity_thresholds(requirements, false),
      (u64::MAX, Some(u64::MAX))
    );
    assert!(
      validate_free_space(MINIMUM_FREE_BYTES, MINIMUM_FREE_BYTES)
        .is_ok()
    );
    assert!(
      validate_free_space(MINIMUM_FREE_BYTES - 1, MINIMUM_FREE_BYTES)
        .is_err()
    );
  }

  #[test]
  fn extraction_capacity_uses_target_local_staging_only() {
    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    fs::create_dir(&directory).unwrap();
    let file =
      fs::File::create(directory.join("archive.zip")).unwrap();
    let mut archive = zip::ZipWriter::new(file);
    archive
      .start_file(
        "file.txt",
        zip::write::SimpleFileOptions::default(),
      )
      .unwrap();
    archive.write_all(b"expanded").unwrap();
    archive.finish().unwrap().sync_all().unwrap();
    fs::create_dir(directory.join("output")).unwrap();
    fs::write(directory.join("output/existing.txt"), b"keep")
      .unwrap();
    let root =
      Dir::open_ambient_dir(&directory, ambient_authority()).unwrap();
    let operation = FileManagerOperation::ExtractArchive {
      path: "archive.zip".into(),
      destination: "output".into(),
    };

    assert_eq!(
      operation_space_requirements(&root, &operation, &[]).unwrap(),
      SpaceRequirements {
        journal_bytes: 0,
        target_bytes: b"expanded".len() as u64,
      }
    );
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
      apply_operation_planned(
        &root,
        None,
        &operation,
        &targets,
        &[],
        None,
      )
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
