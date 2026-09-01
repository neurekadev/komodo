use std::{
  collections::{HashMap, HashSet, VecDeque},
  fs,
  io::{Read as _, Write as _},
  path::{Component, Path, PathBuf},
  sync::{
    Arc, Mutex as StdMutex, OnceLock, RwLock as StdRwLock,
    atomic::{AtomicBool, Ordering},
  },
  time::{Duration, UNIX_EPOCH},
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
    FileManagerExecutionMode, FileManagerJournalStatus,
    FileManagerLimits, FileManagerOperation,
    FileManagerOperationPhase, FileManagerOperationState,
    FileManagerOperationStatus, FileManagerPendingConflict,
    FileManagerPreflight, FileManagerRevision, FileManagerTextFile,
  },
  komodo_timestamp, to_path_compatible_name,
};
use periphery_client::api::file_manager::{
  FileManagerManagedTransactionFinalizeAction,
  FileManagerManagedTransactionState,
  FileManagerManagedTransactionStatus, PeripheryFileManagerTarget,
  StartFileManagerUpload,
};
use periphery_client::{
  api::file_manager::StartFileManagerDownloadResponse,
  transport::{
    EncodedFileTransferMessage, EncodedTransportMessage,
    FileTransferMessage,
  },
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
  PRIVATE_STATE_DIRECTORY, open_dir_nofollow, open_parent_nofollow,
  relative_path, single_name,
};

pub const MAX_TEXT_BYTES: u64 = 4 * 1024 * 1024;
/// Kept in the public limits response for wire compatibility. A value of zero
/// means archive expansion is capacity-limited instead of fixed-size-limited.
pub const MAX_ARCHIVE_EXPANDED_BYTES: u64 = 0;
pub const MAX_ARCHIVE_EXPANSION_RATIO: u64 = 1_000;
pub const MINIMUM_FREE_BYTES: u64 = 256 * 1024 * 1024;
const PLAN_TTL_MS: i64 = 5 * 60 * 1_000;
const JOURNAL_TTL_MS: i64 = 24 * 60 * 60 * 1_000;
const FINALIZED_MANAGED_TRANSACTION_TTL_MS: i64 =
  30 * 24 * 60 * 60 * 1_000;
const MAX_HEAVY_JOBS: usize = 2;
const MAX_READ_JOBS: usize = 4;
const MAX_DOWNLOAD_CREDITS: u32 = 32;
const DOWNLOAD_HEARTBEAT_LEASE: Duration = Duration::from_secs(60);
const FILE_TRANSFER_FINAL_SEND_TIMEOUT: Duration =
  Duration::from_secs(1);
const PRIVATE_STATE_MARKER: &str = ".komodo-owner";
const PRIVATE_STATE_MARKER_CONTENTS: &[u8] =
  b"Komodo File Manager state v1\n";

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
  execution_mode: FileManagerExecutionMode,
  recursive_revisions: bool,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManagedTransactionRecord {
  operation_id: String,
  actor: String,
  root_key: String,
  plan_id: String,
  state: FileManagerManagedTransactionState,
  affected_paths: Vec<String>,
  #[serde(default)]
  finalized_at: Option<i64>,
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
  /// This managed journal is retained until Core explicitly commits or rolls
  /// back the corresponding database transaction.
  #[serde(default)]
  durable_managed: bool,
  #[serde(default)]
  recovery: bool,
  #[serde(default)]
  history_side: JournalHistorySide,
  #[serde(default)]
  transition: Option<JournalTransition>,
  created_at: i64,
  expires_at: i64,
  description: String,
  #[serde(default)]
  execution_mode: FileManagerExecutionMode,
  /// Operation-aware journals keep only namespace actions here. Legacy
  /// journals continue to use `snapshots` during their 24-hour lifetime.
  #[serde(default)]
  actions: Vec<JournalAction>,
  #[serde(default)]
  cleanup_only: bool,
  #[serde(default)]
  snapshots: Vec<JournalSnapshot>,
  #[serde(default)]
  before_revisions: Vec<(String, Option<FileManagerRevision>)>,
  after_revisions: Vec<(String, Option<FileManagerRevision>)>,
}

#[derive(
  Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
enum JournalActionState {
  #[default]
  Prepared,
  Applied,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum JournalActionKind {
  /// Move a visible entry into the operation's target-local quarantine.
  Quarantine {
    path: String,
    quarantine_name: String,
  },
  /// Rename a visible entry within the managed root.
  Relocate { from: String, to: String },
  /// An entry created by the operation. Undo moves it into quarantine so
  /// redo remains metadata-only.
  Created {
    path: String,
    quarantine_name: String,
  },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JournalAction {
  state: JournalActionState,
  kind: JournalActionKind,
}

#[derive(
  Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
enum JournalHistorySide {
  #[default]
  Undo,
  Redo,
}

#[derive(
  Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
enum JournalTransition {
  Undo,
  Redo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JournalSnapshot {
  path: String,
  existed: bool,
  backup_name: String,
  #[serde(default)]
  before_metadata: Vec<JournalEntryMetadata>,
  #[serde(default)]
  after_metadata: Vec<JournalEntryMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JournalEntryMetadata {
  path: String,
  mode: u32,
  uid: u32,
  gid: u32,
}

#[derive(Debug, Default)]
struct JournalHistory {
  undo: Vec<JournalRecord>,
  redo: Vec<JournalRecord>,
}

impl JournalHistory {
  fn insert_loaded(&mut self, record: JournalRecord) {
    if record.recovery
      || record.history_side == JournalHistorySide::Undo
    {
      self.undo.push(record);
    } else {
      // Records are loaded oldest-first. Redo is a stack whose newest
      // element must be replayed last, so insert older redo records at the
      // back-facing end in reverse order.
      self.redo.insert(0, record);
    }
  }
}

#[derive(Debug)]
struct RetainedJournalError {
  message: String,
  record: JournalRecord,
}

impl std::fmt::Display for RetainedJournalError {
  fn fmt(
    &self,
    formatter: &mut std::fmt::Formatter<'_>,
  ) -> std::fmt::Result {
    formatter.write_str(&self.message)
  }
}

impl std::error::Error for RetainedJournalError {}

struct TemporaryJournalDirectory {
  path: PathBuf,
  retained: bool,
}

impl Drop for TemporaryJournalDirectory {
  fn drop(&mut self) {
    if !self.retained {
      let _ = fs::remove_dir_all(&self.path);
    }
  }
}

struct TemporaryUpload<'a> {
  parent: &'a Dir,
  name: String,
  file: Option<cap_std::fs::File>,
  committed: bool,
}

struct StreamingUpload {
  parent: Dir,
  staging: Dir,
  temporary_name: String,
  file: Option<tokio::fs::File>,
  identity_file: cap_std::fs::File,
  staging_identity: FileIdentity,
  identity: FileIdentity,
  publish_metadata: Option<JournalEntryMetadata>,
  initial_revision: Option<FileManagerRevision>,
  committed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
  device: u64,
  inode: u64,
}

impl Drop for StreamingUpload {
  fn drop(&mut self) {
    self.file.take();
    let _ = self.staging.remove_file("payload");
    if self
      .parent
      .symlink_metadata(&self.temporary_name)
      .is_ok_and(|metadata| {
        metadata.is_dir()
          && !metadata.file_type().is_symlink()
          && file_identity(&metadata) == self.staging_identity
      })
    {
      let _ = self.parent.remove_dir(&self.temporary_name);
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

fn managed_transactions()
-> &'static Mutex<HashMap<String, ManagedTransactionRecord>> {
  static TRANSACTIONS: OnceLock<
    Mutex<HashMap<String, ManagedTransactionRecord>>,
  > = OnceLock::new();
  TRANSACTIONS.get_or_init(Default::default)
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
  let (mut records, transactions) = run_heavy_blocking(move || {
    ensure_private_directory(&root)?;
    let _ = fs::remove_dir_all(root.join("transfers"));
    recover_redo_invalidation_batches(&root)?;
    let managed_root = root.join("managed-transactions");
    ensure_private_directory(&managed_root)?;
    let mut transactions = Vec::new();
    for entry in fs::read_dir(&managed_root)? {
      let entry = entry?;
      if !entry.file_type()?.is_file()
        || entry.path().extension().and_then(|value| value.to_str())
          != Some("json")
      {
        continue;
      }
      if let Ok(bytes) = fs::read(entry.path()) {
        match serde_json::from_slice::<ManagedTransactionRecord>(
          &bytes,
        ) {
          Ok(record)
            if managed_transaction_is_prunable(
              &record,
              komodo_timestamp(),
            ) =>
          {
            let _ = fs::remove_file(entry.path());
          }
          Ok(record) => transactions.push(record),
          Err(error) => warn!(
            "Retaining unreadable managed transaction marker {}: {error:#}",
            entry.path().display()
          ),
        }
      }
    }
    let mut records = Vec::new();
    for entry in fs::read_dir(&root)? {
      let entry = entry?;
      if !entry.file_type()?.is_dir() {
        continue;
      }
      if entry.file_name() == "managed-transactions" {
        continue;
      }
      if entry.file_name().to_string_lossy().starts_with(".retired-")
      {
        let _ = fs::remove_dir_all(entry.path());
        continue;
      }
      let record = read_active_journal_record(&entry.path());
      match record {
        Some(record) if journal_is_unexpired(&record) => {
          records.push(record);
        }
        Some(record) => {
          if !record.actions.is_empty()
            && let Err(error) = remove_action_state(&record)
          {
            warn!(
              "Expired File Manager journal {} is retaining target recovery data for retry: {error:#}",
              record.id
            );
            continue;
          }
          let _ = fs::remove_dir_all(entry.path());
        }
        None => {
          // Preserve an unreadable active journal for manual recovery. A
          // retired journal has already been filtered above.
          warn!(
            "Retaining unreadable File Manager journal at {}",
            entry.path().display()
          );
        }
      }
    }
    Ok((records, transactions))
  })
  .await?;
  records.sort_by_key(|record| record.created_at);
  let mut loaded = histories().lock().await;
  for record in records {
    if record.cleanup_only {
      schedule_action_journal_cleanup(record);
      continue;
    }
    let history = loaded
      .entry(history_key(&record.root_key, &record.actor))
      .or_default();
    history.insert_loaded(record);
  }
  drop(loaded);
  let mut loaded_transactions = managed_transactions().lock().await;
  for transaction in transactions {
    loaded_transactions
      .insert(transaction.operation_id.clone(), transaction);
  }
  drop(loaded_transactions);

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
      prune_finalized_managed_transactions().await;
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

#[cfg(not(test))]
pub(super) fn max_entries() -> u64 {
  periphery_config().file_manager_max_entries.get()
}

#[cfg(test)]
pub(super) fn max_entries() -> u64 {
  1_000_000
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

fn managed_transaction_root() -> PathBuf {
  journal_root().join("managed-transactions")
}

fn managed_transaction_path(
  operation_id: &str,
) -> anyhow::Result<PathBuf> {
  let id = Uuid::parse_str(operation_id)
    .context("Managed transaction id is invalid")?;
  Ok(managed_transaction_root().join(format!("{id}.json")))
}

fn persist_managed_transaction(
  record: &ManagedTransactionRecord,
) -> anyhow::Result<()> {
  let root = managed_transaction_root();
  ensure_private_directory(&root)?;
  let destination = managed_transaction_path(&record.operation_id)?;
  let temporary = root.join(format!(
    ".{}.{}.tmp",
    record.operation_id,
    Uuid::new_v4()
  ));
  write_private_file(
    &temporary,
    &serde_json::to_vec_pretty(record)?,
  )?;
  fs::rename(&temporary, &destination)?;
  #[cfg(unix)]
  fs::File::open(&root)?.sync_all()?;
  Ok(())
}

fn managed_transaction_status_from_record(
  record: &ManagedTransactionRecord,
) -> FileManagerManagedTransactionStatus {
  FileManagerManagedTransactionStatus {
    operation_id: record.operation_id.clone(),
    state: record.state,
  }
}

fn managed_transaction_is_open(
  state: FileManagerManagedTransactionState,
) -> bool {
  !matches!(
    state,
    FileManagerManagedTransactionState::RolledBack
      | FileManagerManagedTransactionState::Committed
  )
}

fn managed_transaction_is_prunable(
  record: &ManagedTransactionRecord,
  now: i64,
) -> bool {
  matches!(
    record.state,
    FileManagerManagedTransactionState::RolledBack
      | FileManagerManagedTransactionState::Committed
  ) && record.finalized_at.is_some_and(|finalized_at| {
    finalized_at + FINALIZED_MANAGED_TRANSACTION_TTL_MS < now
  })
}

fn ensure_private_directory(path: &Path) -> anyhow::Result<()> {
  #[cfg(unix)]
  {
    use std::os::unix::fs::{
      DirBuilderExt as _, PermissionsExt as _,
    };
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true).mode(0o700).create(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
  }
  #[cfg(not(unix))]
  fs::create_dir_all(path)?;
  Ok(())
}

fn create_private_directory(path: &Path) -> anyhow::Result<()> {
  #[cfg(unix)]
  {
    use std::os::unix::fs::{
      DirBuilderExt as _, PermissionsExt as _,
    };
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700).create(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
  }
  #[cfg(not(unix))]
  fs::create_dir(path)?;
  Ok(())
}

fn create_private_capability_directory(
  parent: &Dir,
  name: &str,
) -> anyhow::Result<()> {
  let mut builder = cap_std::fs::DirBuilder::new();
  #[cfg(unix)]
  {
    use cap_std::fs::DirBuilderExt as _;
    builder.mode(0o700);
  }
  parent.create_dir_with(name, &builder)?;
  Ok(())
}

pub(super) fn create_private_file(
  path: &Path,
) -> anyhow::Result<fs::File> {
  let mut options = fs::OpenOptions::new();
  options.write(true).create_new(true);
  #[cfg(unix)]
  {
    use std::os::unix::fs::OpenOptionsExt as _;
    options.mode(0o600);
  }
  options.open(path).map_err(Into::into)
}

fn write_private_file(
  path: &Path,
  contents: &[u8],
) -> anyhow::Result<()> {
  let mut options = fs::OpenOptions::new();
  options.write(true).create(true).truncate(true);
  #[cfg(unix)]
  {
    use std::os::unix::fs::OpenOptionsExt as _;
    options.mode(0o600);
  }
  let mut file = options.open(path)?;
  #[cfg(unix)]
  {
    use std::os::unix::fs::PermissionsExt as _;
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
  }
  file.write_all(contents)?;
  file.sync_all()?;
  Ok(())
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

fn file_identity(metadata: &Metadata) -> FileIdentity {
  FileIdentity {
    device: metadata.dev(),
    inode: metadata.ino(),
  }
}

fn verify_upload_staging_identity(
  parent: &Dir,
  name: impl AsRef<Path>,
  expected: FileIdentity,
) -> anyhow::Result<()> {
  let name = name.as_ref();
  let metadata = parent.symlink_metadata(name).map_err(|error| {
    anyhow!(error)
      .context("Upload staging file moved while streaming")
  })?;
  if !metadata.is_file()
    || metadata.file_type().is_symlink()
    || file_identity(&metadata) != expected
  {
    return Err(anyhow!(
      "Upload staging file changed while streaming"
    ));
  }
  Ok(())
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
        supports_execution_modes: true,
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
      supports_execution_modes: true,
    },
    Err(error) => FileManagerCapabilities {
      available: false,
      read_only: true,
      reason: Some(error.to_string()),
      managed_file: None,
      limits: limits(),
      supports_execution_modes: true,
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
      if name.starts_with(".komodo-file-manager-staging-")
        || name.starts_with(".komodo-upload-")
        || name == PRIVATE_STATE_DIRECTORY
      {
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
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    let mut file = parent.open_with(&name, &options)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() {
      return Err(anyhow!("Path is not a regular file"));
    }
    if metadata.len() > MAX_TEXT_BYTES {
      return Err(anyhow!(
        "File is too large for the editor; download it instead"
      ));
    }
    let bytes = read_bounded(&mut file, MAX_TEXT_BYTES)?;
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

fn read_bounded(
  reader: &mut impl std::io::Read,
  maximum: u64,
) -> anyhow::Result<Vec<u8>> {
  let mut bytes = Vec::new();
  reader
    .take(maximum.saturating_add(1))
    .read_to_end(&mut bytes)?;
  if bytes.len() as u64 > maximum {
    return Err(anyhow!(
      "File is too large for the editor; download it instead"
    ));
  }
  Ok(bytes)
}

pub async fn preflight(
  target: &PeripheryFileManagerTarget,
  actor: String,
  operation: FileManagerOperation,
  execution_mode: FileManagerExecutionMode,
) -> anyhow::Result<FileManagerPreflight> {
  let root = resolve_root(target).await?;
  if root.read_only {
    return Err(anyhow!("This File Manager root is read-only"));
  }
  let operation = normalize_operation(operation)?;
  validate_operation(&root, &operation)?;
  let root_key = root.key.clone();
  let plan_root_key = root.key.clone();
  let (
    operation,
    conflicts,
    revisions,
    copy_targets,
    recursive_revisions,
  ) = run_root_blocking(&root_key, move || {
    let root_dir = match open_root(&root, false) {
      Ok(root) => Some(root),
      Err(_) if root.create_if_missing => None,
      Err(error) => return Err(error),
    };
    let root_dir = root_dir.as_ref();
    let copy_targets = resolve_copy_targets(root_dir, &operation)?;
    let conflicts =
      find_conflicts_planned(root_dir, &operation, &copy_targets)?;
    let recursive_revisions = root_dir
      .map(|root| {
        supports_action_journal(root, &operation, &copy_targets)
          .map(|supported| !supported)
      })
      .transpose()?
      .unwrap_or(false);
    let mut watched =
      revision_paths_planned(&operation, &copy_targets)?;
    watched.sort();
    watched.dedup();
    let revisions = watched
      .into_iter()
      .map(|path| {
        let rev = root_dir
          .map(|root| {
            if recursive_revisions {
              metadata_tree_revision(root, &path)
            } else {
              entry_metadata(root, &path).map(|metadata| {
                metadata.map(|metadata| revision(&metadata))
              })
            }
          })
          .transpose()?
          .flatten();
        anyhow::Ok((path, rev))
      })
      .collect::<anyhow::Result<Vec<_>>>()?;
    Ok((
      operation,
      conflicts,
      revisions,
      copy_targets,
      recursive_revisions,
    ))
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
      execution_mode,
      recursive_revisions,
    },
  );
  Ok(FileManagerPreflight {
    plan_id,
    expires_at,
    conflicts,
    confirmation_required,
    supports_durable_managed_transactions: true,
    execution_mode,
  })
}

pub async fn begin_managed_transaction(
  target: &PeripheryFileManagerTarget,
  actor: &str,
  operation_id: &str,
  plan_id: &str,
) -> anyhow::Result<FileManagerManagedTransactionStatus> {
  Uuid::parse_str(operation_id)
    .context("Managed transaction id is invalid")?;
  let root = resolve_root(target).await?;
  if root.read_only || root.managed_file.is_none() {
    return Err(anyhow!(
      "Crash-durable coordination is available only for managed files"
    ));
  }
  let plan = {
    let plans = plans().lock().await;
    let plan = plans.get(plan_id).context(
      "Preflight plan is missing, expired, or already consumed",
    )?;
    if plan.actor != actor {
      return Err(anyhow!("Preflight plan belongs to another user"));
    }
    if plan.root_key != root.key {
      return Err(anyhow!(
        "Preflight plan belongs to another File Manager target"
      ));
    }
    if plan.expires_at < komodo_timestamp() {
      return Err(anyhow!("Preflight plan has expired"));
    }
    if !operation_edits_managed_file(&root, &plan.operation) {
      return Err(anyhow!(
        "Crash-durable coordination requires a managed-file write"
      ));
    }
    plan.clone()
  };
  let mut transactions = managed_transactions().lock().await;
  if let Some(existing) = transactions.get(operation_id) {
    if existing.actor != actor
      || existing.root_key != root.key
      || existing.plan_id != plan_id
    {
      return Err(anyhow!(
        "Managed transaction id belongs to another operation"
      ));
    }
    return Ok(managed_transaction_status_from_record(existing));
  }
  if transactions.values().any(|transaction| {
    transaction.root_key == root.key
      && managed_transaction_is_open(transaction.state)
  }) {
    return Err(anyhow!(
      "Another managed save is still being reconciled for this stack"
    ));
  }
  let record = ManagedTransactionRecord {
    operation_id: operation_id.to_string(),
    actor: actor.to_string(),
    root_key: root.key,
    plan_id: plan_id.to_string(),
    state: FileManagerManagedTransactionState::Prepared,
    affected_paths: affected_paths_planned(
      &plan.operation,
      &plan.copy_targets,
    ),
    finalized_at: None,
  };
  persist_managed_transaction(&record)?;
  let status = managed_transaction_status_from_record(&record);
  transactions.insert(operation_id.to_string(), record);
  Ok(status)
}

pub async fn managed_transaction_status(
  target: &PeripheryFileManagerTarget,
  actor: &str,
  operation_id: &str,
) -> anyhow::Result<Option<FileManagerManagedTransactionStatus>> {
  Uuid::parse_str(operation_id)
    .context("Managed transaction id is invalid")?;
  let root = resolve_root(target).await?;
  let transactions = managed_transactions().lock().await;
  let Some(record) = transactions.get(operation_id) else {
    return Ok(None);
  };
  if record.actor != actor || record.root_key != root.key {
    return Err(anyhow!("Managed transaction was not found"));
  }
  Ok(Some(managed_transaction_status_from_record(record)))
}

async fn take_exact_managed_journal(
  root_key: &str,
  actor: &str,
  operation_id: &str,
) -> anyhow::Result<Option<JournalRecord>> {
  let key = history_key(root_key, actor);
  {
    let mut histories = histories().lock().await;
    if let Some(history) = histories.get_mut(&key) {
      if let Some(position) = history
        .undo
        .iter()
        .rposition(|record| record.id == operation_id)
      {
        return Ok(Some(history.undo.remove(position)));
      }
      if let Some(position) = history
        .redo
        .iter()
        .rposition(|record| record.id == operation_id)
      {
        return Ok(Some(history.redo.remove(position)));
      }
    }
  }
  let directory = journal_root().join(operation_id);
  if !directory.exists() {
    return Ok(None);
  }
  read_active_journal_record(&directory)
    .map(Some)
    .context(
      "Managed transaction journal exists but is unreadable; state is indeterminate",
    )
}

fn validate_exact_managed_journal(
  record: &JournalRecord,
  root: &ResolvedRoot,
  actor: &str,
  operation_id: &str,
) -> anyhow::Result<()> {
  if record.id != operation_id
    || record.actor != actor
    || record.root_key != root.key
    || !record.managed
    || !record.durable_managed
  {
    return Err(anyhow!(
      "Managed transaction journal does not match the requested operation"
    ));
  }
  Ok(())
}

async fn persist_managed_transaction_state(
  operation_id: &str,
  state: FileManagerManagedTransactionState,
) -> anyhow::Result<FileManagerManagedTransactionStatus> {
  let mut transactions = managed_transactions().lock().await;
  let record = transactions
    .get_mut(operation_id)
    .context("Managed transaction disappeared while finalizing")?;
  let previous_state = record.state;
  let previous_finalized_at = record.finalized_at;
  record.state = state;
  record.finalized_at = matches!(
    state,
    FileManagerManagedTransactionState::RolledBack
      | FileManagerManagedTransactionState::Committed
  )
  .then(komodo_timestamp);
  if let Err(error) = persist_managed_transaction(record) {
    record.state = previous_state;
    record.finalized_at = previous_finalized_at;
    return Err(error);
  }
  Ok(managed_transaction_status_from_record(record))
}

pub async fn finalize_managed_transaction(
  target: &PeripheryFileManagerTarget,
  actor: &str,
  operation_id: &str,
  action: FileManagerManagedTransactionFinalizeAction,
) -> anyhow::Result<FileManagerManagedTransactionStatus> {
  Uuid::parse_str(operation_id)
    .context("Managed transaction id is invalid")?;
  let root = resolve_root(target).await?;
  let lock = root_lock(&root.key).await;
  let _guard = lock.lock_owned().await;
  let initial_state = {
    let transactions = managed_transactions().lock().await;
    let record = transactions
      .get(operation_id)
      .context("Managed transaction was not found")?;
    if record.actor != actor || record.root_key != root.key {
      return Err(anyhow!("Managed transaction was not found"));
    }
    record.state
  };

  match action {
    FileManagerManagedTransactionFinalizeAction::Commit => {
      if initial_state
        == FileManagerManagedTransactionState::Committed
      {
        return Ok(FileManagerManagedTransactionStatus {
          operation_id: operation_id.to_string(),
          state: initial_state,
        });
      }
      if !matches!(
        initial_state,
        FileManagerManagedTransactionState::Applied
          | FileManagerManagedTransactionState::CommitRequested
      ) {
        return Err(anyhow!(
          "Managed transaction is not ready to commit"
        ));
      }
      let retrying = initial_state
        == FileManagerManagedTransactionState::CommitRequested;
      if !retrying {
        persist_managed_transaction_state(
          operation_id,
          FileManagerManagedTransactionState::CommitRequested,
        )
        .await?;
      }
      let journal =
        take_exact_managed_journal(&root.key, actor, operation_id)
          .await?;
      match journal {
        Some(record) => {
          validate_exact_managed_journal(
            &record,
            &root,
            actor,
            operation_id,
          )?;
          if record.recovery || record.transition.is_some() {
            store_journal_by_side(
              &history_key(&root.key, actor),
              record,
            )
            .await;
            return Err(anyhow!(
              "Managed transaction journal is not fully applied"
            ));
          }
          let id = record.id.clone();
          if let Err(error) =
            run_heavy_blocking(move || retire_journal_directory(&id))
              .await
          {
            store_journal_by_side(
              &history_key(&root.key, actor),
              record,
            )
            .await;
            return Err(error);
          }
        }
        None if !retrying => {
          return Err(anyhow!(
            "Managed transaction journal is missing; state is indeterminate"
          ));
        }
        None => {}
      }
      persist_managed_transaction_state(
        operation_id,
        FileManagerManagedTransactionState::Committed,
      )
      .await
    }
    FileManagerManagedTransactionFinalizeAction::Rollback => {
      if initial_state
        == FileManagerManagedTransactionState::RolledBack
      {
        return Ok(FileManagerManagedTransactionStatus {
          operation_id: operation_id.to_string(),
          state: initial_state,
        });
      }
      if matches!(
        initial_state,
        FileManagerManagedTransactionState::CommitRequested
          | FileManagerManagedTransactionState::Committed
      ) {
        return Err(anyhow!(
          "Committed managed transaction cannot be rolled back"
        ));
      }
      persist_managed_transaction_state(
        operation_id,
        FileManagerManagedTransactionState::RollbackRequested,
      )
      .await?;
      let journal =
        take_exact_managed_journal(&root.key, actor, operation_id)
          .await?;
      if let Some(record) = journal {
        validate_exact_managed_journal(
          &record,
          &root,
          actor,
          operation_id,
        )?;
        let rollback_root = root.clone();
        let outcome = run_heavy_blocking(move || {
          let mut record = record;
          let result = (|| {
            let root_dir = open_root(&rollback_root, true)?;
            if !record.recovery && record.transition.is_none() {
              verify_revisions(
                &root_dir,
                &record.after_revisions,
                "Managed rollback is unsafe because files changed after the operation",
              )?;
              begin_journal_transition(
                &mut record,
                JournalTransition::Undo,
              )?;
            }
            restore_journal(&root_dir, &record, None)?;
            retire_journal_directory(&record.id)
          })();
          Ok((record, result))
        })
        .await?;
        if let (record, Err(error)) = outcome {
          store_journal_by_side(
            &history_key(&root.key, actor),
            record,
          )
          .await;
          return Err(error);
        }
      } else if initial_state
        == FileManagerManagedTransactionState::Applied
      {
        return Err(anyhow!(
          "Applied managed transaction journal is missing; state is indeterminate"
        ));
      }
      persist_managed_transaction_state(
        operation_id,
        FileManagerManagedTransactionState::RolledBack,
      )
      .await
    }
  }
}

enum DurableCommitDisposition {
  Start,
  Existing(
    periphery_client::api::file_manager::FileManagerCommitResponse,
  ),
}

async fn prepare_durable_managed_commit(
  root: &ResolvedRoot,
  actor: &str,
  operation_id: &str,
  plan_id: &str,
) -> anyhow::Result<DurableCommitDisposition> {
  let mut transactions = managed_transactions().lock().await;
  let record = transactions
    .get_mut(operation_id)
    .context("Managed transaction handshake is missing")?;
  if record.actor != actor
    || record.root_key != root.key
    || record.plan_id != plan_id
  {
    return Err(anyhow!(
      "Managed transaction belongs to another operation"
    ));
  }
  match record.state {
    FileManagerManagedTransactionState::Prepared => {
      let previous = record.state;
      record.state = FileManagerManagedTransactionState::Applying;
      if let Err(error) = persist_managed_transaction(record) {
        record.state = previous;
        return Err(error);
      }
      Ok(DurableCommitDisposition::Start)
    }
    FileManagerManagedTransactionState::Applying
    | FileManagerManagedTransactionState::Applied => {
      Ok(DurableCommitDisposition::Existing(
        periphery_client::api::file_manager::FileManagerCommitResponse {
          operation_id: operation_id.to_string(),
          affected_paths: record.affected_paths.clone(),
          undoable: false,
        },
      ))
    }
    FileManagerManagedTransactionState::RollbackRequested
    | FileManagerManagedTransactionState::RolledBack => Err(anyhow!(
      "Managed transaction has already been rolled back"
    )),
    FileManagerManagedTransactionState::CommitRequested
    | FileManagerManagedTransactionState::Committed => {
      Err(anyhow!(
        "Managed transaction has already been committed"
      ))
    }
  }
}

async fn finish_durable_managed_apply(
  operation_id: &str,
  succeeded: bool,
  rollback_retained: bool,
) -> anyhow::Result<()> {
  let mut transactions = managed_transactions().lock().await;
  let record = transactions
    .get_mut(operation_id)
    .context("Managed transaction disappeared while applying")?;
  let previous = record.state;
  let previous_finalized_at = record.finalized_at;
  record.state = if succeeded {
    match record.state {
      FileManagerManagedTransactionState::Applying => {
        FileManagerManagedTransactionState::Applied
      }
      FileManagerManagedTransactionState::RollbackRequested => {
        return Err(anyhow!(
          "Managed transaction rollback was requested while applying"
        ));
      }
      state => {
        return Err(anyhow!(
          "Managed transaction reached invalid apply state {state:?}"
        ));
      }
    }
  } else if rollback_retained {
    FileManagerManagedTransactionState::RollbackRequested
  } else {
    FileManagerManagedTransactionState::RolledBack
  };
  record.finalized_at = (record.state
    == FileManagerManagedTransactionState::RolledBack)
    .then(komodo_timestamp);
  if let Err(error) = persist_managed_transaction(record) {
    record.state = previous;
    record.finalized_at = previous_finalized_at;
    return Err(error);
  }
  Ok(())
}

async fn ensure_durable_managed_apply_is_current(
  operation_id: &str,
) -> anyhow::Result<()> {
  let transactions = managed_transactions().lock().await;
  let record = transactions
    .get(operation_id)
    .context("Managed transaction disappeared before apply")?;
  if record.state != FileManagerManagedTransactionState::Applying {
    return Err(anyhow!(
      "Managed transaction is no longer authorized to apply"
    ));
  }
  Ok(())
}

pub async fn commit(
  target: &PeripheryFileManagerTarget,
  actor: &str,
  operation_id: &str,
  plan_id: &str,
  decisions: &[FileManagerConflictDecision],
  confirmed: bool,
  durable_managed: bool,
) -> anyhow::Result<
  periphery_client::api::file_manager::FileManagerCommitResponse,
> {
  let root = resolve_root(target).await?;
  if durable_managed {
    match prepare_durable_managed_commit(
      &root,
      actor,
      operation_id,
      plan_id,
    )
    .await?
    {
      DurableCommitDisposition::Start => {}
      DurableCommitDisposition::Existing(response) => {
        return Ok(response);
      }
    }
  }
  let plan = {
    let mut plans = plans().lock().await;
    match take_owned_plan(&mut plans, plan_id, actor) {
      Ok(plan) => plan,
      Err(error) => {
        drop(plans);
        if durable_managed {
          let _ =
            finish_durable_managed_apply(operation_id, false, false)
              .await;
        }
        return Err(error);
      }
    }
  };
  if plan.expires_at < komodo_timestamp() {
    return Err(anyhow!("Preflight plan has expired"));
  }
  if plan.confirmation_required && !confirmed {
    return Err(anyhow!("Explicit confirmation is required"));
  }
  validate_conflict_decisions(&plan.conflicts, decisions)?;

  if root.key != plan.root_key || root.read_only {
    return Err(anyhow!(
      "File Manager target changed after preflight"
    ));
  }
  if !durable_managed
    && operation_edits_managed_file(&root, &plan.operation)
    && managed_transactions().lock().await.values().any(
      |transaction| {
        transaction.root_key == root.key
          && managed_transaction_is_open(transaction.state)
      },
    )
  {
    return Err(anyhow!(
      "Another managed save is still being reconciled for this stack"
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
  let durable_operation_id = operation_id.clone();
  let decisions = decisions.to_vec();
  let job_progress = progress.clone();
  let response =
    periphery_client::api::file_manager::FileManagerCommitResponse {
      operation_id: operation_id.clone(),
      affected_paths: affected_paths_planned(
        &plan.operation,
        &plan.copy_targets,
      ),
      undoable: plan.operation.is_undoable()
        && plan.execution_mode
          == FileManagerExecutionMode::Recoverable,
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
    if durable_managed
      && let Err(error) =
        ensure_durable_managed_apply_is_current(&operation_id).await
    {
      progress.fail(&error);
      return;
    }
    let rollback_root = root.clone();
    let result = run_heavy_blocking(move || {
      job_progress.check_cancelled()?;
      job_progress.phase(
        FileManagerOperationPhase::Preparing,
        WorkTotal::default(),
      );
      let root_dir = open_root(&root, true)?;
      let use_action_journal = supports_action_journal(
        &root_dir,
        &plan.operation,
        &plan.copy_targets,
      )?;
      if use_action_journal {
        if matches!(&plan.operation, FileManagerOperation::Copy { .. }) {
          ensure_free_space(&root.path, MINIMUM_FREE_BYTES)?;
        }
        verify_plan_revisions(
          &root_dir,
          &plan.revisions,
          plan.recursive_revisions,
          Some(&job_progress),
        )?;
        job_progress.phase(
          FileManagerOperationPhase::Applying,
          action_operation_work(&plan.operation),
        );
        let mut journal = create_action_journal(
          &root,
          &actor,
          &operation_id,
          &plan.operation,
          plan.execution_mode,
          durable_managed,
        )?;
        let operation_result = apply_action_operation(
          &root_dir,
          &mut journal,
          &plan.operation,
          &plan.copy_targets,
          &decisions,
          Some(&job_progress),
        );
        if let Err(error) = operation_result {
          job_progress.phase(
            FileManagerOperationPhase::RollingBack,
            action_operation_work(&plan.operation),
          );
          let rollback = rollback_action_journal(&root_dir, &journal);
          if let Err(rollback_error) = rollback {
            return Err(RetainedJournalError {
              message: format!(
                "File operation failed: {error:#}; action rollback also failed: {rollback_error:#}"
              ),
              record: journal,
            }
            .into());
          }
          retire_action_journal(&journal)?;
          return Err(error.context("File operation was rolled back"));
        }
        job_progress.check_cancelled()?;
        let retain = journal.managed
          || (plan.operation.is_undoable()
            && plan.execution_mode
              == FileManagerExecutionMode::Recoverable);
        let journal = finish_action_journal(journal, retain)?;
        return Ok((Some(journal), guard));
      }
      verify_plan_revisions(
        &root_dir,
        &plan.revisions,
        plan.recursive_revisions,
        Some(&job_progress),
      )?;
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
        JournalCreateOptions {
          execution_mode: plan.execution_mode,
          durable_managed,
          progress: Some(&job_progress),
        },
      )?;
      job_progress.add_temporary_storage_bytes(snapshot_total.bytes);
      if let Err(error) = job_progress.check_cancelled() {
        retire_journal_directory(&journal.id).context(
          "Cancelled snapshot journal could not be retired",
        )?;
        return Err(error);
      }
      let operation_total = match operation_work(
        &root_dir,
        &plan.operation,
        &plan.copy_targets,
      ) {
        Ok(total) => total,
        Err(error) => {
          retire_journal_directory(&journal.id).context(
            "Unused snapshot journal could not be retired",
          )?;
          return Err(error);
        }
      };
      job_progress
        .phase(FileManagerOperationPhase::Applying, operation_total);
      if let Err(error) = verify_plan_revisions(
        &root_dir,
        &plan.revisions,
        plan.recursive_revisions,
        Some(&job_progress),
      ) {
        retire_journal_directory(&journal.id).context(
          "Stale preflight snapshot journal could not be retired",
        )?;
        return Err(error);
      }
      let operation_result = (|| {
        apply_operation_planned(
          &root_dir,
          &plan.operation,
          &plan.copy_targets,
          &decisions,
          Some(&job_progress),
        )?;
        job_progress.check_cancelled()?;
        if (plan.operation.is_undoable()
          && plan.execution_mode
            == FileManagerExecutionMode::Recoverable)
          || journal.managed
        {
          let finalizing_total = work_for_paths(
            &root_dir,
            &watched_paths_planned(
              &plan.operation,
              &plan.copy_targets,
            )?,
          )?;
          job_progress.phase(
            FileManagerOperationPhase::Finalizing,
            finalizing_total,
          );
          Ok(Some(finish_journal(
            &root_dir,
            journal.clone(),
            Some(&job_progress),
          )?))
        } else {
          Ok(Some(finish_action_journal(journal.clone(), false)?))
        }
      })();
      match operation_result {
        Ok(journal) => Ok((journal, guard)),
        Err(error) => {
          job_progress.phase(
            FileManagerOperationPhase::RollingBack,
            snapshot_total,
          );
          Err(rollback_or_retain(&root_dir, journal, error))
        }
      }
    })
    .await;
    let result = match result {
      Ok((Some(journal), _guard)) if journal.cleanup_only => {
        schedule_action_journal_cleanup(journal);
        Ok(())
      }
      Ok((Some(journal), _guard)) => {
        let rollback_record = journal.clone();
        match push_journal(journal).await {
          Ok(()) => Ok(()),
          Err(error) => {
            remove_journal_from_history(&rollback_record).await;
            let rollback_record_for_failure = rollback_record.clone();
            let rollback_record_for_runner = rollback_record.clone();
            let registration_error = format!("{error:#}");
            let rollback_error = match run_heavy_blocking(move || {
              let error = match open_root(&rollback_root, true) {
                Ok(root_dir) if !rollback_record.actions.is_empty() => {
                  match rollback_action_journal(
                    &root_dir,
                    &rollback_record,
                  ) {
                    Ok(()) => match retire_action_journal(&rollback_record) {
                      Ok(()) => error.context(
                        "File operation was rolled back after its undo history could not be registered",
                      ),
                      Err(cleanup_error) => RetainedJournalError {
                        message: format!(
                          "Undo history registration failed: {error:#}; rollback succeeded but cleanup failed: {cleanup_error:#}"
                        ),
                        record: rollback_record,
                      }
                      .into(),
                    },
                    Err(restore_error) => RetainedJournalError {
                      message: format!(
                        "Undo history registration failed: {error:#}; action rollback also failed: {restore_error:#}"
                      ),
                      record: rollback_record,
                    }
                    .into(),
                  }
                }
                Ok(root_dir) => rollback_or_retain(
                  &root_dir,
                  rollback_record,
                  error,
                ),
                Err(restore_error) => retain_after_rollback_failure(
                  rollback_record_for_failure,
                  error,
                  restore_error,
                ),
              };
              Ok(error)
            })
            .await
            {
              Ok(error) => error,
              Err(error) => retain_after_rollback_failure(
                rollback_record_for_runner,
                anyhow!(registration_error),
                error,
              ),
            };
            if let Some(record) = retained_journal(&rollback_error) {
              store_journal_by_side(
                &history_key(&record.root_key, &record.actor),
                record,
              )
              .await;
            }
            Err(rollback_error)
          }
        }
      }
      Ok((None, _guard)) => Ok(()),
      Err(error) => {
        if let Some(record) = retained_journal(&error) {
          store_journal_by_side(
            &history_key(&record.root_key, &record.actor),
            record,
          )
          .await;
        }
        Err(error)
      }
    };
    let rollback_retained =
      result.as_ref().err().and_then(retained_journal).is_some();
    let result = if durable_managed {
      match finish_durable_managed_apply(
        &durable_operation_id,
        result.is_ok(),
        rollback_retained,
      )
      .await
      {
        Ok(()) => result,
        Err(error) => Err(error),
      }
    } else {
      result
    };
    match &result {
      Ok(()) => progress.complete(),
      Err(error) => progress.fail(error),
    }
  });
  Ok(response)
}

fn take_owned_plan(
  plans: &mut HashMap<String, OperationPlan>,
  plan_id: &str,
  actor: &str,
) -> anyhow::Result<OperationPlan> {
  let plan = plans.get(plan_id).context(
    "Preflight plan is missing, expired, or already consumed",
  )?;
  if plan.actor != actor {
    return Err(anyhow!("Preflight plan belongs to another user"));
  }
  plans.remove(plan_id).context(
    "Preflight plan is missing, expired, or already consumed",
  )
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
  let (mut status, expired, retained_ids, has_action_journal) = {
    let mut histories = histories().lock().await;
    let history = histories.entry(key).or_default();
    let expired = prune_history(history);
    let visible_undo = history
      .undo
      .iter()
      .rev()
      .find(|record| journal_is_visible(record));
    let visible_redo = history
      .redo
      .iter()
      .rev()
      .find(|record| journal_is_visible(record));
    let status = FileManagerJournalStatus {
      can_undo: visible_undo.is_some(),
      can_redo: visible_redo.is_some(),
      undo_description: history
        .undo
        .iter()
        .rev()
        .find(|record| journal_is_visible(record))
        .map(|r| r.description.clone()),
      redo_description: history
        .redo
        .iter()
        .rev()
        .find(|record| journal_is_visible(record))
        .map(|record| record.description.clone()),
      expires_at: visible_undo
        .filter(|record| !record.recovery)
        .map(|record| record.expires_at),
      retained_storage_bytes: 0,
      retained_storage_bytes_exact: Some(true),
      storage_description: "Recovery manifests use Periphery private storage. Large deleted or overwritten trees are retained in the target's hidden .komodo-file-manager directory so namespace changes remain fast.".into(),
    };
    let retained_ids = history
      .undo
      .iter()
      .chain(&history.redo)
      .filter(|record| record.actions.is_empty())
      .map(|record| record.id.clone())
      .collect::<Vec<_>>();
    let has_action_journal = history
      .undo
      .iter()
      .chain(&history.redo)
      .any(|record| !record.actions.is_empty());
    (status, expired, retained_ids, has_action_journal)
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
  status.retained_storage_bytes_exact = Some(!has_action_journal);
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
    let position = match rollback_operation_id {
      Some(id) => {
        history.undo.iter().rposition(|record| record.id == id)
      }
      None => history.undo.iter().rposition(journal_is_visible),
    };
    let record = position
      .map(|position| history.undo.remove(position))
      .with_context(|| {
        if rollback_operation_id.is_some() {
          "Requested File Manager rollback is unavailable"
        } else {
          "Nothing is available to undo"
        }
      });
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
      affected_paths: if record.actions.is_empty() {
        record
          .snapshots
          .iter()
          .map(|snapshot| snapshot.path.clone())
          .collect()
      } else {
        action_paths(&record)
      },
      undoable: !record.recovery,
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
        if !record.actions.is_empty() {
          job_progress.phase(
            if record.recovery {
              FileManagerOperationPhase::RollingBack
            } else {
              FileManagerOperationPhase::Applying
            },
            WorkTotal {
              entries: record.actions.len() as u64,
              bytes: 0,
            },
          );
          if record.recovery {
            rollback_action_journal(&root_dir, &record)?;
            retire_action_journal(&record)?;
            return Ok(());
          }
          undo_action_journal(
            &root_dir,
            &mut record,
            Some(&job_progress),
          )?;
          return Ok(());
        }
        if record.recovery {
          job_progress.phase(
            FileManagerOperationPhase::Applying,
            restore_total,
          );
          ensure_free_space(
            &root.path,
            restore_total.bytes.saturating_add(MINIMUM_FREE_BYTES),
          )?;
          restore_journal(&root_dir, &record, Some(&job_progress))?;
          retire_journal_directory(&record.id)?;
          return Ok(());
        }
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
          &mut record,
          Some(&job_progress),
        )?;
        begin_journal_transition(
          &mut record,
          JournalTransition::Undo,
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
        complete_journal_transition(
          &mut record,
          JournalHistorySide::Redo,
        )?;
        Ok(())
      })();
      Ok((record, result, guard))
    })
    .await;
    let (record, result, _guard) = match outcome {
      Ok(outcome) => outcome,
      Err(error) => {
        store_journal_by_side(&key, fallback_record).await;
        progress.fail(&error);
        return;
      }
    };
    if let Err(error) = result {
      store_journal_by_side(&key, record).await;
      progress.fail(&error);
      return;
    }
    if record.recovery {
      progress.complete();
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
      .rposition(journal_is_visible)
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
      affected_paths: if record.actions.is_empty() {
        record
          .snapshots
          .iter()
          .map(|snapshot| snapshot.path.clone())
          .collect()
      } else {
        action_paths(&record)
      },
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
        if !record.actions.is_empty() {
          let root_dir = open_root(&root, true)?;
          job_progress.phase(
            FileManagerOperationPhase::Applying,
            WorkTotal {
              entries: record.actions.len() as u64,
              bytes: 0,
            },
          );
          redo_action_journal(
            &root_dir,
            &mut record,
            Some(&job_progress),
          )?;
          return Ok(());
        }
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
        begin_journal_transition(
          &mut record,
          JournalTransition::Redo,
        )?;
        restore_after_journal(
          &root_dir,
          &record,
          Some(&job_progress),
        )?;
        record.after_revisions =
          capture_snapshot_revisions(&root_dir, &record.snapshots)?;
        complete_journal_transition(
          &mut record,
          JournalHistorySide::Undo,
        )?;
        Ok(())
      })();
      Ok((record, result, guard))
    })
    .await;
    let (record, result, _guard) = match outcome {
      Ok(outcome) => outcome,
      Err(error) => {
        store_journal_by_side(&key, fallback_record).await;
        progress.fail(&error);
        return;
      }
    };
    if let Err(error) = result {
      store_journal_by_side(&key, record).await;
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

async fn send_file_transfer_final_with_timeout(
  sender: &transport::channel::Sender<EncodedTransportMessage>,
  channel: Uuid,
  data: anyhow::Result<Vec<u8>>,
  timeout: Duration,
) {
  let _ = tokio::time::timeout(
    timeout,
    sender.send_file_transfer(channel, data),
  )
  .await;
}

fn spawn_file_transfer_final(
  sender: transport::channel::Sender<EncodedTransportMessage>,
  channel: Uuid,
  data: anyhow::Result<Vec<u8>>,
) {
  tokio::spawn(async move {
    send_file_transfer_final_with_timeout(
      &sender,
      channel,
      data,
      FILE_TRANSFER_FINAL_SEND_TIMEOUT,
    )
    .await;
  });
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
      let prepare_root = root.clone();
      let prepare_relative = relative.clone();
      let prepare_path = path.clone();
      let prepare_expected_revision = expected_revision.clone();
      let mut upload = run_heavy_blocking(move || {
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
        let publish_metadata = match parent
          .symlink_metadata(&destination_name)
        {
          Ok(metadata)
            if metadata.is_file()
              && !metadata.file_type().is_symlink() =>
          {
            if initial_revision.as_ref() != Some(&revision(&metadata))
            {
              return Err(anyhow!(
                "Upload destination changed while preparing"
              ));
            }
            recorded_file_metadata(&metadata)
          }
          Ok(_) => None,
          Err(error)
            if error.kind() == std::io::ErrorKind::NotFound =>
          {
            None
          }
          Err(error) => return Err(error.into()),
        };
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
          format!(".komodo-upload-{}", Uuid::new_v4());
        create_private_capability_directory(
          &parent,
          &temporary_name,
        )?;
        let prepared = (|| {
          let staging = parent.open_dir_nofollow(&temporary_name)?;
          let staging_identity =
            file_identity(&staging.dir_metadata()?);
          let mut options = OpenOptions::new();
          options
            .write(true)
            .create_new(true)
            .follow(FollowSymlinks::No);
          let file = staging.open_with("payload", &options)?;
          let identity = file_identity(&file.metadata()?);
          let identity_file = file.try_clone()?;
          anyhow::Ok((
            staging,
            staging_identity,
            file,
            identity_file,
            identity,
          ))
        })();
        if prepared.is_err() {
          let _ = parent.remove_dir_all(&temporary_name);
        }
        let (
          staging,
          staging_identity,
          file,
          identity_file,
          identity,
        ) = prepared?;
        Ok(StreamingUpload {
          parent,
          staging,
          temporary_name,
          file: Some(tokio::fs::File::from_std(file.into_std())),
          identity_file,
          staging_identity,
          identity,
          publish_metadata,
          initial_revision,
          committed: false,
        })
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
            let file = upload
              .file
              .as_mut()
              .context("Upload staging file is unavailable")?;
            file.flush().await?;
            file.sync_all().await?;
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
            let finalize_relative = relative.clone();
            progress.check_cancelled()?;
            let lock = root_lock(&root.key).await;
            let guard = lock.lock_owned().await;
            let finalize_progress = progress.clone();
            let (message, _guard, cleanup) =
              run_heavy_blocking(move || {
              finalize_progress.check_cancelled()?;
              let root_dir = open_root(&finalize_root, true)?;
              if metadata_tree_revision(&root_dir, &finalize_path)?
                != upload.initial_revision
              {
                return Err(anyhow!(
                  "Upload destination changed while streaming"
                ));
              }
              let (publish_parent, destination_name) =
                open_parent_nofollow(&root_dir, &finalize_relative)?;
              let staging_metadata = publish_parent
                .symlink_metadata(&upload.temporary_name)
                .map_err(|error| {
                  anyhow!(error).context(
                    "Upload staging directory moved while streaming",
                  )
                })?;
              if !staging_metadata.is_dir()
                || staging_metadata.file_type().is_symlink()
                || file_identity(&staging_metadata)
                  != upload.staging_identity
              {
                return Err(anyhow!(
                  "Upload staging directory changed while streaming"
                ));
              }
              verify_upload_staging_identity(
                &upload.staging,
                "payload",
                upload.identity,
              )?;
              let operation = FileManagerOperation::CreateFile {
                path: finalize_path.clone(),
              };
              let journal = create_journal(
                &finalize_root,
                &finalize_actor,
                &Uuid::new_v4().to_string(),
                &operation,
                &[],
                JournalCreateOptions {
                  execution_mode:
                    FileManagerExecutionMode::Recoverable,
                  durable_managed: false,
                  progress: None,
                },
              )?;
              if metadata_tree_revision(&root_dir, &finalize_path)?
                != upload.initial_revision
              {
                retire_journal_directory(&journal.id).context(
                  "Unused upload journal could not be retired",
                )?;
                return Err(anyhow!(
                  "Upload destination changed while finalizing"
                ));
              }
              let commit = (|| {
                if let Some(metadata) = &upload.publish_metadata {
                  apply_recorded_file_metadata(
                    &upload.identity_file,
                    metadata,
                    false,
                  )?;
                  upload.identity_file.sync_all()?;
                }
                let destination_metadata = match publish_parent
                  .symlink_metadata(&destination_name)
                {
                  Ok(metadata) => Some(metadata),
                  Err(error)
                    if error.kind()
                      == std::io::ErrorKind::NotFound =>
                  {
                    None
                  }
                  Err(error) => return Err(error.into()),
                };
                if let Some(metadata) = destination_metadata {
                  if !overwrite {
                    return Err(anyhow!(
                      "Upload destination changed while streaming"
                    ));
                  }
                  if !metadata.is_file()
                    || metadata.file_type().is_symlink()
                  {
                    remove_entry(&publish_parent, &destination_name)?;
                  }
                }
                verify_upload_staging_identity(
                  &upload.staging,
                  "payload",
                  upload.identity,
                )?;
                upload.staging.rename(
                  "payload",
                  &publish_parent,
                  &destination_name,
                )?;
                upload.committed = true;
                verify_upload_staging_identity(
                  &publish_parent,
                  &destination_name,
                  upload.identity,
                )?;
                anyhow::Ok(())
              })();
              if let Err(error) = commit {
                return Err(rollback_or_retain(
                  &root_dir, journal, error,
                ));
              }
              let cleanup = match finish_action_journal(
                journal.clone(),
                false,
              ) {
                Ok(cleanup) => cleanup,
                Err(error) => {
                return Err(rollback_or_retain(
                  &root_dir,
                  journal,
                  error.context(
                    "Published upload journal could not be finalized for cleanup",
                  ),
                ));
                }
              };
              Ok((
                FileTransferMessage::Complete {
                  bytes: received,
                  sha256: actual,
                },
                guard,
                cleanup,
              ))
            })
            .await?;
            schedule_action_journal_cleanup(cleanup);
            progress.add_entry();
            return Ok(message);
          }
          FileTransferMessage::Cancel => {
            return Err(anyhow!("Upload was cancelled"));
          }
          FileTransferMessage::Begin
          | FileTransferMessage::BeginWithCredit { .. }
          | FileTransferMessage::BeginWithCreditAndHeartbeat {
            ..
          }
          | FileTransferMessage::Credit { .. }
          | FileTransferMessage::Heartbeat => {
            return Err(anyhow!("Unexpected upload control message"));
          }
        }
      }
    }
    .await;
    if let Some(record) =
      result.as_ref().err().and_then(retained_journal)
    {
      store_journal_by_side(
        &history_key(&record.root_key, &record.actor),
        record,
      )
      .await;
    }
    match &result {
      Ok(_) => progress.complete(),
      Err(error) if error.to_string().contains("cancel") => {
        progress.cancel(error.to_string())
      }
      Err(error) => progress.fail(error),
    }
    file_transfer_channels().remove(&channel).await;
    spawn_file_transfer_final(
      connection.sender.clone(),
      channel,
      result.map(FileTransferMessage::into_raw),
    );
  });
  Ok(channel)
}

pub async fn start_download(
  core: &str,
  target: PeripheryFileManagerTarget,
  actor: String,
  operation_id: String,
  paths: Vec<String>,
  allow_managed: bool,
) -> anyhow::Result<StartFileManagerDownloadResponse> {
  if paths.is_empty() {
    return Err(anyhow!("Select at least one entry to download"));
  }
  let root = resolve_root(&target).await?;
  if root
    .managed_file
    .as_ref()
    .is_some_and(|managed| paths.iter().any(|path| path == managed))
    && !allow_managed
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
    ensure_private_directory(&staging)?;
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
      let (mut available_credits, heartbeat_enabled) =
        download_begin_mode(begin)?;
      let mut file = tokio::fs::File::open(&staged).await?;
      let mut buffer = vec![0_u8; 256 * 1024];
      let mut sent = 0_u64;
      use tokio::io::AsyncReadExt as _;
      loop {
        progress.check_cancelled()?;
        if available_credits == Some(0) {
          available_credits = Some(
            wait_for_download_credit(
              &mut receiver,
              &progress,
              heartbeat_enabled,
            )
            .await?,
          );
        }
        loop {
          match receiver.try_recv() {
            Ok(Ok(FileTransferMessage::Credit { credits }))
              if available_credits.is_some() =>
            {
              available_credits = Some(add_download_credits(
                available_credits.unwrap_or_default(),
                credits,
              )?);
            }
            Ok(Ok(FileTransferMessage::Cancel)) => {
              return Err(anyhow!("Download was cancelled"));
            }
            Ok(Ok(FileTransferMessage::Heartbeat))
              if heartbeat_enabled => {}
            Ok(Ok(_)) => {
              return Err(anyhow!(
                "Unexpected download control message"
              ));
            }
            Ok(Err(error)) => return Err(error),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
              break;
            }
            Err(
              tokio::sync::mpsc::error::TryRecvError::Disconnected,
            ) => {
              return Err(anyhow!("Download channel closed"));
            }
          }
        }
        let read = file.read(&mut buffer).await?;
        if read == 0 {
          break;
        }
        sent += read as u64;
        progress.add_bytes(read as u64);
        let send = connection.sender.send_file_transfer(
          channel,
          Ok(
            FileTransferMessage::Chunk(buffer[..read].to_vec())
              .into_raw(),
          ),
        );
        if heartbeat_enabled {
          tokio::time::timeout(DOWNLOAD_HEARTBEAT_LEASE, send)
            .await
            .context(
              "Download connection liveness expired while sending",
            )??;
        } else {
          send.await?;
        }
        if let Some(credits) = &mut available_credits {
          *credits -= 1;
        }
      }
      if sent != total_bytes {
        return Err(anyhow!(
          "Download byte count changed while streaming"
        ));
      }
      progress.check_cancelled()?;
      anyhow::Ok(FileTransferMessage::Complete {
        bytes: sent,
        sha256,
      })
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
    let _ = run_heavy_blocking(move || {
      let _ = fs::remove_dir_all(staging);
      Ok(())
    })
    .await;
    spawn_file_transfer_final(
      connection.sender.clone(),
      channel,
      result.map(FileTransferMessage::into_raw),
    );
  });
  Ok(StartFileManagerDownloadResponse {
    channel,
    file_name,
    total_bytes,
    sha256: hex::encode(sha256),
    supports_download_credit: true,
    supports_download_heartbeat: true,
  })
}

fn add_download_credits(
  current: u32,
  additional: u32,
) -> anyhow::Result<u32> {
  if additional == 0 {
    return Err(anyhow!("Download credit must be greater than zero"));
  }
  Ok(current.saturating_add(additional).min(MAX_DOWNLOAD_CREDITS))
}

fn download_begin_mode(
  begin: FileTransferMessage,
) -> anyhow::Result<(Option<u32>, bool)> {
  match begin {
    FileTransferMessage::Begin => Ok((None, false)),
    FileTransferMessage::BeginWithCredit { credits } => {
      Ok((Some(add_download_credits(0, credits)?), false))
    }
    FileTransferMessage::BeginWithCreditAndHeartbeat { credits } => {
      Ok((Some(add_download_credits(0, credits)?), true))
    }
    _ => Err(anyhow!("Download did not begin correctly")),
  }
}

async fn wait_for_download_credit(
  receiver: &mut tokio::sync::mpsc::Receiver<
    anyhow::Result<FileTransferMessage>,
  >,
  progress: &OperationProgress,
  heartbeat_enabled: bool,
) -> anyhow::Result<u32> {
  wait_for_download_credit_with_lease(
    receiver,
    progress,
    heartbeat_enabled,
    DOWNLOAD_HEARTBEAT_LEASE,
  )
  .await
}

async fn wait_for_download_credit_with_lease(
  receiver: &mut tokio::sync::mpsc::Receiver<
    anyhow::Result<FileTransferMessage>,
  >,
  progress: &OperationProgress,
  heartbeat_enabled: bool,
  lease: Duration,
) -> anyhow::Result<u32> {
  let mut deadline = tokio::time::Instant::now() + lease;
  loop {
    progress.check_cancelled()?;
    let message = tokio::select! {
      biased;
      _ = tokio::time::sleep_until(deadline), if heartbeat_enabled => {
        return Err(anyhow!("Download connection heartbeat expired"));
      }
      message = receiver.recv() => {
        message.context("Download channel closed")??
      }
      _ = tokio::time::sleep(Duration::from_millis(250)) => {
        continue;
      }
    };
    match message {
      FileTransferMessage::Credit { credits } => {
        return add_download_credits(0, credits);
      }
      FileTransferMessage::Cancel => {
        return Err(anyhow!("Download was cancelled"));
      }
      FileTransferMessage::Heartbeat if heartbeat_enabled => {
        deadline = tokio::time::Instant::now() + lease;
      }
      _ => {
        return Err(anyhow!("Unexpected download control message"));
      }
    }
  }
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
) -> anyhow::Result<FileManagerOperation> {
  if let FileManagerOperation::CreateArchive {
    destination,
    format,
    ..
  } = &mut operation
  {
    *destination = ensure_archive_extension(destination, *format);
  }
  match &mut operation {
    FileManagerOperation::Delete { paths }
    | FileManagerOperation::Move { paths, .. } => {
      *paths = normalize_redundant_paths(paths)?;
    }
    _ => {}
  }
  Ok(operation)
}

fn normalize_redundant_paths(
  paths: &[String],
) -> anyhow::Result<Vec<String>> {
  let parsed = paths
    .iter()
    .map(|path| relative_path(path, false))
    .collect::<anyhow::Result<Vec<_>>>()?;
  let mut normalized = Vec::new();
  for (index, path) in parsed.iter().enumerate() {
    let duplicate = parsed[..index].contains(path);
    let has_selected_ancestor = parsed
      .iter()
      .any(|other| other != path && path.starts_with(other));
    if !duplicate && !has_selected_ancestor {
      normalized.push(path_string(path)?);
    }
  }
  Ok(normalized)
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
    operation,
    &copy_targets,
    decisions,
    None,
  )
}

fn apply_operation_planned(
  root: &Dir,
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
      let existing_metadata = match parent.symlink_metadata(&name) {
        Ok(metadata) => {
          if !metadata.is_file() || metadata.file_type().is_symlink()
          {
            return Err(anyhow!("Path is not a regular file"));
          }
          let mut read_options = OpenOptions::new();
          read_options.read(true).follow(FollowSymlinks::No);
          let mut current = parent.open_with(&name, &read_options)?;
          let metadata = current.metadata()?;
          let mut current_bytes = Vec::new();
          current.read_to_end(&mut current_bytes)?;
          if content_revision(&metadata, &current_bytes)
            != *expected_revision
          {
            return Err(anyhow!(
              "File changed since it was opened; reload before saving"
            ));
          }
          Some(metadata)
        }
        Err(error)
          if error.kind() == std::io::ErrorKind::NotFound
            && expected_revision.id.is_empty() =>
        {
          None
        }
        Err(error) => return Err(error.into()),
      };

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
      {
        let file = temporary_write
          .file
          .as_mut()
          .context("Text staging file is unavailable")?;
        file.write_all(contents.as_bytes())?;
        file.sync_data()?;
      }
      if let Some(metadata) = existing_metadata.as_ref() {
        apply_capability_file_metadata(
          temporary_write
            .file
            .as_ref()
            .context("Text staging file is unavailable")?,
          metadata,
          false,
        )?;
      }
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
      archive::extract(root, path, destination, decisions, progress)?
    }
  }
  Ok(())
}

fn supports_action_journal(
  root: &Dir,
  operation: &FileManagerOperation,
  copy_targets: &[CopyTarget],
) -> anyhow::Result<bool> {
  let root_device = root.dir_metadata()?.dev();
  let destination_is_local = |path: &str| -> anyhow::Result<bool> {
    let path = relative_path(path, false)?;
    let (parent, _) = open_parent_nofollow(root, &path)?;
    Ok(parent.dir_metadata()?.dev() == root_device)
  };
  let entry_is_local = |path: &str| -> anyhow::Result<bool> {
    let path = relative_path(path, false)?;
    let (parent, name) = open_parent_nofollow(root, &path)?;
    Ok(parent.symlink_metadata(name)?.dev() == root_device)
  };
  let destination_can_be_replaced =
    |path: &str| -> anyhow::Result<bool> {
      if !destination_is_local(path)? {
        return Ok(false);
      }
      Ok(!visible_exists(root, path)? || entry_is_local(path)?)
    };
  match operation {
    FileManagerOperation::CreateFile { path }
    | FileManagerOperation::CreateDirectory { path } => {
      destination_can_be_replaced(path)
    }
    FileManagerOperation::Rename { path, new_name } => {
      let source = relative_path(path, false)?;
      let destination = path_string(
        &source
          .parent()
          .unwrap_or_else(|| Path::new(""))
          .join(single_name(new_name)?),
      )?;
      Ok(
        entry_is_local(path)?
          && destination_can_be_replaced(&destination)?,
      )
    }
    FileManagerOperation::Delete { paths } => {
      for path in paths {
        if !entry_is_local(path)? {
          return Ok(false);
        }
      }
      Ok(true)
    }
    FileManagerOperation::Move { paths, destination } => {
      let destination = relative_path(destination, true)?;
      for source in paths {
        let source = relative_path(source, false)?;
        let (source_parent, source_name) =
          open_parent_nofollow(root, &source)?;
        let source_metadata =
          source_parent.symlink_metadata(&source_name)?;
        let target = destination.join(&source_name);
        let (target_parent, target_name) =
          open_parent_nofollow(root, &target)?;
        if source_metadata.dev() != root_device
          || target_parent.dir_metadata()?.dev() != root_device
        {
          return Ok(false);
        }
        if let Ok(target_metadata) =
          target_parent.symlink_metadata(&target_name)
          && (target_metadata.dev() != root_device
            || (source_metadata.is_dir()
              && !source_metadata.file_type().is_symlink()
              && target_metadata.is_dir()
              && !target_metadata.file_type().is_symlink()))
        {
          return Ok(false);
        }
      }
      Ok(true)
    }
    FileManagerOperation::Copy { .. } => {
      for target in copy_targets {
        if visible_exists(root, &target.destination)?
          || !destination_is_local(&target.destination)?
        {
          return Ok(false);
        }
      }
      Ok(true)
    }
    FileManagerOperation::WriteText { .. }
    | FileManagerOperation::CreateArchive { .. }
    | FileManagerOperation::ExtractArchive { .. } => Ok(false),
  }
}

fn action_operation_work(
  operation: &FileManagerOperation,
) -> WorkTotal {
  let entries = match operation {
    FileManagerOperation::Delete { paths }
    | FileManagerOperation::Move { paths, .. }
    | FileManagerOperation::Copy { paths, .. } => paths.len() as u64,
    _ => 1,
  };
  WorkTotal { entries, bytes: 0 }
}

fn apply_action_operation(
  root: &Dir,
  record: &mut JournalRecord,
  operation: &FileManagerOperation,
  copy_targets: &[CopyTarget],
  decisions: &[FileManagerConflictDecision],
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  match operation {
    FileManagerOperation::CreateFile { path } => {
      if visible_exists(root, path)? {
        match decision_for(path, decisions) {
          Some(FileManagerConflictAction::Skip) => return Ok(()),
          Some(FileManagerConflictAction::Overwrite) => {
            quarantine_visible(root, record, path)?;
          }
          None => return Err(anyhow!("Destination already exists")),
        }
      }
      let action = prepare_created_action(record, path)?;
      let path = relative_path(path, false)?;
      let (parent, name) = open_parent_nofollow(root, &path)?;
      let mut options = OpenOptions::new();
      options
        .write(true)
        .create_new(true)
        .follow(FollowSymlinks::No);
      parent.open_with(name, &options)?.sync_all()?;
      sync_capability_directory(&parent)?;
      mark_action_applied(record, action)?;
    }
    FileManagerOperation::CreateDirectory { path } => {
      if visible_exists(root, path)? {
        match decision_for(path, decisions) {
          Some(FileManagerConflictAction::Skip) => return Ok(()),
          Some(FileManagerConflictAction::Overwrite) => {
            quarantine_visible(root, record, path)?;
          }
          None => return Err(anyhow!("Destination already exists")),
        }
      }
      let action = prepare_created_action(record, path)?;
      let path = relative_path(path, false)?;
      let (parent, name) = open_parent_nofollow(root, &path)?;
      parent.create_dir(name)?;
      sync_capability_directory(&parent)?;
      mark_action_applied(record, action)?;
    }
    FileManagerOperation::Rename { path, new_name } => {
      let source = relative_path(path, false)?;
      let destination = source
        .parent()
        .unwrap_or_else(|| Path::new(""))
        .join(single_name(new_name)?);
      let destination = path_string(&destination)?;
      if visible_exists(root, &destination)? {
        match decision_for(&destination, decisions) {
          Some(FileManagerConflictAction::Skip) => return Ok(()),
          Some(FileManagerConflictAction::Overwrite) => {
            quarantine_visible(root, record, &destination)?;
          }
          None => return Err(anyhow!("Destination already exists")),
        }
      }
      relocate_visible(root, record, path, &destination)?;
    }
    FileManagerOperation::Move { paths, destination } => {
      let destination_path = relative_path(destination, true)?;
      for source in paths {
        if let Some(progress) = progress {
          progress.check_cancelled()?;
        }
        let source_path = relative_path(source, false)?;
        let target = path_string(
          &destination_path.join(
            source_path
              .file_name()
              .context("Source path has no filename")?,
          ),
        )?;
        if visible_exists(root, &target)? {
          match decision_for(&target, decisions) {
            Some(FileManagerConflictAction::Skip) => continue,
            Some(FileManagerConflictAction::Overwrite) => {
              quarantine_visible(root, record, &target)?;
            }
            None => {
              return Err(anyhow!("Destination already exists"));
            }
          }
        }
        relocate_visible(root, record, source, &target)?;
        if let Some(progress) = progress {
          progress.add_entry();
        }
      }
      return Ok(());
    }
    FileManagerOperation::Copy { .. } => {
      let operation_dir =
        action_operation_directory(root, &record.id, false)?;
      let staging = operation_dir.open_dir_nofollow("staging")?;
      let mut copied_entries = 0_u64;
      for (index, target) in copy_targets.iter().enumerate() {
        if let Some(progress) = progress {
          progress.check_cancelled()?;
        }
        let source = relative_path(&target.source, false)?;
        let destination = relative_path(&target.destination, false)?;
        let (source_parent, source_name) =
          open_parent_nofollow(root, &source)?;
        let staging_name = format!("copy-{index}");
        copy_entry_counted(
          &source_parent,
          &source_name,
          &staging,
          std::ffi::OsStr::new(&staging_name),
          progress,
          &mut copied_entries,
        )?;
        let action =
          prepare_created_action(record, &target.destination)?;
        let (destination_parent, destination_name) =
          open_parent_nofollow(root, &destination)?;
        staging.rename(
          &staging_name,
          &destination_parent,
          destination_name,
        )?;
        sync_capability_directory(&staging)?;
        sync_capability_directory(&destination_parent)?;
        mark_action_applied(record, action)?;
      }
      return Ok(());
    }
    FileManagerOperation::Delete { paths } => {
      for path in paths {
        if let Some(progress) = progress {
          progress.check_cancelled()?;
        }
        quarantine_visible(root, record, path)?;
        if let Some(progress) = progress {
          progress.add_entry();
        }
      }
      return Ok(());
    }
    FileManagerOperation::WriteText { .. }
    | FileManagerOperation::CreateArchive { .. }
    | FileManagerOperation::ExtractArchive { .. } => {
      return Err(anyhow!(
        "Operation requires the legacy transaction path"
      ));
    }
  }
  if let Some(progress) = progress {
    progress.add_entry();
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
  let mut copied_entries = 0_u64;
  copy_entry_counted(
    source_parent,
    source_name,
    destination_parent,
    destination_name,
    progress,
    &mut copied_entries,
  )
}

fn copy_entry_counted(
  source_parent: &Dir,
  source_name: &std::ffi::OsStr,
  destination_parent: &Dir,
  destination_name: &std::ffi::OsStr,
  progress: Option<&OperationProgress>,
  copied_entries: &mut u64,
) -> anyhow::Result<()> {
  *copied_entries = copied_entries.saturating_add(1);
  ensure_entry_limit(*copied_entries)?;
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
    let source_metadata = source.metadata()?;
    let mut write_options = OpenOptions::new();
    write_options
      .write(true)
      .create_new(true)
      .follow(FollowSymlinks::No);
    let mut destination = destination_parent
      .open_with(destination_name, &write_options)?;
    let mut buffer = [0_u8; 128 * 1024];
    loop {
      let read = source.read(&mut buffer)?;
      if read == 0 {
        break;
      }
      destination.write_all(&buffer[..read])?;
      if let Some(progress) = progress {
        progress.add_bytes(read as u64);
      }
    }
    apply_capability_file_metadata(
      &destination,
      &source_metadata,
      false,
    )?;
    destination.sync_all()?;
  } else if metadata.is_dir() {
    let source = source_parent.open_dir_nofollow(source_name)?;
    let source_metadata = source.dir_metadata()?;
    destination_parent.create_dir(destination_name)?;
    let destination =
      destination_parent.open_dir_nofollow(destination_name)?;
    for entry in source.entries()? {
      let entry = entry?;
      let name = entry.file_name();
      copy_entry_counted(
        &source,
        &name,
        &destination,
        &name,
        progress,
        copied_entries,
      )?;
    }
    let destination_file = destination.open(".")?;
    apply_capability_file_metadata(
      &destination_file,
      &source_metadata,
      true,
    )?;
    destination_file.sync_all()?;
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

#[cfg(unix)]
fn apply_capability_file_metadata(
  file: &cap_std::fs::File,
  metadata: &Metadata,
  preserve_privilege_bits: bool,
) -> anyhow::Result<()> {
  use cap_std::fs::PermissionsExt as _;
  use std::os::fd::AsRawFd as _;

  let current = file.metadata()?;
  let uid = cap_std::fs::MetadataExt::uid(metadata);
  let gid = cap_std::fs::MetadataExt::gid(metadata);
  if cap_std::fs::MetadataExt::uid(&current) != uid
    || cap_std::fs::MetadataExt::gid(&current) != gid
  {
    let result = unsafe {
      libc::fchown(
        file.as_raw_fd(),
        uid as libc::uid_t,
        gid as libc::gid_t,
      )
    };
    if result != 0 {
      return Err(std::io::Error::last_os_error())
        .context("Failed to preserve filesystem ownership");
    }
  }
  let mode = cap_std::fs::MetadataExt::mode(metadata)
    & if preserve_privilege_bits {
      0o7777
    } else {
      0o1777
    };
  file.set_permissions(cap_std::fs::Permissions::from_mode(mode))?;
  Ok(())
}

#[cfg(not(unix))]
fn apply_capability_file_metadata(
  _file: &cap_std::fs::File,
  _metadata: &Metadata,
  _preserve_privilege_bits: bool,
) -> anyhow::Result<()> {
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
  watched_paths_planned(operation, copy_targets)
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

fn verify_plan_revisions(
  root: &Dir,
  revisions: &[(String, Option<FileManagerRevision>)],
  recursive: bool,
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  for (path, expected) in revisions {
    if let Some(progress) = progress {
      progress.check_cancelled()?;
    }
    let actual = if recursive {
      metadata_tree_revision(root, path)?
    } else {
      entry_metadata(root, path)?.map(|metadata| revision(&metadata))
    };
    if &actual != expected {
      return Err(anyhow!(
        "File Manager contents changed after preflight; retry the operation"
      ));
    }
  }
  Ok(())
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

fn action_state_directory(root: &Dir) -> anyhow::Result<Dir> {
  let created = match root.symlink_metadata(PRIVATE_STATE_DIRECTORY) {
    Ok(metadata)
      if metadata.is_dir() && !metadata.file_type().is_symlink() =>
    {
      #[cfg(unix)]
      if cap_std::fs::MetadataExt::uid(&metadata)
        != unsafe { libc::geteuid() }
      {
        return Err(anyhow!(
          "Reserved File Manager recovery path is not owned by Periphery"
        ));
      }
      false
    }
    Ok(_) => {
      return Err(anyhow!(
        "Reserved File Manager recovery path is not a directory"
      ));
    }
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
      create_private_capability_directory(
        root,
        PRIVATE_STATE_DIRECTORY,
      )?;
      sync_capability_directory(root)?;
      true
    }
    Err(error) => return Err(error.into()),
  };
  let state = root
    .open_dir_nofollow(PRIVATE_STATE_DIRECTORY)
    .context("File Manager recovery state is inaccessible")?;
  if created {
    let mut options = OpenOptions::new();
    options
      .write(true)
      .create_new(true)
      .follow(FollowSymlinks::No);
    let mut marker =
      state.open_with(PRIVATE_STATE_MARKER, &options)?;
    marker.write_all(PRIVATE_STATE_MARKER_CONTENTS)?;
    #[cfg(unix)]
    {
      use cap_std::fs::PermissionsExt as _;
      marker.set_permissions(cap_std::fs::Permissions::from_mode(
        0o600,
      ))?;
    }
    marker.sync_all()?;
    sync_capability_directory(&state)?;
  } else {
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    let marker = state
      .open_with(PRIVATE_STATE_MARKER, &options)
      .context(
        "Reserved File Manager recovery path is not managed by Periphery",
      )?;
    let mut contents = Vec::new();
    marker
      .take((PRIVATE_STATE_MARKER_CONTENTS.len() + 1) as u64)
      .read_to_end(&mut contents)?;
    if contents != PRIVATE_STATE_MARKER_CONTENTS {
      return Err(anyhow!(
        "Reserved File Manager recovery path has an invalid ownership marker"
      ));
    }
  }
  #[cfg(unix)]
  {
    use cap_std::fs::PermissionsExt as _;
    let directory = state.open(".")?;
    directory
      .set_permissions(cap_std::fs::Permissions::from_mode(0o700))?;
    directory.sync_all()?;
  }
  Ok(state)
}

fn action_operation_directory(
  root: &Dir,
  id: &str,
  create: bool,
) -> anyhow::Result<Dir> {
  let state = action_state_directory(root)?;
  if create {
    create_private_capability_directory(&state, id)?;
    sync_capability_directory(&state)?;
    let operation = state.open_dir_nofollow(id)?;
    create_private_capability_directory(&operation, "quarantine")?;
    create_private_capability_directory(&operation, "staging")?;
    sync_capability_directory(&operation)?;
    Ok(operation)
  } else {
    state.open_dir_nofollow(id).map_err(Into::into)
  }
}

fn action_quarantine_directory(
  root: &Dir,
  id: &str,
) -> anyhow::Result<Dir> {
  action_operation_directory(root, id, false)?
    .open_dir_nofollow("quarantine")
    .map_err(Into::into)
}

fn sync_capability_directory(directory: &Dir) -> anyhow::Result<()> {
  #[cfg(unix)]
  directory.open(".")?.sync_all()?;
  Ok(())
}

fn rename_visible(
  root: &Dir,
  from: &str,
  to: &str,
) -> anyhow::Result<()> {
  let from = relative_path(from, false)?;
  let to = relative_path(to, false)?;
  let (from_parent, from_name) = open_parent_nofollow(root, &from)?;
  let (to_parent, to_name) = open_parent_nofollow(root, &to)?;
  from_parent.rename(from_name, &to_parent, to_name)?;
  sync_capability_directory(&from_parent)?;
  sync_capability_directory(&to_parent)?;
  Ok(())
}

fn visible_exists(root: &Dir, path: &str) -> anyhow::Result<bool> {
  let path = relative_path(path, false)?;
  let (parent, name) = open_parent_nofollow(root, &path)?;
  match parent.symlink_metadata(name) {
    Ok(_) => Ok(true),
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
      Ok(false)
    }
    Err(error) => Err(error.into()),
  }
}

fn append_action(
  record: &mut JournalRecord,
  kind: JournalActionKind,
) -> anyhow::Result<usize> {
  record.actions.push(JournalAction {
    state: JournalActionState::Prepared,
    kind,
  });
  persist_journal_record(record)?;
  Ok(record.actions.len() - 1)
}

fn mark_action_applied(
  record: &mut JournalRecord,
  index: usize,
) -> anyhow::Result<()> {
  record.actions[index].state = JournalActionState::Applied;
  persist_journal_record(record)
}

fn quarantine_visible(
  root: &Dir,
  record: &mut JournalRecord,
  path: &str,
) -> anyhow::Result<()> {
  let quarantine_name = record.actions.len().to_string();
  let index = append_action(
    record,
    JournalActionKind::Quarantine {
      path: path.to_string(),
      quarantine_name: quarantine_name.clone(),
    },
  )?;
  let relative = relative_path(path, false)?;
  let (parent, name) = open_parent_nofollow(root, &relative)?;
  let quarantine = action_quarantine_directory(root, &record.id)?;
  parent.rename(name, &quarantine, &quarantine_name)?;
  sync_capability_directory(&parent)?;
  sync_capability_directory(&quarantine)?;
  mark_action_applied(record, index)
}

fn relocate_visible(
  root: &Dir,
  record: &mut JournalRecord,
  from: &str,
  to: &str,
) -> anyhow::Result<()> {
  let index = append_action(
    record,
    JournalActionKind::Relocate {
      from: from.to_string(),
      to: to.to_string(),
    },
  )?;
  rename_visible(root, from, to)?;
  mark_action_applied(record, index)
}

fn prepare_created_action(
  record: &mut JournalRecord,
  path: &str,
) -> anyhow::Result<usize> {
  append_action(
    record,
    JournalActionKind::Created {
      path: path.to_string(),
      quarantine_name: format!("created-{}", record.actions.len()),
    },
  )
}

fn create_action_journal(
  root: &ResolvedRoot,
  actor: &str,
  id: &str,
  operation: &FileManagerOperation,
  execution_mode: FileManagerExecutionMode,
  durable_managed: bool,
) -> anyhow::Result<JournalRecord> {
  let root_dir = open_root(root, true)?;
  let journal_root = journal_root();
  ensure_private_directory(&journal_root)?;
  let central_directory = journal_root.join(id);
  create_private_directory(&central_directory)?;
  #[cfg(unix)]
  if let Err(error) =
    fs::File::open(&journal_root).and_then(|root| root.sync_all())
  {
    let _ = fs::remove_dir(&central_directory);
    return Err(error.into());
  }
  let result = (|| {
    action_operation_directory(&root_dir, id, true)?;
    let created_at = komodo_timestamp();
    let record = JournalRecord {
      id: id.to_string(),
      actor: actor.to_string(),
      root_key: root.key.clone(),
      root_path: root.path.clone(),
      managed: operation_edits_managed_file(root, operation),
      durable_managed: durable_managed
        && operation_edits_managed_file(root, operation),
      recovery: true,
      history_side: JournalHistorySide::Undo,
      transition: None,
      created_at,
      expires_at: created_at + JOURNAL_TTL_MS,
      description: operation_description(operation),
      execution_mode,
      actions: Vec::new(),
      cleanup_only: false,
      snapshots: Vec::new(),
      before_revisions: Vec::new(),
      after_revisions: Vec::new(),
    };
    persist_journal_record(&record)?;
    Ok(record)
  })();
  if result.is_err() {
    let _ = fs::remove_dir_all(
      root.path.join(PRIVATE_STATE_DIRECTORY).join(id),
    );
    let _ = fs::remove_dir_all(&central_directory);
  }
  result
}

fn finish_action_journal(
  mut record: JournalRecord,
  retain: bool,
) -> anyhow::Result<JournalRecord> {
  record.recovery = false;
  record.cleanup_only = !retain;
  persist_journal_record(&record)?;
  Ok(record)
}

fn remove_action_state(record: &JournalRecord) -> anyhow::Result<()> {
  let parent = record.root_path.join(PRIVATE_STATE_DIRECTORY);
  let path = parent.join(&record.id);
  match fs::remove_dir_all(path) {
    Ok(()) => {
      #[cfg(unix)]
      fs::File::open(parent)?.sync_all()?;
      Ok(())
    }
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
      Ok(())
    }
    Err(error) => Err(error.into()),
  }
}

fn retire_action_journal(
  record: &JournalRecord,
) -> anyhow::Result<()> {
  let mut cleanup = record.clone();
  cleanup.recovery = false;
  cleanup.cleanup_only = true;
  persist_journal_record(&cleanup)?;
  remove_action_state(&cleanup)?;
  retire_journal_directory(&record.id)
}

fn schedule_action_journal_cleanup(record: JournalRecord) {
  tokio::spawn(async move {
    let id = record.id.clone();
    let result = run_heavy_blocking(move || {
      remove_action_state(&record)?;
      retire_journal_directory(&record.id)
    })
    .await;
    if let Err(error) = result {
      warn!(
        "Failed to clean up File Manager operation {id}: {error:#}"
      );
    }
  });
}

fn action_was_applied(
  root: &Dir,
  record: &JournalRecord,
  action: &JournalAction,
) -> anyhow::Result<bool> {
  if action.state == JournalActionState::Applied {
    return Ok(true);
  }
  let quarantine = action_quarantine_directory(root, &record.id)?;
  match &action.kind {
    JournalActionKind::Quarantine {
      path,
      quarantine_name,
    } => Ok(
      !visible_exists(root, path)?
        && quarantine.symlink_metadata(quarantine_name).is_ok(),
    ),
    JournalActionKind::Relocate { from, to } => {
      Ok(!visible_exists(root, from)? && visible_exists(root, to)?)
    }
    JournalActionKind::Created { path, .. } => {
      visible_exists(root, path)
    }
  }
}

fn rollback_action_journal(
  root: &Dir,
  record: &JournalRecord,
) -> anyhow::Result<()> {
  let quarantine = action_quarantine_directory(root, &record.id)?;
  for action in record.actions.iter().rev() {
    if !action_was_applied(root, record, action)? {
      continue;
    }
    match &action.kind {
      JournalActionKind::Quarantine {
        path,
        quarantine_name,
      } => {
        let relative = relative_path(path, false)?;
        let (parent, name) = open_parent_nofollow(root, &relative)?;
        quarantine.rename(quarantine_name, &parent, name)?;
        sync_capability_directory(&quarantine)?;
        sync_capability_directory(&parent)?;
      }
      JournalActionKind::Relocate { from, to } => {
        rename_visible(root, to, from)?;
      }
      JournalActionKind::Created { path, .. } => {
        let relative = relative_path(path, false)?;
        let (parent, name) = open_parent_nofollow(root, &relative)?;
        if parent.symlink_metadata(&name).is_ok() {
          remove_entry(&parent, name)?;
        }
      }
    }
  }
  Ok(())
}

fn action_paths(record: &JournalRecord) -> Vec<String> {
  let mut paths = record
    .actions
    .iter()
    .flat_map(|action| match &action.kind {
      JournalActionKind::Quarantine { path, .. }
      | JournalActionKind::Created { path, .. } => vec![path.clone()],
      JournalActionKind::Relocate { from, to } => {
        vec![from.clone(), to.clone()]
      }
    })
    .collect::<Vec<_>>();
  paths.sort();
  paths.dedup();
  paths
}

fn ensure_visible_absent(
  root: &Dir,
  path: &str,
) -> anyhow::Result<()> {
  if visible_exists(root, path)? {
    Err(anyhow!("Undo or redo is unsafe because {path} now exists"))
  } else {
    Ok(())
  }
}

fn undo_action_journal(
  root: &Dir,
  record: &mut JournalRecord,
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  begin_journal_transition(record, JournalTransition::Undo)?;
  let quarantine = action_quarantine_directory(root, &record.id)?;
  for action in record.actions.iter().rev() {
    if let Some(progress) = progress {
      progress.check_cancelled()?;
    }
    match &action.kind {
      JournalActionKind::Quarantine {
        path,
        quarantine_name,
      } => {
        ensure_visible_absent(root, path)?;
        let relative = relative_path(path, false)?;
        let (parent, name) = open_parent_nofollow(root, &relative)?;
        quarantine.rename(quarantine_name, &parent, name)?;
        sync_capability_directory(&quarantine)?;
        sync_capability_directory(&parent)?;
      }
      JournalActionKind::Relocate { from, to } => {
        ensure_visible_absent(root, from)?;
        rename_visible(root, to, from)?;
      }
      JournalActionKind::Created {
        path,
        quarantine_name,
      } => {
        let relative = relative_path(path, false)?;
        let (parent, name) = open_parent_nofollow(root, &relative)?;
        parent.rename(name, &quarantine, quarantine_name)?;
        sync_capability_directory(&parent)?;
        sync_capability_directory(&quarantine)?;
      }
    }
    if let Some(progress) = progress {
      progress.add_entry();
    }
  }
  complete_journal_transition(record, JournalHistorySide::Redo)
}

fn redo_action_journal(
  root: &Dir,
  record: &mut JournalRecord,
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  begin_journal_transition(record, JournalTransition::Redo)?;
  let quarantine = action_quarantine_directory(root, &record.id)?;
  for action in &record.actions {
    if let Some(progress) = progress {
      progress.check_cancelled()?;
    }
    match &action.kind {
      JournalActionKind::Quarantine {
        path,
        quarantine_name,
      } => {
        let relative = relative_path(path, false)?;
        let (parent, name) = open_parent_nofollow(root, &relative)?;
        parent.rename(name, &quarantine, quarantine_name)?;
        sync_capability_directory(&parent)?;
        sync_capability_directory(&quarantine)?;
      }
      JournalActionKind::Relocate { from, to } => {
        ensure_visible_absent(root, to)?;
        rename_visible(root, from, to)?;
      }
      JournalActionKind::Created {
        path,
        quarantine_name,
      } => {
        ensure_visible_absent(root, path)?;
        let relative = relative_path(path, false)?;
        let (parent, name) = open_parent_nofollow(root, &relative)?;
        quarantine.rename(quarantine_name, &parent, name)?;
        sync_capability_directory(&quarantine)?;
        sync_capability_directory(&parent)?;
      }
    }
    if let Some(progress) = progress {
      progress.add_entry();
    }
  }
  complete_journal_transition(record, JournalHistorySide::Undo)
}

#[derive(Clone, Copy)]
struct JournalCreateOptions<'a> {
  execution_mode: FileManagerExecutionMode,
  durable_managed: bool,
  progress: Option<&'a OperationProgress>,
}

fn create_journal(
  root: &ResolvedRoot,
  actor: &str,
  id: &str,
  operation: &FileManagerOperation,
  copy_targets: &[CopyTarget],
  options: JournalCreateOptions<'_>,
) -> anyhow::Result<JournalRecord> {
  let journal_root = journal_root();
  ensure_private_directory(&journal_root)?;
  let operation_directory = journal_root.join(id);
  create_private_directory(&operation_directory)?;
  let mut cleanup = TemporaryJournalDirectory {
    path: operation_directory.clone(),
    retained: false,
  };
  let result = (|| {
    let directory = operation_directory.join("before");
    create_private_directory(&directory)?;
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
      let before_metadata = if existed {
        capture_journal_metadata(&root_dir, &relative)?
      } else {
        Vec::new()
      };
      if existed {
        backup_from_capability(
          &root_dir,
          &relative,
          &backup,
          options.progress,
        )?;
      }
      snapshots.push(JournalSnapshot {
        path: path.clone(),
        existed,
        backup_name,
        before_metadata,
        after_metadata: Vec::new(),
      });
      before_revisions.push((path, before_revision));
    }
    let created_at = komodo_timestamp();
    let record = JournalRecord {
      id: id.to_string(),
      actor: actor.to_string(),
      root_key: root.key.clone(),
      root_path: root.path.clone(),
      managed: operation_edits_managed_file(root, operation),
      durable_managed: options.durable_managed
        && operation_edits_managed_file(root, operation),
      recovery: true,
      history_side: JournalHistorySide::Undo,
      transition: None,
      created_at,
      expires_at: created_at + JOURNAL_TTL_MS,
      description: operation_description(operation),
      execution_mode: options.execution_mode,
      actions: Vec::new(),
      cleanup_only: false,
      snapshots,
      before_revisions,
      after_revisions: Vec::new(),
    };
    let mut recovery_record = record.clone();
    recovery_record.description =
      format!("Recover interrupted {}", record.description);
    persist_journal_record(&recovery_record)?;
    Ok(record)
  })();
  match result {
    Ok(record) => {
      cleanup.retained = true;
      Ok(record)
    }
    Err(error) => match fs::remove_dir_all(&operation_directory) {
      Ok(()) => {
        cleanup.retained = true;
        Err(error)
      }
      Err(cleanup_error)
        if cleanup_error.kind() == std::io::ErrorKind::NotFound =>
      {
        cleanup.retained = true;
        Err(error)
      }
      Err(cleanup_error) => Err(error.context(format!(
        "Partial snapshot journal could not be removed: {cleanup_error:#}"
      ))),
    },
  }
}

fn operation_edits_managed_file(
  root: &ResolvedRoot,
  operation: &FileManagerOperation,
) -> bool {
  matches!(
    operation,
    FileManagerOperation::WriteText { path, .. }
      if root.managed_file.as_deref() == Some(path.as_str())
  )
}

fn finish_journal(
  root: &Dir,
  mut record: JournalRecord,
  _progress: Option<&OperationProgress>,
) -> anyhow::Result<JournalRecord> {
  let mut after_revisions = Vec::new();
  for snapshot in &record.snapshots {
    let revision = tree_revision(root, &snapshot.path)?;
    after_revisions.push((snapshot.path.clone(), revision));
  }
  record.after_revisions = after_revisions;
  record.recovery = false;
  record.history_side = JournalHistorySide::Undo;
  persist_journal_record(&record)?;
  Ok(record)
}

fn persist_journal_record(
  record: &JournalRecord,
) -> anyhow::Result<()> {
  let directory = journal_root().join(&record.id);
  let manifest = serde_json::to_vec_pretty(record)?;
  let temporary = directory.join("manifest.json.tmp");
  write_private_file(&temporary, &manifest)?;
  fs::rename(&temporary, directory.join("manifest.json"))?;
  #[cfg(unix)]
  fs::File::open(&directory)?.sync_all()?;
  Ok(())
}

fn begin_journal_transition(
  record: &mut JournalRecord,
  transition: JournalTransition,
) -> anyhow::Result<()> {
  let mut durable = record.clone();
  durable.transition = Some(transition);
  persist_journal_record(&durable)?;
  record.transition = Some(transition);
  Ok(())
}

fn complete_journal_transition(
  record: &mut JournalRecord,
  side: JournalHistorySide,
) -> anyhow::Result<()> {
  let transition = record.transition;
  record.transition = None;
  record.history_side = side;
  record.recovery = false;
  if let Err(error) = persist_journal_record(record) {
    record.transition = transition;
    *record = normalize_loaded_journal(record.clone());
    return Err(error);
  }
  Ok(())
}

fn remove_journal_directory(id: &str) -> anyhow::Result<()> {
  match fs::remove_dir_all(journal_root().join(id)) {
    Ok(()) => Ok(()),
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
      Ok(())
    }
    Err(error) => Err(error.into()),
  }
}

fn retire_journal_directory(id: &str) -> anyhow::Result<()> {
  let directory = journal_root().join(id);
  if !directory.exists() {
    return Ok(());
  }
  let retired =
    journal_root().join(format!(".retired-{}", Uuid::new_v4()));
  match fs::rename(&directory, &retired) {
    Ok(()) => {
      #[cfg(unix)]
      if let Err(error) = fs::File::open(journal_root())
        .and_then(|directory| directory.sync_all())
      {
        match fs::rename(&retired, &directory) {
          Ok(()) => {
            let _ = fs::File::open(journal_root())
              .and_then(|directory| directory.sync_all());
            return Err(anyhow!(error).context(
              "Retired File Manager journal directory metadata could not be synced",
            ));
          }
          Err(restore_error) => {
            warn!(
              "Retired File Manager journal directory metadata could not be synced, but its startup-invisible name could not be reverted: {error:#}; revert failed: {restore_error:#}"
            );
          }
        }
      }
      if let Err(error) = fs::remove_dir_all(&retired) {
        warn!(
          "Retired File Manager journal awaits cleanup: {error:#}"
        );
      }
      return Ok(());
    }
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
      return Ok(());
    }
    Err(_) => {}
  }
  let tombstone = directory.join("retired");
  match write_private_file(&tombstone, b"retired\n") {
    Ok(()) => {
      #[cfg(unix)]
      if let Err(error) = fs::File::open(&directory)
        .and_then(|directory| directory.sync_all())
      {
        return Err(anyhow!(error).context(
          "Retired File Manager journal tombstone metadata could not be synced",
        ));
      }
      if let Err(error) = remove_journal_directory(id) {
        warn!(
          "Retired File Manager journal {id} awaits cleanup: {error:#}"
        );
      }
      Ok(())
    }
    Err(tombstone_error) => match remove_journal_directory(id) {
      Ok(()) => Ok(()),
      Err(cleanup_error) => Err(anyhow!(
        "Could not retire File Manager journal {id}: tombstone failed: {tombstone_error:#}; cleanup failed: {cleanup_error:#}"
      )),
    },
  }
}

fn rollback_or_retain(
  root: &Dir,
  record: JournalRecord,
  operation_error: anyhow::Error,
) -> anyhow::Error {
  match restore_journal(root, &record, None) {
    Ok(()) => {
      if let Err(error) = retire_journal_directory(&record.id) {
        warn!(
          "Failed to retire rolled-back File Manager journal {}: {error:#}",
          record.id
        );
      }
      operation_error
    }
    Err(rollback_error) => retain_after_rollback_failure(
      record,
      operation_error,
      rollback_error,
    ),
  }
}

fn retain_after_rollback_failure(
  mut record: JournalRecord,
  operation_error: anyhow::Error,
  rollback_error: anyhow::Error,
) -> anyhow::Error {
  let tombstone = journal_root().join(&record.id).join("retired");
  let reactivation_error = match fs::remove_file(&tombstone) {
    Ok(()) => fs::File::open(journal_root().join(&record.id))
      .and_then(|directory| directory.sync_all())
      .err(),
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
      None
    }
    Err(error) => Some(error),
  };
  record.recovery = true;
  record.history_side = JournalHistorySide::Undo;
  record.transition = None;
  if !record.description.starts_with("Recover interrupted ") {
    record.description =
      format!("Recover interrupted {}", record.description);
  }
  let persistence_error = persist_journal_record(&record).err();
  let mut message = format!(
    "{operation_error:#}; automatic rollback failed: {rollback_error:#}. A recovery record was retained for manual undo"
  );
  if let Some(error) = reactivation_error {
    message.push_str(&format!(
      "; the recovery journal could not be reactivated: {error:#}"
    ));
  }
  if let Some(error) = persistence_error {
    message.push_str(&format!(
      "; the recovery manifest could not be refreshed: {error:#}"
    ));
  }
  RetainedJournalError { message, record }.into()
}

fn retained_journal(error: &anyhow::Error) -> Option<JournalRecord> {
  error
    .downcast_ref::<RetainedJournalError>()
    .map(|error| error.record.clone())
}

fn capture_redo_journal(
  root: &Dir,
  record: &mut JournalRecord,
  progress: Option<&OperationProgress>,
) -> anyhow::Result<()> {
  let after_directory = journal_root().join(&record.id).join("after");
  if after_directory.exists() {
    fs::remove_dir_all(&after_directory)?;
  }
  create_private_directory(&after_directory)?;
  let result = (|| {
    for snapshot in &mut record.snapshots {
      let has_after = record
        .after_revisions
        .iter()
        .find(|(path, _)| path == &snapshot.path)
        .is_some_and(|(_, revision)| revision.is_some());
      if has_after {
        let relative = relative_path(&snapshot.path, false)?;
        snapshot.after_metadata =
          capture_journal_metadata(root, &relative)?;
        backup_from_capability(
          root,
          &relative,
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

#[derive(Debug)]
struct RedoInvalidationBatch {
  path: PathBuf,
}

fn recover_redo_invalidation_batches(
  journal_root: &Path,
) -> anyhow::Result<()> {
  for entry in fs::read_dir(journal_root)? {
    let entry = entry?;
    if !entry.file_type()?.is_dir()
      || !entry
        .file_name()
        .to_string_lossy()
        .starts_with(".redo-invalidation-")
    {
      continue;
    }
    let batch = RedoInvalidationBatch { path: entry.path() };
    if batch.path.join("committed").exists() {
      cleanup_redo_invalidation_batch(batch);
    } else {
      restore_redo_invalidation_batch(journal_root, &batch)?;
    }
  }
  Ok(())
}

fn restore_redo_invalidation_batch(
  journal_root: &Path,
  batch: &RedoInvalidationBatch,
) -> anyhow::Result<()> {
  if !batch.path.exists() {
    return Ok(());
  }
  let mut entries = fs::read_dir(&batch.path)?
    .collect::<std::io::Result<Vec<_>>>()?;
  entries.sort_by_key(|entry| entry.file_name());
  for entry in entries {
    if !entry.file_type()?.is_dir() {
      continue;
    }
    let destination = journal_root.join(entry.file_name());
    if destination.exists() {
      return Err(anyhow!(
        "Cannot restore staged redo journal because its original path exists"
      ));
    }
    fs::rename(entry.path(), destination)?;
  }
  fs::remove_dir_all(&batch.path)?;
  #[cfg(unix)]
  fs::File::open(journal_root)?.sync_all()?;
  Ok(())
}

fn stage_redo_invalidation_at(
  journal_root: &Path,
  new_operation_id: &str,
  redo_ids: &[String],
  fail_before_index: Option<usize>,
) -> anyhow::Result<RedoInvalidationBatch> {
  let batch = RedoInvalidationBatch {
    path: journal_root
      .join(format!(".redo-invalidation-{}", Uuid::new_v4())),
  };
  create_private_directory(&batch.path)?;
  let result = (|| {
    write_private_file(
      &batch.path.join("new-operation-id"),
      new_operation_id.as_bytes(),
    )?;
    for (index, id) in redo_ids.iter().enumerate() {
      if fail_before_index == Some(index) {
        return Err(anyhow!(
          "Injected redo invalidation failure before index {index}"
        ));
      }
      fs::rename(journal_root.join(id), batch.path.join(id))?;
    }
    #[cfg(unix)]
    {
      fs::File::open(&batch.path)?.sync_all()?;
      fs::File::open(journal_root)?.sync_all()?;
    }
    Ok(())
  })();
  match result {
    Ok(()) => Ok(batch),
    Err(error) => match restore_redo_invalidation_batch(
      journal_root,
      &batch,
    ) {
      Ok(()) => Err(error),
      Err(restore_error) => Err(error.context(format!(
        "Staged redo journals could not be fully restored: {restore_error:#}"
      ))),
    },
  }
}

fn commit_redo_invalidation_batch(
  batch: &RedoInvalidationBatch,
) -> anyhow::Result<()> {
  write_private_file(&batch.path.join("committed"), b"committed\n")?;
  #[cfg(unix)]
  {
    let journal_root = batch
      .path
      .parent()
      .context("Redo invalidation batch path has no parent")?;
    fs::File::open(&batch.path)?.sync_all()?;
    fs::File::open(journal_root)?.sync_all()?;
  }
  Ok(())
}

fn cleanup_redo_invalidation_batch(batch: RedoInvalidationBatch) {
  let entries = match fs::read_dir(&batch.path) {
    Ok(entries) => entries,
    Err(error) => {
      warn!(
        "Committed redo invalidation could not be inspected at {}: {error:#}",
        batch.path.display()
      );
      return;
    }
  };
  for entry in entries.flatten() {
    if !entry.file_type().is_ok_and(|kind| kind.is_dir()) {
      continue;
    }
    if let Some(record) = read_active_journal_record(&entry.path())
      && !record.actions.is_empty()
      && let Err(error) = remove_action_state(&record)
    {
      warn!(
        "Committed redo invalidation is retaining target recovery data for retry at {}: {error:#}",
        batch.path.display()
      );
      return;
    }
  }
  if let Err(error) = fs::remove_dir_all(&batch.path) {
    warn!(
      "Committed redo invalidation awaits cleanup at {}: {error:#}",
      batch.path.display()
    );
  }
}

async fn push_journal(record: JournalRecord) -> anyhow::Result<()> {
  let key = history_key(&record.root_key, &record.actor);
  let redo_ids = {
    let mut histories = histories().lock().await;
    let history = histories.entry(key.clone()).or_default();
    history
      .redo
      .iter()
      .map(|record| record.id.clone())
      .collect::<Vec<_>>()
  };
  let batch = if redo_ids.is_empty() {
    None
  } else {
    let new_operation_id = record.id.clone();
    let staged_redo_ids = redo_ids.clone();
    Some(
      run_heavy_blocking(move || {
        stage_redo_invalidation_at(
          &journal_root(),
          &new_operation_id,
          &staged_redo_ids,
          None,
        )
      })
      .await
      .context(
        "Redo journals could not be staged for invalidation",
      )?,
    )
  };
  let expired = {
    let mut histories = histories().lock().await;
    let history = histories.entry(key).or_default();
    let current_redo_ids = history
      .redo
      .iter()
      .map(|record| record.id.as_str())
      .collect::<Vec<_>>();
    let expected_redo_ids =
      redo_ids.iter().map(String::as_str).collect::<Vec<_>>();
    if current_redo_ids != expected_redo_ids {
      drop(histories);
      if let Some(batch) = &batch {
        run_heavy_blocking({
          let path = batch.path.clone();
          move || {
            restore_redo_invalidation_batch(
              &journal_root(),
              &RedoInvalidationBatch { path },
            )
          }
        })
        .await?;
      }
      return Err(anyhow!(
        "Redo history changed while registering the new operation"
      ));
    }
    if let Some(batch) = &batch
      && let Err(error) = commit_redo_invalidation_batch(batch)
    {
      drop(histories);
      run_heavy_blocking({
        let path = batch.path.clone();
        move || {
          restore_redo_invalidation_batch(
            &journal_root(),
            &RedoInvalidationBatch { path },
          )
        }
      })
      .await?;
      return Err(error.context(
        "Redo invalidation could not be committed durably",
      ));
    }
    history.redo.clear();
    history.undo.push(record);
    prune_history(history)
  };
  if let Some(batch) = batch {
    tokio::spawn(async move {
      let _ = run_heavy_blocking(move || {
        cleanup_redo_invalidation_batch(batch);
        Ok(())
      })
      .await;
    });
  }
  schedule_journal_cleanup(expired);
  Ok(())
}

async fn store_journal_by_side(key: &str, record: JournalRecord) {
  let record = normalize_loaded_journal(record);
  let mut histories = histories().lock().await;
  let history = histories.entry(key.to_string()).or_default();
  if record.recovery
    || record.history_side == JournalHistorySide::Undo
  {
    history.undo.push(record);
  } else {
    history.redo.push(record);
  }
}

async fn remove_journal_from_history(record: &JournalRecord) {
  let key = history_key(&record.root_key, &record.actor);
  let mut histories = histories().lock().await;
  if let Some(history) = histories.get_mut(&key) {
    history.undo.retain(|entry| entry.id != record.id);
    history.redo.retain(|entry| entry.id != record.id);
  }
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
      restore_journal_metadata(
        root,
        &relative,
        &snapshot.before_metadata,
      )?;
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
      restore_journal_metadata(
        root,
        &relative,
        &snapshot.after_metadata,
      )?;
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

#[cfg(unix)]
fn capture_journal_metadata(
  root: &Dir,
  relative: &Path,
) -> anyhow::Result<Vec<JournalEntryMetadata>> {
  fn visit(
    parent: &Dir,
    name: &std::ffi::OsStr,
    relative: &Path,
    entries: &mut Vec<JournalEntryMetadata>,
  ) -> anyhow::Result<()> {
    let metadata = parent.symlink_metadata(name)?;
    entries.push(JournalEntryMetadata {
      path: if relative.as_os_str().is_empty() {
        String::new()
      } else {
        path_string(relative)?
      },
      mode: cap_std::fs::MetadataExt::mode(&metadata),
      uid: cap_std::fs::MetadataExt::uid(&metadata),
      gid: cap_std::fs::MetadataExt::gid(&metadata),
    });
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
      let directory = parent.open_dir_nofollow(name)?;
      let mut children = directory
        .entries()?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<std::io::Result<Vec<_>>>()?;
      children.sort();
      for child in children {
        visit(&directory, &child, &relative.join(&child), entries)?;
      }
    }
    Ok(())
  }

  let (parent, name) = open_parent_nofollow(root, relative)?;
  let mut entries = Vec::new();
  visit(&parent, &name, Path::new(""), &mut entries)?;
  Ok(entries)
}

#[cfg(not(unix))]
fn capture_journal_metadata(
  _root: &Dir,
  _relative: &Path,
) -> anyhow::Result<Vec<JournalEntryMetadata>> {
  Ok(Vec::new())
}

#[cfg(unix)]
fn restore_journal_metadata(
  root: &Dir,
  relative: &Path,
  entries: &[JournalEntryMetadata],
) -> anyhow::Result<()> {
  use std::{
    ffi::CString,
    os::{fd::AsRawFd as _, unix::ffi::OsStrExt as _},
  };

  for entry in entries.iter().rev() {
    let path = if entry.path.is_empty() {
      relative.to_path_buf()
    } else {
      relative.join(relative_path(&entry.path, false)?)
    };
    let (parent, name) = open_parent_nofollow(root, &path)?;
    let metadata = parent.symlink_metadata(&name)?;
    if metadata.file_type().is_symlink() {
      let current_uid = cap_std::fs::MetadataExt::uid(&metadata);
      let current_gid = cap_std::fs::MetadataExt::gid(&metadata);
      if current_uid != entry.uid || current_gid != entry.gid {
        let name = CString::new(name.as_bytes())?;
        let result = unsafe {
          libc::fchownat(
            parent.as_raw_fd(),
            name.as_ptr(),
            entry.uid as libc::uid_t,
            entry.gid as libc::gid_t,
            libc::AT_SYMLINK_NOFOLLOW,
          )
        };
        if result != 0 {
          return Err(std::io::Error::last_os_error())
            .context("Failed to restore symlink ownership");
        }
      }
      continue;
    }
    let file = if metadata.is_dir() {
      parent.open_dir_nofollow(&name)?.open(".")?
    } else if metadata.is_file() {
      let mut options = OpenOptions::new();
      options.read(true).follow(FollowSymlinks::No);
      parent.open_with(&name, &options)?
    } else {
      return Err(anyhow!(
        "Special journal entries cannot have metadata restored"
      ));
    };
    let expected = JournalEntryMetadata {
      path: entry.path.clone(),
      mode: entry.mode,
      uid: entry.uid,
      gid: entry.gid,
    };
    apply_recorded_file_metadata(&file, &expected, true)?;
    file.sync_all()?;
  }
  Ok(())
}

#[cfg(not(unix))]
fn restore_journal_metadata(
  _root: &Dir,
  _relative: &Path,
  _entries: &[JournalEntryMetadata],
) -> anyhow::Result<()> {
  Ok(())
}

#[cfg(unix)]
fn apply_recorded_file_metadata(
  file: &cap_std::fs::File,
  metadata: &JournalEntryMetadata,
  preserve_privilege_bits: bool,
) -> anyhow::Result<()> {
  use cap_std::fs::PermissionsExt as _;
  use std::os::fd::AsRawFd as _;

  let current = file.metadata()?;
  if cap_std::fs::MetadataExt::uid(&current) != metadata.uid
    || cap_std::fs::MetadataExt::gid(&current) != metadata.gid
  {
    let result = unsafe {
      libc::fchown(
        file.as_raw_fd(),
        metadata.uid as libc::uid_t,
        metadata.gid as libc::gid_t,
      )
    };
    if result != 0 {
      return Err(std::io::Error::last_os_error())
        .context("Failed to restore filesystem ownership");
    }
  }
  file.set_permissions(cap_std::fs::Permissions::from_mode(
    metadata.mode
      & if preserve_privilege_bits {
        0o7777
      } else {
        0o1777
      },
  ))?;
  Ok(())
}

#[cfg(not(unix))]
fn apply_recorded_file_metadata(
  _file: &cap_std::fs::File,
  _metadata: &JournalEntryMetadata,
  _preserve_privilege_bits: bool,
) -> anyhow::Result<()> {
  Ok(())
}

#[cfg(unix)]
fn recorded_file_metadata(
  metadata: &Metadata,
) -> Option<JournalEntryMetadata> {
  Some(JournalEntryMetadata {
    path: String::new(),
    mode: cap_std::fs::MetadataExt::mode(metadata),
    uid: cap_std::fs::MetadataExt::uid(metadata),
    gid: cap_std::fs::MetadataExt::gid(metadata),
  })
}

#[cfg(not(unix))]
fn recorded_file_metadata(
  _metadata: &Metadata,
) -> Option<JournalEntryMetadata> {
  None
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
    let mut destination = create_private_file(destination)?;
    copy_with_progress(&mut source, &mut destination, progress)?;
    destination.sync_all()?;
  } else if metadata.is_dir() {
    create_private_directory(destination)?;
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

fn journal_is_visible(record: &JournalRecord) -> bool {
  !record.cleanup_only && (!record.managed || record.recovery)
}

fn journal_is_unexpired(record: &JournalRecord) -> bool {
  record.durable_managed
    || record.recovery
    || record.transition.is_some()
    || record.expires_at > komodo_timestamp()
}

fn normalize_loaded_journal(
  mut record: JournalRecord,
) -> JournalRecord {
  if record.transition.is_some() {
    record.recovery = true;
    record.history_side = JournalHistorySide::Undo;
    if !record.description.starts_with("Recover interrupted ") {
      record.description =
        format!("Recover interrupted {}", record.description);
    }
  }
  record
}

fn read_active_journal_record(
  directory: &Path,
) -> Option<JournalRecord> {
  if directory.join("retired").exists() {
    return None;
  }
  fs::read(directory.join("manifest.json"))
    .ok()
    .and_then(|manifest| {
      serde_json::from_slice::<JournalRecord>(&manifest).ok()
    })
    .map(normalize_loaded_journal)
}

fn prune_history(history: &mut JournalHistory) -> Vec<String> {
  let now = komodo_timestamp();
  let mut expired = Vec::new();
  history.undo.retain(|record| {
    let keep = record.durable_managed
      || record.recovery
      || record.transition.is_some()
      || record.expires_at > now;
    if !keep {
      expired.push(record.id.clone());
    }
    keep
  });
  history.redo.retain(|record| {
    let keep = record.durable_managed
      || record.recovery
      || record.transition.is_some()
      || record.expires_at > now;
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
        if let Some(record) =
          read_active_journal_record(&journal_root().join(&id))
          && !record.actions.is_empty()
          && let Err(error) = remove_action_state(&record)
        {
          warn!(
            "Expired File Manager journal {id} is retaining target recovery data for retry: {error:#}"
          );
          continue;
        }
        if let Err(error) =
          fs::remove_dir_all(journal_root().join(&id))
          && error.kind() != std::io::ErrorKind::NotFound
        {
          warn!(
            "Expired File Manager journal {id} awaits cleanup: {error:#}"
          );
        }
      }
      Ok(())
    })
    .await;
    if let Err(error) = result {
      warn!("Failed to clean up File Manager journals: {error:#}");
    }
  });
}

async fn prune_finalized_managed_transactions() {
  let now = komodo_timestamp();
  let candidates = {
    let transactions = managed_transactions().lock().await;
    transactions
      .values()
      .filter(|record| managed_transaction_is_prunable(record, now))
      .map(|record| record.operation_id.clone())
      .collect::<Vec<_>>()
  };
  if candidates.is_empty() {
    return;
  }
  let removed = match run_heavy_blocking(move || {
    let mut removed = Vec::new();
    for operation_id in candidates {
      let path = managed_transaction_path(&operation_id)?;
      match fs::remove_file(path) {
        Ok(()) => removed.push(operation_id),
        Err(error)
          if error.kind() == std::io::ErrorKind::NotFound =>
        {
          removed.push(operation_id)
        }
        Err(error) => warn!(
          "Failed to prune finalized managed transaction {operation_id}: {error:#}"
        ),
      }
    }
    #[cfg(unix)]
    fs::File::open(managed_transaction_root())?.sync_all()?;
    Ok(removed)
  })
  .await
  {
    Ok(removed) => removed,
    Err(error) => {
      warn!(
        "Failed to prune finalized managed transactions: {error:#}"
      );
      return;
    }
  };
  let mut transactions = managed_transactions().lock().await;
  for operation_id in removed {
    transactions.remove(&operation_id);
  }
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
  #[cfg(unix)]
  {
    hash
      .update(cap_std::fs::MetadataExt::mode(metadata).to_le_bytes());
    hash
      .update(cap_std::fs::MetadataExt::uid(metadata).to_le_bytes());
    hash
      .update(cap_std::fs::MetadataExt::gid(metadata).to_le_bytes());
  }
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

  fn test_journal_record(
    id: &str,
    created_at: i64,
    side: JournalHistorySide,
  ) -> JournalRecord {
    JournalRecord {
      id: id.into(),
      actor: "actor".into(),
      root_key: "root".into(),
      root_path: PathBuf::from("/unused"),
      managed: false,
      durable_managed: false,
      recovery: false,
      history_side: side,
      transition: None,
      created_at,
      expires_at: created_at + JOURNAL_TTL_MS,
      description: id.into(),
      execution_mode: FileManagerExecutionMode::Recoverable,
      actions: Vec::new(),
      cleanup_only: false,
      snapshots: Vec::new(),
      before_revisions: Vec::new(),
      after_revisions: Vec::new(),
    }
  }

  #[test]
  fn file_manager_entry_limit_accepts_exact_boundary() {
    assert!(ensure_entry_limit_with_max(5, 5).is_ok());
    assert!(ensure_entry_limit_with_max(6, 5).is_err());
  }

  #[test]
  fn wrong_actor_cannot_consume_a_preflight_plan() {
    let mut plans = HashMap::new();
    plans.insert(
      "plan".into(),
      OperationPlan {
        actor: "owner".into(),
        root_key: "root".into(),
        operation: FileManagerOperation::CreateFile {
          path: "file.txt".into(),
        },
        expires_at: komodo_timestamp() + PLAN_TTL_MS,
        conflicts: Vec::new(),
        confirmation_required: false,
        revisions: Vec::new(),
        copy_targets: Vec::new(),
        execution_mode: FileManagerExecutionMode::Recoverable,
        recursive_revisions: false,
      },
    );

    assert!(take_owned_plan(&mut plans, "plan", "attacker").is_err());
    assert!(plans.contains_key("plan"));
    assert!(take_owned_plan(&mut plans, "plan", "owner").is_ok());
    assert!(!plans.contains_key("plan"));
  }

  #[test]
  fn bounded_text_read_rejects_growth_past_the_limit() {
    let mut exact = std::io::Cursor::new(b"four".to_vec());
    assert_eq!(read_bounded(&mut exact, 4).unwrap(), b"four");

    let mut grown = std::io::Cursor::new(b"five!".to_vec());
    assert!(read_bounded(&mut grown, 4).is_err());
  }

  #[test]
  fn redundant_delete_and_move_paths_are_normalized_only_once() {
    let delete = normalize_operation(FileManagerOperation::Delete {
      paths: vec![
        "folder/child.txt".into(),
        "folder".into(),
        "folder".into(),
      ],
    })
    .unwrap();
    assert!(matches!(
      delete,
      FileManagerOperation::Delete { paths }
        if paths == vec!["folder"]
    ));

    let copy = normalize_operation(FileManagerOperation::Copy {
      paths: vec!["folder".into(), "folder/child.txt".into()],
      destination: "copies".into(),
    })
    .unwrap();
    assert!(matches!(
      copy,
      FileManagerOperation::Copy { paths, .. }
        if paths == vec!["folder", "folder/child.txt"]
    ));
  }

  #[test]
  fn parent_and_child_delete_completes_without_order_dependent_loss()
  {
    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    fs::create_dir_all(directory.join("folder")).unwrap();
    fs::write(directory.join("folder/child.txt"), "contents")
      .unwrap();
    let root =
      Dir::open_ambient_dir(&directory, ambient_authority()).unwrap();
    let operation =
      normalize_operation(FileManagerOperation::Delete {
        paths: vec!["folder/child.txt".into(), "folder".into()],
      })
      .unwrap();

    apply_operation(&root, &operation, &[]).unwrap();
    assert!(!directory.join("folder").exists());
    fs::remove_dir_all(directory).unwrap();
  }

  #[test]
  fn large_directory_delete_uses_a_reversible_metadata_action() {
    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    let tree = directory.join("tree");
    fs::create_dir_all(tree.join("nested")).unwrap();
    for index in 0..128 {
      fs::write(
        tree.join("nested").join(format!("{index}.txt")),
        "contents",
      )
      .unwrap();
    }
    let root =
      Dir::open_ambient_dir(&directory, ambient_authority()).unwrap();
    let before =
      file_identity(&root.symlink_metadata("tree").unwrap());
    let operation = FileManagerOperation::Delete {
      paths: vec!["tree".into()],
    };
    assert!(supports_action_journal(&root, &operation, &[]).unwrap());

    let mut record = test_journal_record(
      &Uuid::new_v4().to_string(),
      komodo_timestamp(),
      JournalHistorySide::Undo,
    );
    record.root_path = directory.clone();
    let operation_directory =
      directory.join(PRIVATE_STATE_DIRECTORY).join(&record.id);
    fs::create_dir(directory.join(PRIVATE_STATE_DIRECTORY)).unwrap();
    fs::write(
      directory
        .join(PRIVATE_STATE_DIRECTORY)
        .join(PRIVATE_STATE_MARKER),
      PRIVATE_STATE_MARKER_CONTENTS,
    )
    .unwrap();
    fs::create_dir_all(operation_directory.join("quarantine"))
      .unwrap();
    fs::create_dir(operation_directory.join("staging")).unwrap();
    fs::rename(&tree, operation_directory.join("quarantine/0"))
      .unwrap();
    record.actions.push(JournalAction {
      state: JournalActionState::Applied,
      kind: JournalActionKind::Quarantine {
        path: "tree".into(),
        quarantine_name: "0".into(),
      },
    });

    rollback_action_journal(&root, &record).unwrap();
    assert_eq!(
      file_identity(&root.symlink_metadata("tree").unwrap()),
      before
    );
    assert_eq!(
      fs::read_to_string(tree.join("nested/127.txt")).unwrap(),
      "contents"
    );
    fs::remove_dir_all(directory).unwrap();
  }

  #[test]
  fn target_local_recovery_state_requires_ownership_marker() {
    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    let reserved = directory.join(PRIVATE_STATE_DIRECTORY);
    fs::create_dir_all(&reserved).unwrap();
    fs::write(reserved.join("application-data"), "keep").unwrap();
    let root =
      Dir::open_ambient_dir(&directory, ambient_authority()).unwrap();

    let error = action_state_directory(&root).unwrap_err();
    assert!(error.to_string().contains("not managed by Periphery"));
    assert_eq!(
      fs::read_to_string(reserved.join("application-data")).unwrap(),
      "keep"
    );
    fs::remove_dir_all(directory).unwrap();
  }

  #[test]
  fn extract_destination_is_snapshotted_for_failure_rollback() {
    let operation = FileManagerOperation::ExtractArchive {
      path: "archive.zip".into(),
      destination: "destination".into(),
    };
    assert_eq!(
      journal_paths_planned(&operation, &[]).unwrap(),
      vec!["destination"]
    );
  }

  #[test]
  fn managed_journal_classification_is_limited_to_its_text_edit() {
    let root = ResolvedRoot {
      path: PathBuf::from("/unused"),
      key: "root".into(),
      read_only: false,
      managed_file: Some("compose.yaml".into()),
      create_if_missing: false,
    };
    assert!(operation_edits_managed_file(
      &root,
      &FileManagerOperation::WriteText {
        path: "compose.yaml".into(),
        contents: String::new(),
        expected_revision: Default::default(),
      }
    ));
    assert!(!operation_edits_managed_file(
      &root,
      &FileManagerOperation::Delete {
        paths: vec!["notes.txt".into()],
      }
    ));
  }

  #[test]
  fn loaded_redo_records_keep_stack_order() {
    let mut records = vec![
      test_journal_record("C", 3, JournalHistorySide::Redo),
      test_journal_record("B", 2, JournalHistorySide::Redo),
    ];
    records.sort_by_key(|record| record.created_at);
    let mut history = JournalHistory::default();
    for record in records {
      history.insert_loaded(record);
    }
    assert_eq!(history.redo.pop().unwrap().id, "B");
    assert_eq!(history.redo.pop().unwrap().id, "C");
  }

  #[test]
  fn interrupted_history_transition_reloads_as_recovery_undo() {
    let mut record =
      test_journal_record("transition", 1, JournalHistorySide::Redo);
    record.managed = true;
    record.transition = Some(JournalTransition::Redo);
    let record = normalize_loaded_journal(record);
    assert!(record.recovery);
    assert_eq!(record.history_side, JournalHistorySide::Undo);
    assert!(journal_is_visible(&record));
    assert!(record.description.starts_with("Recover interrupted "));
  }

  #[test]
  fn hidden_managed_journal_remains_loadable_for_exact_rollback() {
    let mut record = test_journal_record(
      "managed-host-rollback",
      komodo_timestamp(),
      JournalHistorySide::Undo,
    );
    record.managed = true;
    record.expires_at = komodo_timestamp() + JOURNAL_TTL_MS;

    assert!(!journal_is_visible(&record));
    assert!(journal_is_unexpired(&record));
    let mut history = JournalHistory::default();
    history.insert_loaded(record);
    assert!(history.undo.iter().any(|record| {
      record.id == "managed-host-rollback"
        && !journal_is_visible(record)
    }));
  }

  #[test]
  fn hidden_managed_journal_does_not_displace_visible_undo() {
    let ordinary = test_journal_record(
      "ordinary-A",
      komodo_timestamp(),
      JournalHistorySide::Undo,
    );
    let mut managed = test_journal_record(
      "managed-B",
      komodo_timestamp() + 1,
      JournalHistorySide::Undo,
    );
    managed.managed = true;
    let history = JournalHistory {
      undo: vec![ordinary, managed],
      redo: Vec::new(),
    };

    let visible = history
      .undo
      .iter()
      .rposition(journal_is_visible)
      .map(|position| &history.undo[position]);
    assert_eq!(visible.unwrap().id, "ordinary-A");
  }

  #[test]
  fn redo_invalidation_failure_restores_the_complete_stack() {
    for fail_before in [0, 1] {
      let directory =
        std::env::temp_dir().join(Uuid::new_v4().to_string());
      fs::create_dir_all(&directory).unwrap();
      let ids = vec!["redo-a".to_string(), "redo-b".to_string()];
      for id in &ids {
        fs::create_dir(directory.join(id)).unwrap();
        fs::write(directory.join(id).join("payload"), id).unwrap();
      }

      let error = stage_redo_invalidation_at(
        &directory,
        "new-operation",
        &ids,
        Some(fail_before),
      )
      .unwrap_err();
      assert!(error.to_string().contains("Injected"));
      for id in &ids {
        assert_eq!(
          fs::read_to_string(directory.join(id).join("payload"))
            .unwrap(),
          id.as_str()
        );
      }
      assert!(fs::read_dir(&directory).unwrap().all(|entry| {
        !entry
          .unwrap()
          .file_name()
          .to_string_lossy()
          .starts_with(".redo-invalidation-")
      }));
      fs::remove_dir_all(directory).unwrap();
    }
  }

  #[test]
  fn committed_redo_invalidation_is_completed_on_restart() {
    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    fs::create_dir_all(&directory).unwrap();
    let ids = vec!["redo-a".to_string(), "redo-b".to_string()];
    for id in &ids {
      fs::create_dir(directory.join(id)).unwrap();
    }
    let batch = stage_redo_invalidation_at(
      &directory,
      "new-operation",
      &ids,
      None,
    )
    .unwrap();
    commit_redo_invalidation_batch(&batch).unwrap();

    recover_redo_invalidation_batches(&directory).unwrap();

    for id in ids {
      assert!(!directory.join(id).exists());
    }
    assert!(!batch.path.exists());
    fs::remove_dir_all(directory).unwrap();
  }

  #[test]
  fn recovery_and_transition_journals_do_not_expire() {
    let mut expired = test_journal_record(
      "expired",
      komodo_timestamp() - JOURNAL_TTL_MS - 1,
      JournalHistorySide::Undo,
    );
    expired.expires_at = komodo_timestamp() - 1;
    let mut recovery = expired.clone();
    recovery.id = "recovery".into();
    recovery.recovery = true;
    let mut transition = expired.clone();
    transition.id = "transition".into();
    transition.transition = Some(JournalTransition::Undo);
    let mut durable = expired.clone();
    durable.id = "durable-managed".into();
    durable.managed = true;
    durable.durable_managed = true;

    assert!(!journal_is_unexpired(&expired));
    assert!(journal_is_unexpired(&recovery));
    assert!(journal_is_unexpired(&transition));
    assert!(journal_is_unexpired(&durable));
    let mut history = JournalHistory {
      undo: vec![expired, recovery, transition, durable],
      redo: Vec::new(),
    };
    let removed = prune_history(&mut history);
    assert_eq!(removed, vec!["expired"]);
    assert_eq!(history.undo.len(), 3);
  }

  #[test]
  fn retired_journal_manifest_is_never_reloaded() {
    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    fs::create_dir(&directory).unwrap();
    let record = test_journal_record(
      "retired",
      komodo_timestamp(),
      JournalHistorySide::Undo,
    );
    fs::write(
      directory.join("manifest.json"),
      serde_json::to_vec(&record).unwrap(),
    )
    .unwrap();
    assert!(read_active_journal_record(&directory).is_some());
    fs::write(directory.join("retired"), b"retired").unwrap();
    assert!(read_active_journal_record(&directory).is_none());
    fs::remove_dir_all(directory).unwrap();
  }

  #[test]
  fn partial_snapshot_directory_is_removed_by_its_guard() {
    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    fs::create_dir(&directory).unwrap();
    {
      let _cleanup = TemporaryJournalDirectory {
        path: directory.clone(),
        retained: false,
      };
      fs::write(directory.join("partial"), b"secret").unwrap();
    }
    assert!(!directory.exists());
  }

  #[cfg(unix)]
  #[test]
  fn journal_directories_and_files_are_private() {
    use std::os::unix::fs::PermissionsExt as _;

    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    ensure_private_directory(&directory).unwrap();
    let child = directory.join("before");
    create_private_directory(&child).unwrap();
    let file = child.join("0");
    create_private_file(&file).unwrap().sync_all().unwrap();

    assert_eq!(
      fs::metadata(&directory).unwrap().permissions().mode() & 0o777,
      0o700
    );
    assert_eq!(
      fs::metadata(&child).unwrap().permissions().mode() & 0o777,
      0o700
    );
    assert_eq!(
      fs::metadata(&file).unwrap().permissions().mode() & 0o777,
      0o600
    );
    fs::remove_dir_all(directory).unwrap();
  }

  #[cfg(unix)]
  #[test]
  fn private_upload_staging_preserves_default_published_mode() {
    use std::os::unix::fs::PermissionsExt as _;

    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    fs::create_dir(&directory).unwrap();
    fs::write(directory.join("reference"), b"").unwrap();
    let root =
      Dir::open_ambient_dir(&directory, ambient_authority()).unwrap();
    create_private_capability_directory(&root, ".komodo-upload-test")
      .unwrap();
    let staging =
      root.open_dir_nofollow(".komodo-upload-test").unwrap();
    let mut options = OpenOptions::new();
    options
      .write(true)
      .create_new(true)
      .follow(FollowSymlinks::No);
    staging.open_with("payload", &options).unwrap();

    assert_eq!(
      fs::metadata(directory.join(".komodo-upload-test"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777,
      0o700
    );
    assert_eq!(
      fs::metadata(directory.join(".komodo-upload-test/payload"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777,
      fs::metadata(directory.join("reference"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777
    );

    fs::write(directory.join("existing"), b"old").unwrap();
    fs::set_permissions(
      directory.join("existing"),
      fs::Permissions::from_mode(0o6750),
    )
    .unwrap();
    let existing = root.symlink_metadata("existing").unwrap();
    let recorded = recorded_file_metadata(&existing).unwrap();
    let replacement =
      staging.open_with("replacement", &options).unwrap();
    apply_recorded_file_metadata(&replacement, &recorded, false)
      .unwrap();
    assert_eq!(
      fs::metadata(directory.join(".komodo-upload-test/replacement"))
        .unwrap()
        .permissions()
        .mode()
        & 0o7777,
      0o750
    );
    fs::remove_dir_all(directory).unwrap();
  }

  #[cfg(unix)]
  #[test]
  fn upload_publish_replaces_regular_file_with_one_rename() {
    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    fs::create_dir(&directory).unwrap();
    fs::write(directory.join("destination"), b"old").unwrap();
    fs::create_dir(directory.join("staging")).unwrap();
    fs::write(directory.join("staging/payload"), b"new").unwrap();
    let root =
      Dir::open_ambient_dir(&directory, ambient_authority()).unwrap();
    let staging = root.open_dir_nofollow("staging").unwrap();
    let payload_identity =
      file_identity(&staging.symlink_metadata("payload").unwrap());

    staging.rename("payload", &root, "destination").unwrap();

    assert_eq!(
      fs::read(directory.join("destination")).unwrap(),
      b"new"
    );
    assert_eq!(
      file_identity(&root.symlink_metadata("destination").unwrap()),
      payload_identity
    );
    fs::remove_dir_all(directory).unwrap();
  }

  #[test]
  fn download_credit_is_positive_and_bounded() {
    assert!(add_download_credits(0, 0).is_err());
    assert_eq!(add_download_credits(0, 4).unwrap(), 4);
    assert_eq!(
      add_download_credits(31, u32::MAX).unwrap(),
      MAX_DOWNLOAD_CREDITS
    );
  }

  #[test]
  fn download_heartbeat_requires_the_additive_begin_variant() {
    assert_eq!(
      download_begin_mode(FileTransferMessage::BeginWithCredit {
        credits: 4
      })
      .unwrap(),
      (Some(4), false)
    );
    assert_eq!(
      download_begin_mode(
        FileTransferMessage::BeginWithCreditAndHeartbeat {
          credits: 4
        }
      )
      .unwrap(),
      (Some(4), true)
    );
  }

  #[tokio::test]
  async fn cancellation_interrupts_a_download_waiting_for_credit() {
    let progress = OperationProgress::new(
      "credit-cancel".into(),
      "Download files".into(),
    );
    let (_sender, mut receiver) = tokio::sync::mpsc::channel(1);
    progress.request_cancel();
    let error = tokio::time::timeout(
      std::time::Duration::from_secs(1),
      wait_for_download_credit(&mut receiver, &progress, false),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(error.to_string().contains("cancelled"));
  }

  #[tokio::test]
  async fn heartbeat_download_expires_when_core_disappears() {
    let progress = OperationProgress::new(
      "heartbeat-expiry".into(),
      "Download files".into(),
    );
    let (_sender, mut receiver) = tokio::sync::mpsc::channel(1);

    let error = wait_for_download_credit_with_lease(
      &mut receiver,
      &progress,
      true,
      Duration::from_millis(20),
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("heartbeat expired"));
  }

  #[tokio::test]
  async fn heartbeat_renews_a_paused_download_lease() {
    let progress = OperationProgress::new(
      "heartbeat-renewal".into(),
      "Download files".into(),
    );
    let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
    tokio::spawn(async move {
      tokio::time::sleep(Duration::from_millis(100)).await;
      sender
        .send(Ok(FileTransferMessage::Heartbeat))
        .await
        .unwrap();
      tokio::time::sleep(Duration::from_millis(100)).await;
      sender
        .send(Ok(FileTransferMessage::Credit { credits: 1 }))
        .await
        .unwrap();
    });

    let credits = wait_for_download_credit_with_lease(
      &mut receiver,
      &progress,
      true,
      Duration::from_millis(150),
    )
    .await
    .unwrap();

    assert_eq!(credits, 1);
  }

  #[tokio::test]
  async fn file_transfer_final_send_is_bounded_when_outgoing_stalls()
  {
    let channel = Uuid::new_v4();
    let (sender, _receiver) =
      transport::channel::channel_with_capacity::<
        EncodedTransportMessage,
      >(1);
    sender
      .send_file_transfer(
        Uuid::new_v4(),
        Ok(FileTransferMessage::Cancel.into_raw()),
      )
      .await
      .unwrap();

    tokio::time::timeout(
      Duration::from_millis(100),
      send_file_transfer_final_with_timeout(
        &sender,
        channel,
        Ok(FileTransferMessage::Cancel.into_raw()),
        Duration::from_millis(10),
      ),
    )
    .await
    .unwrap();
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

  #[cfg(unix)]
  #[test]
  fn text_write_and_copy_preserve_mode_and_ownership() {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    fs::create_dir(&directory).unwrap();
    let edited = directory.join("editable.sh");
    fs::write(&edited, "old").unwrap();
    fs::set_permissions(&edited, fs::Permissions::from_mode(0o6750))
      .unwrap();
    let root =
      Dir::open_ambient_dir(&directory, ambient_authority()).unwrap();
    let before = fs::metadata(&edited).unwrap();
    let expected = content_revision(
      &root.symlink_metadata("editable.sh").unwrap(),
      b"old",
    );
    apply_operation(
      &root,
      &FileManagerOperation::WriteText {
        path: "editable.sh".into(),
        contents: "new".into(),
        expected_revision: expected,
      },
      &[],
    )
    .unwrap();
    let edited_after = fs::metadata(&edited).unwrap();
    assert_eq!(edited_after.mode() & 0o7777, 0o750);
    assert_eq!(edited_after.uid(), before.uid());
    assert_eq!(edited_after.gid(), before.gid());

    fs::create_dir(directory.join("source")).unwrap();
    fs::set_permissions(
      directory.join("source"),
      fs::Permissions::from_mode(0o711),
    )
    .unwrap();
    fs::write(directory.join("source/secret"), "contents").unwrap();
    fs::set_permissions(
      directory.join("source/secret"),
      fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    fs::write(directory.join("source/privileged"), "same bytes")
      .unwrap();
    fs::set_permissions(
      directory.join("source/privileged"),
      fs::Permissions::from_mode(0o6750),
    )
    .unwrap();
    copy_entry(
      &root,
      std::ffi::OsStr::new("source"),
      &root,
      std::ffi::OsStr::new("copied"),
      None,
    )
    .unwrap();
    assert_eq!(
      fs::metadata(directory.join("copied")).unwrap().mode() & 0o7777,
      0o711
    );
    assert_eq!(
      fs::metadata(directory.join("copied/secret"))
        .unwrap()
        .mode()
        & 0o7777,
      0o600
    );
    assert_eq!(
      fs::metadata(directory.join("copied/privileged"))
        .unwrap()
        .mode()
        & 0o7777,
      0o750
    );
    fs::remove_dir_all(directory).unwrap();
  }

  #[cfg(unix)]
  #[test]
  fn journal_backup_stays_private_and_restores_nested_metadata() {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    let managed = directory.join("managed");
    let backup = directory.join("backup");
    fs::create_dir_all(managed.join("nested")).unwrap();
    fs::write(managed.join("nested/tool"), "contents").unwrap();
    fs::set_permissions(&managed, fs::Permissions::from_mode(0o751))
      .unwrap();
    fs::set_permissions(
      managed.join("nested"),
      fs::Permissions::from_mode(0o711),
    )
    .unwrap();
    fs::set_permissions(
      managed.join("nested/tool"),
      fs::Permissions::from_mode(0o6750),
    )
    .unwrap();
    let original = fs::metadata(managed.join("nested/tool")).unwrap();
    let root =
      Dir::open_ambient_dir(&directory, ambient_authority()).unwrap();
    let metadata =
      capture_journal_metadata(&root, Path::new("managed")).unwrap();
    backup_from_capability(
      &root,
      Path::new("managed"),
      &backup,
      None,
    )
    .unwrap();
    assert_eq!(
      fs::metadata(&backup).unwrap().permissions().mode() & 0o777,
      0o700
    );
    assert_eq!(
      fs::metadata(backup.join("nested/tool"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777,
      0o600
    );

    root.remove_dir_all("managed").unwrap();
    restore_to_capability(&root, Path::new("managed"), &backup, None)
      .unwrap();
    restore_journal_metadata(&root, Path::new("managed"), &metadata)
      .unwrap();
    assert_eq!(
      fs::metadata(&managed).unwrap().permissions().mode() & 0o7777,
      0o751
    );
    assert_eq!(
      fs::metadata(managed.join("nested"))
        .unwrap()
        .permissions()
        .mode()
        & 0o7777,
      0o711
    );
    let restored = fs::metadata(managed.join("nested/tool")).unwrap();
    assert_eq!(restored.mode() & 0o7777, 0o6750);
    assert_eq!(restored.uid(), original.uid());
    assert_eq!(restored.gid(), original.gid());
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

  #[cfg(unix)]
  #[test]
  fn metadata_revision_detects_permission_changes() {
    use std::os::unix::fs::PermissionsExt as _;

    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    fs::create_dir(&directory).unwrap();
    let path = directory.join("config.txt");
    fs::write(&path, "contents").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
      .unwrap();
    let root =
      Dir::open_ambient_dir(&directory, ambient_authority()).unwrap();
    let before = metadata_tree_revision(&root, "config.txt").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644))
      .unwrap();
    let after = metadata_tree_revision(&root, "config.txt").unwrap();

    assert_ne!(before, after);
    fs::remove_dir_all(directory).unwrap();
  }

  #[test]
  fn upload_staging_identity_rejects_regular_file_replacement() {
    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    fs::create_dir(&directory).unwrap();
    let root =
      Dir::open_ambient_dir(&directory, ambient_authority()).unwrap();
    fs::write(directory.join("upload.tmp"), "verified bytes")
      .unwrap();
    let expected =
      file_identity(&root.symlink_metadata("upload.tmp").unwrap());

    fs::rename(
      directory.join("upload.tmp"),
      directory.join("original.tmp"),
    )
    .unwrap();
    fs::write(directory.join("upload.tmp"), "replacement").unwrap();

    assert!(
      verify_upload_staging_identity(&root, "upload.tmp", expected)
        .is_err()
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
  fn planned_revision_recheck_rejects_post_snapshot_change() {
    let directory =
      std::env::temp_dir().join(Uuid::new_v4().to_string());
    fs::create_dir(&directory).unwrap();
    let path = directory.join("config.txt");
    fs::write(&path, "accepted").unwrap();
    let root =
      Dir::open_ambient_dir(&directory, ambient_authority()).unwrap();
    let revisions = vec![(
      "config.txt".into(),
      metadata_tree_revision(&root, "config.txt").unwrap(),
    )];

    verify_plan_revisions(&root, &revisions, true, None).unwrap();
    fs::write(&path, "changed after snapshot").unwrap();
    let error = verify_plan_revisions(&root, &revisions, true, None)
      .unwrap_err();
    assert_eq!(
      error.to_string(),
      "File Manager contents changed after preflight; retry the operation"
    );
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
  fn extraction_capacity_includes_outer_rollback_and_local_staging() {
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
        journal_bytes: b"keep".len() as u64,
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
      apply_operation_planned(&root, &operation, &targets, &[], None)
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
      before_metadata: Vec::new(),
      after_metadata: Vec::new(),
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
