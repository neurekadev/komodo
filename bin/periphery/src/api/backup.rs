use std::{
  collections::{BTreeMap, BTreeSet, HashMap, HashSet},
  fs::OpenOptions,
  io::{Read, Write},
  os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
  path::{Path, PathBuf},
  sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicBool, Ordering},
  },
  time::{Duration, Instant},
};

use anyhow::{Context, anyhow};
use command::{CommandOptions, run_komodo_standard_command};
use komodo_backup::{
  VykarPatternMatcher, VykarRepository, backup_manifest_source_name,
};
use komodo_client::entities::{
  backup::BackupRestorePathSummary,
  docker::{
    container::{ContainerListItem, ContainerStateStatusEnum},
    volume::{VolumeListItem, VolumeScopeEnum, is_anonymous_volume},
  },
};
use mogh_resolver::Resolve;
use periphery_client::api::backup::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use shell_escape::unix::escape;

use crate::{config::periphery_config, state::docker_client};

use super::Args;

const COMPOSE_PROJECT_LABEL: &str = "com.docker.compose.project";
const RESTORE_PLAN_VOLUME_LABEL: &str = "komodo.restore-plan";
const PENDING_CANCELLATION_TTL: Duration = Duration::from_secs(60);
const MAX_PENDING_CANCELLATIONS: usize = 1_024;
const RESTORE_PREFLIGHT_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_RESTORE_PREVIEW_ROWS: usize = 10_000;
const MAX_RESTORE_PREVIEW_BYTES: usize = 1024 * 1024;
const MAX_RESTORE_PREVIEW_DEPTH: usize =
  komodo_backup::MAX_RESTORE_PATH_DEPTH;

fn backup_inventory_slots() -> &'static Arc<tokio::sync::Semaphore> {
  static SLOTS: OnceLock<Arc<tokio::sync::Semaphore>> =
    OnceLock::new();
  SLOTS.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(1)))
}

async fn bounded_backup_inventory<T>(
  slots: &Arc<tokio::sync::Semaphore>,
  timeout: Duration,
  work: impl std::future::Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
  let _permit = slots.clone().try_acquire_owned().context(
    "Backup Docker inventory is already running; retry after it finishes",
  )?;
  // These are read-only async Docker queries, with no spawned filesystem work.
  // Dropping the future on expiry cancels the queries before releasing the slot.
  tokio::time::timeout(timeout, work)
    .await
    .context("Backup Docker inventory exceeded its deadline")?
}

impl Resolve<Args> for GetBackupVolumeInventory {
  async fn resolve(
    self,
    _: &Args,
  ) -> anyhow::Result<Vec<VolumeListItem>> {
    bounded_backup_inventory(
      backup_inventory_slots(),
      Duration::from_secs(60),
      async {
        let docker_guard = docker_client().load();
        let client = docker_guard
          .as_ref()
          .as_ref()
          .context("Docker is not connected")?;
        let containers = client
          .list_containers()
          .await
          .context("Backup container inventory failed")?;
        client
          .list_volumes(&containers)
          .await
          .context("Backup volume inventory failed")
      },
    )
    .await
  }
}

fn preflight_slots() -> &'static Arc<tokio::sync::Semaphore> {
  static SLOTS: OnceLock<Arc<tokio::sync::Semaphore>> =
    OnceLock::new();
  SLOTS.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(1)))
}

/// Inventory collection is bounded separately. Hash every classified path
/// before limiting the display, so a full recovered Stack can be confirmed
/// without losing changes beyond the first page from execution revalidation.
fn bounded_restore_preview(
  mut created_paths: Vec<String>,
  mut overwritten_paths: Vec<String>,
  mut deleted_paths: Vec<String>,
) -> PreflightVykarRestoreResponse {
  let mut digest = Sha256::new();
  digest.update(b"komodo-restore-paths-v1");
  for (category, paths) in [
    (b'c', &mut created_paths),
    (b'o', &mut overwritten_paths),
    (b'd', &mut deleted_paths),
  ] {
    paths.sort();
    paths.dedup();
    digest.update([category]);
    digest.update((paths.len() as u64).to_le_bytes());
    for path in paths.iter() {
      digest.update((path.len() as u64).to_le_bytes());
      digest.update(path.as_bytes());
    }
  }
  let summary = BackupRestorePathSummary {
    // The complete inventory is capped at 100,000 entries before this call.
    created: created_paths.len() as u32,
    overwritten: overwritten_paths.len() as u32,
    deleted: deleted_paths.len() as u32,
    sha256: hex::encode(digest.finalize()),
  };
  let mut remaining_rows = MAX_RESTORE_PREVIEW_ROWS;
  let mut remaining_bytes = MAX_RESTORE_PREVIEW_BYTES;
  for paths in [
    &mut created_paths,
    &mut overwritten_paths,
    &mut deleted_paths,
  ] {
    let keep = paths
      .iter()
      .take_while(|path| {
        if remaining_rows == 0 || path.len() > remaining_bytes {
          return false;
        }
        remaining_rows -= 1;
        remaining_bytes -= path.len();
        true
      })
      .count();
    paths.truncate(keep);
  }
  PreflightVykarRestoreResponse {
    created_paths,
    overwritten_paths,
    deleted_paths,
    path_summary: Some(summary),
    ..Default::default()
  }
}

#[derive(Deserialize)]
struct BackupComposeConfig {
  #[serde(default)]
  services: HashMap<String, BackupComposeService>,
}

#[derive(Default, Deserialize)]
struct BackupComposeService {
  #[serde(default)]
  volumes: Vec<BackupComposeMount>,
}

#[derive(Clone, Deserialize)]
#[serde(untagged)]
enum BackupComposeMount {
  Short(String),
  Long {
    #[serde(rename = "type")]
    mount_type: Option<String>,
    source: Option<String>,
    target: Option<String>,
  },
}

#[derive(Default)]
struct OperationCancellationRegistry {
  active: HashMap<String, Arc<AtomicBool>>,
  registrations: HashMap<String, usize>,
  pending: HashMap<String, Instant>,
}

impl OperationCancellationRegistry {
  fn prune_pending(&mut self, now: Instant) {
    self.pending.retain(|_, expires_at| *expires_at > now);
  }
}

fn cancellation_registry()
-> &'static Mutex<OperationCancellationRegistry> {
  static REGISTRY: OnceLock<Mutex<OperationCancellationRegistry>> =
    OnceLock::new();
  REGISTRY.get_or_init(Default::default)
}

fn operation_cancellation_token(
  operation_id: &str,
) -> Arc<AtomicBool> {
  cancellation_registry()
    .lock()
    .unwrap()
    .active
    .get(operation_id)
    .expect("backup operation cancellation was not registered")
    .clone()
}

struct OperationCancellationRegistration(String);

impl Drop for OperationCancellationRegistration {
  fn drop(&mut self) {
    let mut registry = cancellation_registry().lock().unwrap();
    if let Some(count) = registry.registrations.get_mut(&self.0) {
      *count -= 1;
      if *count == 0 {
        registry.registrations.remove(&self.0);
        registry.active.remove(&self.0);
      }
    }
  }
}

fn register_operation_cancellation(
  operation_id: &str,
) -> (Arc<AtomicBool>, OperationCancellationRegistration) {
  let mut registry = cancellation_registry().lock().unwrap();
  let now = Instant::now();
  registry.prune_pending(now);
  let cancelled = registry.pending.remove(operation_id).is_some();
  let token = registry
    .active
    .entry(operation_id.to_string())
    .or_insert_with(|| Arc::new(AtomicBool::new(cancelled)))
    .clone();
  *registry
    .registrations
    .entry(operation_id.to_string())
    .or_default() += 1;
  (
    token,
    OperationCancellationRegistration(operation_id.to_string()),
  )
}

fn operation_cancelled(operation_id: &str) -> bool {
  cancellation_registry()
    .lock()
    .unwrap()
    .active
    .get(operation_id)
    .is_some_and(|token| token.load(Ordering::SeqCst))
}

fn request_operation_cancellation(operation_id: &str) -> bool {
  let mut registry = cancellation_registry().lock().unwrap();
  if let Some(token) = registry.active.get(operation_id) {
    token.store(true, Ordering::SeqCst);
    return true;
  }

  // Cancellation and backup requests use separate HTTP connections, so the
  // cancellation can arrive first. Retain only a short-lived, size-bounded
  // marker; registration consumes it atomically under this same lock.
  let now = Instant::now();
  registry.prune_pending(now);
  if registry.pending.len() >= MAX_PENDING_CANCELLATIONS
    && let Some(oldest) = registry
      .pending
      .iter()
      .min_by_key(|(_, expires_at)| **expires_at)
      .map(|(operation_id, _)| operation_id.clone())
  {
    registry.pending.remove(&oldest);
  }
  registry
    .pending
    .insert(operation_id.to_string(), now + PENDING_CANCELLATION_TTL);
  true
}

fn backup_operation_lock() -> &'static tokio::sync::Mutex<()> {
  static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
  LOCK.get_or_init(Default::default)
}

#[derive(Debug)]
struct ExcludedBackupSource(String);

impl std::fmt::Display for ExcludedBackupSource {
  fn fmt(
    &self,
    formatter: &mut std::fmt::Formatter<'_>,
  ) -> std::fmt::Result {
    formatter.write_str(&self.0)
  }
}

impl std::error::Error for ExcludedBackupSource {}

#[derive(Serialize, Deserialize)]
struct BackupCompletionReceipt {
  core: String,
  run_id: String,
  #[serde(default)]
  batch: Option<bool>,
  #[serde(default)]
  kind: Option<BackupDispatchKind>,
  /// Durable finalization proof before acknowledgement can erase its journal.
  #[serde(default)]
  finalized: Option<FinalizeVykarRestoreResponse>,
  completion: VykarBackupCompletion,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
enum BackupDispatchKind {
  Backup,
  BackupBatch,
  Restore {
    journal_id: String,
    deferred: bool,
  },
  FinalizeRestore {
    journal_id: String,
    restore_operation_id: String,
    commit: bool,
    acknowledge: bool,
  },
}

impl BackupCompletionReceipt {
  fn kind(&self) -> Option<BackupDispatchKind> {
    self.kind.clone().or_else(|| {
      self.batch.map(|batch| {
        if batch {
          BackupDispatchKind::BackupBatch
        } else {
          BackupDispatchKind::Backup
        }
      })
    })
  }
}

fn backup_completion_lock() -> &'static Mutex<()> {
  static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
  LOCK.get_or_init(Default::default)
}

fn backup_completion_dir() -> anyhow::Result<PathBuf> {
  let directory =
    internal_storage_dir().join("backup-completion-journals");
  std::fs::create_dir_all(&directory)?;
  std::fs::set_permissions(
    &directory,
    std::fs::Permissions::from_mode(0o700),
  )?;
  fsync_parent(&internal_storage_dir())?;
  fsync_parent(&directory)?;
  Ok(directory)
}

fn backup_completion_path(
  directory: &Path,
  operation_id: &str,
) -> anyhow::Result<PathBuf> {
  let operation_id = uuid::Uuid::parse_str(operation_id)
    .context("Backup dispatch requires a valid operation UUID")?;
  Ok(directory.join(format!("{operation_id}.json")))
}

fn read_backup_completion(
  path: &Path,
) -> anyhow::Result<Option<BackupCompletionReceipt>> {
  match std::fs::read(path) {
    Ok(bytes) => serde_json::from_slice(&bytes)
      .map(Some)
      .context("Invalid backup completion receipt"),
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
      Ok(None)
    }
    Err(error) => Err(error.into()),
  }
}

fn check_backup_completion_owner(
  receipt: &BackupCompletionReceipt,
  core: &str,
  run_id: &str,
) -> anyhow::Result<()> {
  if receipt.core != core || receipt.run_id != run_id {
    return Err(anyhow!(
      "Backup dispatch identity belongs to another Core or run"
    ));
  }
  Ok(())
}

fn claim_backup_completion(
  directory: &Path,
  operation_id: &str,
  core: &str,
  run_id: &str,
  batch: bool,
) -> anyhow::Result<Option<VykarBackupCompletion>> {
  claim_dispatch_completion(
    directory,
    operation_id,
    core,
    run_id,
    if batch {
      BackupDispatchKind::BackupBatch
    } else {
      BackupDispatchKind::Backup
    },
  )
}

fn claim_dispatch_completion(
  directory: &Path,
  operation_id: &str,
  core: &str,
  run_id: &str,
  kind: BackupDispatchKind,
) -> anyhow::Result<Option<VykarBackupCompletion>> {
  if core.is_empty() || run_id.is_empty() {
    return Err(anyhow!(
      "Dispatch requires an authenticated Core and run identity"
    ));
  }
  let _lock = backup_completion_lock().lock().unwrap();
  let path = backup_completion_path(directory, operation_id)?;
  if let Some(receipt) = read_backup_completion(&path)? {
    check_backup_completion_owner(&receipt, core, run_id)?;
    if receipt.kind().is_some_and(|existing| existing != kind) {
      return Err(anyhow!(
        "Backup dispatch identity has a different operation kind"
      ));
    }
    if matches!(
      receipt.completion.state,
      VykarBackupCompletionState::Unknown
        | VykarBackupCompletionState::Running
    ) {
      return Err(anyhow!(
        "Backup dispatch is already running; query its completion receipt"
      ));
    }
    return Ok(Some(receipt.completion));
  }
  persist_journal(
    &path,
    &BackupCompletionReceipt {
      core: core.into(),
      run_id: run_id.into(),
      batch: None,
      kind: Some(kind),
      finalized: None,
      completion: VykarBackupCompletion {
        state: VykarBackupCompletionState::Running,
        ..Default::default()
      },
    },
  )?;
  Ok(None)
}

fn finish_backup_completion(
  directory: &Path,
  operation_id: &str,
  core: &str,
  run_id: &str,
  completion: VykarBackupCompletion,
) -> anyhow::Result<()> {
  let _lock = backup_completion_lock().lock().unwrap();
  let path = backup_completion_path(directory, operation_id)?;
  let mut receipt = read_backup_completion(&path)?
    .context("Backup dispatch receipt disappeared")?;
  check_backup_completion_owner(&receipt, core, run_id)?;
  if receipt.completion.state != VykarBackupCompletionState::Running {
    return Err(anyhow!("Backup dispatch receipt is not running"));
  }
  receipt.completion = completion;
  persist_journal(&path, &receipt)
}

fn query_backup_completion(
  directory: &Path,
  request: &GetVykarBackupCompletion,
  core: &str,
) -> anyhow::Result<VykarBackupCompletion> {
  let _lock = backup_completion_lock().lock().unwrap();
  let path =
    backup_completion_path(directory, &request.operation_id)?;
  let mut receipt = match read_backup_completion(&path)? {
    Some(receipt) => receipt,
    None if request.cancel_if_unknown => {
      // Serialized with dispatch claim: a late request cannot pass this fence.
      let receipt = BackupCompletionReceipt {
        core: core.into(),
        run_id: request.run_id.clone(),
        batch: None,
        kind: None,
        finalized: None,
        completion: VykarBackupCompletion {
          state: VykarBackupCompletionState::Complete,
          error: Some(
            "Backup dispatch was fenced before it started".into(),
          ),
          ..Default::default()
        },
      };
      persist_journal(&path, &receipt)?;
      receipt
    }
    None => return Ok(VykarBackupCompletion::default()),
  };
  check_backup_completion_owner(&receipt, core, &request.run_id)?;
  let response = receipt.completion.clone();
  if request.acknowledge
    && receipt.completion.state
      == VykarBackupCompletionState::Complete
  {
    // Keep the identity forever: deleting it could admit a delayed dispatch.
    receipt.completion.result = None;
    receipt.completion.batch_result = None;
    receipt.completion.restore_result = None;
    receipt.completion.finalize_restore_result = None;
    receipt.completion.error =
      Some("Backup completion was already acknowledged".into());
    persist_journal(&path, &receipt)?;
  }
  Ok(response)
}

impl Resolve<Args> for GetVykarBackupCompletion {
  async fn resolve(
    self,
    args: &Args,
  ) -> anyhow::Result<VykarBackupCompletion> {
    query_backup_completion(
      &backup_completion_dir()?,
      &self,
      &args.core,
    )
  }
}

fn recover_backup_completions_in(
  directory: &Path,
) -> anyhow::Result<()> {
  let _lock = backup_completion_lock().lock().unwrap();
  for entry in std::fs::read_dir(directory)? {
    let path = entry?.path();
    if path.extension().and_then(|value| value.to_str())
      != Some("json")
    {
      continue;
    }
    let mut receipt = read_backup_completion(&path)?
      .context("Backup completion disappeared during recovery")?;
    if receipt.completion.state
      == VykarBackupCompletionState::Complete
    {
      continue;
    }
    let (journal, pending, original) = match receipt.kind() {
      Some(BackupDispatchKind::Restore { journal_id, .. }) => (
        read_restore_journal(&restore_journal_path(&journal_id)?)?,
        restore_has_pending_journals(&journal_id)?,
        None,
      ),
      Some(BackupDispatchKind::FinalizeRestore {
        journal_id,
        restore_operation_id,
        ..
      }) => (
        read_restore_journal(&restore_journal_path(&journal_id)?)?,
        restore_has_pending_journals(&journal_id)?,
        if restore_operation_id.is_empty() {
          None
        } else {
          read_backup_completion(&backup_completion_path(
            directory,
            &restore_operation_id,
          )?)?
        },
      ),
      _ => (None, false, None),
    };
    receipt.completion = recovered_dispatch_completion(
      &receipt,
      journal.as_ref(),
      pending,
      original.as_ref(),
    );
    persist_journal(&path, &receipt)?;
  }
  Ok(())
}

impl Resolve<Args> for DiscoverBackupSource {
  async fn resolve(
    self,
    _: &Args,
  ) -> anyhow::Result<DiscoverBackupSourceResponse> {
    static SLOTS: OnceLock<Arc<tokio::sync::Semaphore>> =
      OnceLock::new();
    let runtime = tokio::runtime::Handle::current();
    let deadline = Instant::now() + Duration::from_secs(60);
    bounded_backup_discovery(
      SLOTS
        .get_or_init(|| Arc::new(tokio::sync::Semaphore::new(1)))
        .clone(),
      Duration::from_secs(60),
      move || {
        runtime.block_on(async {
          // Cancel pending async Docker queries at the same deadline. Only a
          // synchronous filesystem call that cannot yet return retains the slot.
          tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            discover_source(
              &self.target,
              &self.protected_repository_paths,
              &self.filters,
            ),
          )
          .await
          .context("Backup source discovery exceeded 60 seconds")?
        })
      },
    )
    .await
  }
}

impl Resolve<Args> for RunVykarBackup {
  async fn resolve(
    self,
    args: &Args,
  ) -> anyhow::Result<RunVykarBackupResponse> {
    let directory = backup_completion_dir()?;
    if let Some(completion) = claim_backup_completion(
      &directory,
      &self.operation_id,
      &args.core,
      &self.run_id,
      false,
    )? {
      return completion.result.ok_or_else(|| {
        anyhow!(completion.error.unwrap_or_else(|| {
          "Backup dispatch has no replayable result".into()
        }))
      });
    }
    let core = args.core.clone();
    let (_, cancellation_registration) =
      register_operation_cancellation(&self.run_id);
    // The task owns both work and completion publication. Dropping an HTTP
    // waiter cannot release guards while blocking Vykar work is still active.
    tokio::spawn(async move {
      let operation_id = self.operation_id.clone();
      let run_id = self.run_id.clone();
      let result = self.run().await;
      drop(cancellation_registration);
      let completion = VykarBackupCompletion {
        state: VykarBackupCompletionState::Complete,
        result: result.as_ref().ok().cloned(),
        error: result
          .as_ref()
          .err()
          .map(|error| format!("{error:#}")),
        ..Default::default()
      };
      finish_backup_completion(
        &directory,
        &operation_id,
        &core,
        &run_id,
        completion,
      )?;
      result
    })
    .await
    .context(
      "Backup operation task failed; completion remains uncertain",
    )?
  }
}

async fn bounded_backup_discovery<T: Send + 'static>(
  slots: Arc<tokio::sync::Semaphore>,
  timeout: Duration,
  work: impl FnOnce() -> anyhow::Result<T> + Send + 'static,
) -> anyhow::Result<T> {
  let permit = slots.try_acquire_owned().context("Backup source discovery is already running; retry after it finishes")?;
  let worker = tokio::task::spawn_blocking(move || {
    // Discovery includes synchronous path inspection. If that is stalled on
    // storage, response expiry must not admit another detached discovery.
    let _permit = permit;
    work()
  });
  tokio::time::timeout(timeout, worker)
    .await
    .context("Backup source discovery exceeded 60 seconds")?
    .context("Backup source discovery worker failed")?
}

trait RunBackupOperation {
  type Response;
  async fn run(self) -> anyhow::Result<Self::Response>;
}

impl RunBackupOperation for RunVykarBackup {
  type Response = RunVykarBackupResponse;

  async fn run(self) -> anyhow::Result<RunVykarBackupResponse> {
    let _operation = backup_operation_lock().lock().await;
    if operation_cancelled(&self.run_id) {
      return Err(anyhow!(
        "Backup cancelled before worker admission"
      ));
    }
    let _filesystem = protected_filesystem_guard()?;
    ensure_no_pending_recovery()?;
    let discovered = discover_source(
      &self.target,
      &self.protected_repository_paths,
      &self.filters,
    )
    .await?;
    let container_journal = if self.stop_containers {
      persist_container_quiesce_journal(
        &self.run_id,
        &discovered.running_containers,
      )?
    } else {
      None
    };
    let mut stopped: Vec<String> = Vec::new();
    if self.stop_containers {
      for container in &discovered.running_containers {
        if let Err(error) =
          run_container_command("stop", container).await
        {
          let (restarted, restart_errors) =
            restart_quiesced_containers(
              container_journal.as_deref(),
              &stopped,
            )
            .await?;
          if !restart_errors.is_empty() {
            return Ok(RunVykarBackupResponse {
              primary: VykarBackupRepositoryResult {
                error: Some(format!(
                  "Failed to quiesce every affected container: {error:#}"
                )),
                ..Default::default()
              },
              stopped_containers: stopped,
              restarted_containers: restarted,
              restart_errors,
              ..Default::default()
            });
          }
          return Err(error.context(
            "Failed to quiesce every affected container; already stopped containers were restarted",
          ));
        }
        stopped.push(container.clone());
      }
    }

    let result = if operation_cancelled(&self.run_id) {
      Err(anyhow!("Backup cancelled before repository write"))
    } else {
      run_backup_repositories(&self, &discovered.paths).await
    };

    let (restarted, restart_errors) = restart_quiesced_containers(
      container_journal.as_deref(),
      &stopped,
    )
    .await?;

    let (primary, mirror) = match result {
      Ok(result) => result,
      Err(error) if !restart_errors.is_empty() => {
        return Ok(RunVykarBackupResponse {
          primary: VykarBackupRepositoryResult {
            error: Some(format!("{error:#}")),
            ..Default::default()
          },
          stopped_containers: stopped,
          restarted_containers: restarted,
          restart_errors,
          ..Default::default()
        });
      }
      Err(error) => return Err(error),
    };
    Ok(RunVykarBackupResponse {
      excluded: None,
      primary,
      mirror,
      stopped_containers: stopped,
      restarted_containers: restarted,
      restart_errors,
    })
  }
}

impl Resolve<Args> for RunVykarBackupBatch {
  async fn resolve(
    self,
    args: &Args,
  ) -> anyhow::Result<RunVykarBackupBatchResponse> {
    let directory = backup_completion_dir()?;
    if let Some(completion) = claim_backup_completion(
      &directory,
      &self.operation_id,
      &args.core,
      &self.run_id,
      true,
    )? {
      return completion.batch_result.ok_or_else(|| {
        anyhow!(completion.error.unwrap_or_else(|| {
          "Backup dispatch has no replayable batch result".into()
        }))
      });
    }
    let core = args.core.clone();
    let (_, cancellation_registration) =
      register_operation_cancellation(&self.run_id);
    tokio::spawn(async move {
      let operation_id = self.operation_id.clone();
      let run_id = self.run_id.clone();
      let result = self.run().await;
      drop(cancellation_registration);
      let completion = VykarBackupCompletion {
        state: VykarBackupCompletionState::Complete,
        batch_result: result.as_ref().ok().cloned(),
        error: result
          .as_ref()
          .err()
          .map(|error| format!("{error:#}")),
        ..Default::default()
      };
      finish_backup_completion(
        &directory,
        &operation_id,
        &core,
        &run_id,
        completion,
      )?;
      result
    })
    .await
    .context(
      "Backup batch task failed; completion remains uncertain",
    )?
  }
}

impl RunBackupOperation for RunVykarBackupBatch {
  type Response = RunVykarBackupBatchResponse;

  async fn run(self) -> anyhow::Result<RunVykarBackupBatchResponse> {
    let _operation = backup_operation_lock().lock().await;
    if operation_cancelled(&self.run_id) {
      return Err(anyhow!(
        "Backup cancelled before worker admission"
      ));
    }
    let _filesystem = protected_filesystem_guard()?;
    ensure_no_pending_recovery()?;
    let mut discovered = Vec::new();
    let mut results = Vec::new();
    let mut discovery_errors = Vec::new();
    let mut running = BTreeSet::new();
    for task in self.tasks {
      match discover_source(
        &task.target,
        &self.protected_repository_paths,
        &self.filters,
      )
      .await
      {
        Ok(source) => {
          running.extend(source.running_containers.iter().cloned());
          discovered.push((task, source.paths));
        }
        Err(error) if error.is::<ExcludedBackupSource>() => {
          results.push(VykarBackupTaskResult {
            source_label: task.source_label,
            result: RunVykarBackupResponse {
              excluded: Some(error.to_string()),
              ..Default::default()
            },
          });
        }
        Err(error) => discovery_errors
          .push(format!("{}: {error:#}", task.source_label)),
      }
    }
    let running = running.into_iter().collect::<Vec<_>>();
    let container_journal = if self.stop_containers {
      persist_container_quiesce_journal(&self.run_id, &running)?
    } else {
      None
    };
    let mut stopped: Vec<String> = Vec::new();
    if self.stop_containers {
      for container in running {
        if let Err(error) =
          run_container_command("stop", &container).await
        {
          let (_, restart_errors) = restart_quiesced_containers(
            container_journal.as_deref(),
            &stopped,
          )
          .await?;
          if !restart_errors.is_empty() {
            return Ok(RunVykarBackupBatchResponse {
              discovery_errors: vec![format!(
                "Failed to quiesce every affected container on the node: {error:#}"
              )],
              restart_errors,
              ..Default::default()
            });
          }
          return Err(error.context(
            "Failed to quiesce every affected container on the node",
          ));
        }
        stopped.push(container);
      }
    }

    for (task, paths) in discovered {
      if operation_cancelled(&self.run_id) {
        break;
      }
      let request = RunVykarBackup {
        operation_id: self.operation_id.clone(),
        target: task.target,
        primary: self.primary.clone(),
        mirror: self.mirror.clone(),
        advanced: self.advanced.clone(),
        hostname: self.hostname.clone(),
        source_label: task.source_label.clone(),
        snapshot_name: task.snapshot_name,
        run_id: self.run_id.clone(),
        komodo_version: self.komodo_version.clone(),
        protected_repository_paths: self
          .protected_repository_paths
          .clone(),
        filters: self.filters.clone(),
        stop_containers: false,
        mirror_only: task.mirror_only,
        primary_only: task.primary_only,
      };
      match run_backup_repositories(&request, &paths).await {
        Ok((primary, mirror)) => {
          results.push(VykarBackupTaskResult {
            source_label: task.source_label,
            result: RunVykarBackupResponse {
              primary,
              mirror,
              ..Default::default()
            },
          })
        }
        Err(error) => discovery_errors
          .push(format!("{}: {error:#}", task.source_label)),
      }
    }

    let (_, restart_errors) = restart_quiesced_containers(
      container_journal.as_deref(),
      &stopped,
    )
    .await?;
    Ok(RunVykarBackupBatchResponse {
      results,
      discovery_errors,
      restart_errors,
    })
  }
}

async fn run_backup_repositories(
  request: &RunVykarBackup,
  source_paths: &[String],
) -> anyhow::Result<(
  VykarBackupRepositoryResult,
  Option<VykarBackupRepositoryResult>,
)> {
  if request.primary_only && request.mirror_only {
    return Err(anyhow!(
      "A backup retry cannot be both primary-only and mirror-only"
    ));
  }
  if (request.primary_only || request.mirror_only)
    && request.mirror.is_none()
  {
    return Err(anyhow!(
      "Repository-specific retry requested without a configured mirror"
    ));
  }
  let manifest_staging = backup_manifest_staging_dir();
  std::fs::create_dir_all(&manifest_staging).with_context(|| {
    format!(
      "Failed to create backup manifest staging root {}",
      manifest_staging.display()
    )
  })?;
  let manifest_dir = manifest_staging
    .join(backup_manifest_source_name(&request.snapshot_name));
  // Operations are serialized. Removing a same-snapshot directory here
  // recovers staging left by a process exit before the drop guard ran.
  remove_path(&manifest_dir)?;
  std::fs::create_dir(&manifest_dir).with_context(|| {
    format!(
      "Failed to create backup manifest staging directory {}",
      manifest_dir.display()
    )
  })?;
  let _manifest_cleanup =
    RemovePathsOnDrop(vec![manifest_dir.clone()]);
  write_manifest(request, source_paths, &manifest_dir)?;
  let mut paths = source_paths.to_vec();
  paths.push(manifest_dir.to_string_lossy().into_owned());

  let primary = if request.mirror_only {
    VykarBackupRepositoryResult {
      complete: true,
      ..Default::default()
    }
  } else {
    run_repository_backup(
      request.primary.clone(),
      request,
      paths.clone(),
    )
    .await
  };
  if operation_cancelled(&request.run_id) {
    return Err(anyhow!("Backup cancelled before mirror write"));
  }
  let mirror = if request.primary_only {
    request
      .mirror
      .as_ref()
      .map(|_| VykarBackupRepositoryResult {
        complete: true,
        ..Default::default()
      })
  } else if let Some(repository) = request.mirror.clone() {
    Some(run_repository_backup(repository, request, paths).await)
  } else {
    None
  };
  Ok((primary, mirror))
}

fn backup_manifest_staging_dir() -> PathBuf {
  periphery_config()
    .stack_dir()
    .join(".komodo-vykar")
    .join("backup-manifests")
}

async fn run_repository_backup(
  repository: komodo_client::entities::backup::BackupRepository,
  request: &RunVykarBackup,
  source_paths: Vec<String>,
) -> VykarBackupRepositoryResult {
  let advanced = request.advanced.clone();
  let hostname = request.hostname.clone();
  let snapshot_name = request.snapshot_name.clone();
  let source_label = request.source_label.clone();
  let cancellation = operation_cancellation_token(&request.run_id);
  let one_file_system =
    !request.filters.include_cross_filesystem_mounts;
  let result = tokio::task::spawn_blocking(move || {
    let cache = vykar_cache_dir(&hostname)?;
    let repository = VykarRepository::new(
      &repository,
      &hostname,
      &cache,
      &cache,
      &advanced,
    )?;
    repository.backup_cancellable_with_options(
      &snapshot_name,
      &source_label,
      &source_paths,
      Some(cancellation.as_ref()),
      one_file_system,
    )
  })
  .await;
  match result {
    Ok(Ok(result)) => VykarBackupRepositoryResult {
      complete: !result.partial,
      partial: result.partial,
      files: result.files,
      original_size: result.original_size,
      stored_size: result.stored_size,
      error: None,
    },
    Ok(Err(error)) => VykarBackupRepositoryResult {
      error: Some(format!("{error:#}")),
      ..Default::default()
    },
    Err(error) => VykarBackupRepositoryResult {
      error: Some(format!("Vykar worker failed: {error}")),
      ..Default::default()
    },
  }
}

#[derive(Serialize)]
struct KomodoBackupManifest<'a> {
  schema: &'static str,
  version: u32,
  run_id: &'a str,
  source_label: &'a str,
  hostname: &'a str,
  komodo_version: &'a str,
  paths: &'a [String],
  path_aliases: &'a BTreeMap<String, String>,
  target: &'a PeripheryBackupTarget,
  configuration_sha256: String,
  paths_sha256: String,
  path_aliases_sha256: String,
}

fn write_manifest(
  request: &RunVykarBackup,
  paths: &[String],
  directory: &Path,
) -> anyhow::Result<()> {
  let target = serde_json::to_vec(&request.target)
    .context("Failed to serialize backup source identity")?;
  let path_aliases = backup_manifest_path_aliases(request, paths)?;
  let manifest = KomodoBackupManifest {
    schema: "komodo.backup-manifest/v1",
    version: 1,
    run_id: &request.run_id,
    source_label: &request.source_label,
    hostname: &request.hostname,
    komodo_version: &request.komodo_version,
    paths,
    path_aliases: &path_aliases,
    target: &request.target,
    configuration_sha256: hex::encode(Sha256::digest(target)),
    paths_sha256: hex::encode(Sha256::digest(
      serde_json::to_vec(paths)
        .context("Failed to serialize backup source paths")?,
    )),
    path_aliases_sha256: hex::encode(Sha256::digest(
      serde_json::to_vec(&path_aliases)
        .context("Failed to serialize backup source path aliases")?,
    )),
  };
  let bytes = serde_json::to_vec_pretty(&manifest)
    .context("Failed to serialize backup manifest")?;
  let path = directory.join("komodo-backup-manifest.json");
  let mut file = OpenOptions::new()
    .create_new(true)
    .write(true)
    .open(&path)
    .with_context(|| {
      format!("Failed to create {}", path.display())
    })?;
  file.write_all(&bytes)?;
  file.sync_all()?;
  Ok(())
}

fn backup_manifest_path_aliases(
  request: &RunVykarBackup,
  paths: &[String],
) -> anyhow::Result<BTreeMap<String, String>> {
  let PeripheryBackupTarget::Stack { stack, .. } = &request.target
  else {
    return Ok(BTreeMap::new());
  };
  let run_directory = paths
    .first()
    .context("Stack backup has no run-directory source")?;
  compose_bind_path_aliases(stack, Path::new(run_directory))
}

fn paths_overlap(left: &Path, right: &Path) -> anyhow::Result<bool> {
  komodo_backup::filesystem::paths_overlap(left, right)
}

pub(crate) fn internal_storage_dir() -> PathBuf {
  periphery_config().stack_dir().join(".komodo-vykar")
}

fn validate_path_outside_internal_storage(
  path: &Path,
  internal_storage: &Path,
  label: &str,
) -> anyhow::Result<()> {
  if paths_overlap(path, internal_storage)? {
    return Err(ExcludedBackupSource(format!(
      "{label} '{}' overlaps Periphery's internal backup storage '{}'",
      path.display(),
      internal_storage.display()
    )).into());
  }
  Ok(())
}

fn validate_resolved_restore_destinations(
  publish: &[RestorePublishPath],
) -> anyhow::Result<()> {
  validate_resolved_restore_destinations_against(
    publish,
    &internal_storage_dir(),
  )
}

fn validate_resolved_restore_destinations_against(
  publish: &[RestorePublishPath],
  internal_storage: &Path,
) -> anyhow::Result<()> {
  let destinations = publish
    .iter()
    .map(|item| {
      validate_restore_destination_ancestors(item)?;
      let destination = Path::new(&item.destination);
      if komodo_backup::filesystem::entry_overlaps_path(
        destination, internal_storage,
      )? {
        return Err(anyhow!("Restore destination overlaps Periphery's internal backup storage"));
      }
      Ok((item.destination.as_str(), destination))
    })
    .collect::<anyhow::Result<Vec<_>>>()?;
  for (index, (left_label, left)) in destinations.iter().enumerate() {
    for (right_label, right) in destinations.iter().skip(index + 1) {
      if komodo_backup::filesystem::entry_paths_overlap(left, right)?
      {
        return Err(anyhow!(
          "Restore destinations overlap after resolving filesystem aliases: '{left_label}' and '{right_label}'"
        ));
      }
    }
  }
  Ok(())
}

async fn validate_restore_destinations(
  publish: &[RestorePublishPath],
  protected_repository_paths: &[ProtectedRepositoryPath],
) -> anyhow::Result<()> {
  validate_resolved_restore_destinations(publish)?;
  let docker_guard = docker_client().load();
  let docker = docker_guard
    .as_ref()
    .as_ref()
    .context("Docker is unavailable")?;
  let containers = docker.list_containers().await?;
  let protected_repository_sources =
    resolve_protected_repository_sources(
      docker,
      &containers,
      protected_repository_paths,
      true,
    )
    .await?;
  for item in publish {
    for protected in &protected_repository_sources {
      if komodo_backup::filesystem::entry_overlaps_path(
        Path::new(&item.destination),
        protected,
      )? {
        return Err(anyhow!(
          "Restore destination overlaps protected repository storage"
        ));
      }
    }
  }
  Ok(())
}

fn validate_restore_destination_ancestors(
  item: &RestorePublishPath,
) -> anyhow::Result<()> {
  let destination = Path::new(&item.destination);
  if !destination.is_absolute()
    || destination
      .components()
      .any(|part| matches!(part, std::path::Component::ParentDir))
  {
    return Err(anyhow!(
      "Restore destination is not an absolute normalized path"
    ));
  }
  if let Some(root) = item.destination_root.as_deref().map(Path::new)
    && (!root.is_absolute()
      || root
        .components()
        .any(|part| matches!(part, std::path::Component::ParentDir))
      || !destination.starts_with(root))
  {
    return Err(anyhow!(
      "Selected restore destination is outside its confirmed absolute root"
    ));
  }
  // A full mapped destination has no selection boundary. Both forms still
  // need every ancestor checked from the filesystem root, not just the leaf.
  let root = Path::new("/");
  let relative = destination.strip_prefix(root)?;
  let mut ancestor = root.to_path_buf();
  for part in relative.components() {
    match std::fs::symlink_metadata(&ancestor) {
      Ok(metadata)
        if metadata.file_type().is_symlink()
          || !metadata.is_dir() =>
      {
        return Err(anyhow!(
          "Restore cannot traverse symlink or non-directory ancestor '{}'",
          ancestor.display()
        ));
      }
      Ok(_) => {}
      Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
        return Ok(());
      }
      Err(error) => return Err(error.into()),
    }
    ancestor.push(part.as_os_str());
  }
  // The leaf itself may be a symlink: publication replaces that entry.
  Ok(())
}

fn insert_bind_backup_root(
  bind_paths: &mut BTreeSet<PathBuf>,
  run_directory: &Path,
  path: &Path,
) -> anyhow::Result<()> {
  let bind = validate_source_path(path)?;
  if bind == run_directory || bind.starts_with(run_directory) {
    // Vykar traverses mounts, so the Stack root already captures a bind below
    // it.
    return Ok(());
  }
  if run_directory.starts_with(&bind) {
    return Err(anyhow!(
      "Bind source '{}' contains the Stack run directory '{}'; overlapping backup roots cannot be restored atomically",
      bind.display(),
      run_directory.display()
    ));
  }
  if paths_overlap(run_directory, &bind)? {
    return Err(anyhow!(
      "Bind source '{}' aliases the Stack run directory; use one non-overlapping source namespace",
      bind.display()
    ));
  }
  if bind_paths.iter().any(|existing| bind.starts_with(existing)) {
    // An ancestor already captures this tree. Keeping both roots would make
    // the resulting full snapshot impossible to publish atomically.
    return Ok(());
  }
  for existing in bind_paths.iter() {
    if !existing.starts_with(&bind) && paths_overlap(existing, &bind)?
    {
      return Err(anyhow!(
        "Selected bind roots '{}' and '{}' overlap through filesystem aliases",
        existing.display(),
        bind.display()
      ));
    }
  }
  bind_paths.retain(|existing| !existing.starts_with(&bind));
  bind_paths.insert(bind);
  Ok(())
}

fn compose_bind_paths(
  stack: &komodo_client::entities::stack::Stack,
  run_directory: &Path,
) -> anyhow::Result<BTreeSet<PathBuf>> {
  let Some(config) = stack.info.deployed_config.as_deref() else {
    return Ok(BTreeSet::new());
  };
  let config: BackupComposeConfig =
    serde_yaml_ng::from_str(config)
      .context("Failed to parse deployed Compose configuration")?;
  let mut paths = BTreeSet::new();
  for mount in config
    .services
    .into_values()
    .flat_map(|service| service.volumes)
  {
    let source = compose_bind_source(mount);
    let Some(source) = source else {
      continue;
    };
    let source = Path::new(&source);
    let source = if source.is_absolute() {
      source.to_path_buf()
    } else {
      run_directory.join(source)
    };
    // Do not discard descendants before include/exclude policy is applied.
    paths.insert(validate_source_path(&source)?);
  }
  Ok(paths)
}

fn compose_bind_source(mount: BackupComposeMount) -> Option<String> {
  match mount {
    BackupComposeMount::Long {
      mount_type, source, ..
    } => source.filter(|source| {
      mount_type.as_deref() == Some("bind")
        || mount_type.is_none() && Path::new(source).is_absolute()
    }),
    BackupComposeMount::Short(value) => {
      split_compose_short_mount(&value).and_then(|(source, _)| {
        (Path::new(source).is_absolute() || source.starts_with('.'))
          .then(|| source.to_string())
      })
    }
  }
}

fn compose_bind_path_aliases(
  stack: &komodo_client::entities::stack::Stack,
  _run_directory: &Path,
) -> anyhow::Result<BTreeMap<String, String>> {
  let Some(config) = stack.info.deployed_config.as_deref() else {
    return Ok(BTreeMap::new());
  };
  let config: BackupComposeConfig =
    serde_yaml_ng::from_str(config)
      .context("Failed to parse deployed Compose configuration")?;
  let mut aliases = BTreeMap::new();
  for mount in config
    .services
    .into_values()
    .flat_map(|service| service.volumes)
  {
    let Some(source) = compose_bind_source(mount) else {
      continue;
    };
    let source_path = Path::new(&source);
    if !source_path.is_absolute() {
      // Relative bind paths move with the recovered run directory and do not
      // need an absolute source rewrite.
      continue;
    }
    let canonical = validate_source_path(source_path)?;
    if canonical != source_path {
      aliases
        .insert(source, canonical.to_string_lossy().into_owned());
    }
  }
  Ok(aliases)
}

fn remap_absolute_bind_source(
  source: &str,
  mappings: &HashMap<String, String>,
  path_aliases: &HashMap<String, String>,
) -> Option<String> {
  let source = Path::new(
    path_aliases
      .get(source)
      .map(String::as_str)
      .unwrap_or(source),
  );
  if !source.is_absolute() {
    return None;
  }
  mappings
    .iter()
    .filter_map(|(from, to)| {
      let from = Path::new(from);
      source.strip_prefix(from).ok().map(|relative| {
        (
          from.components().count(),
          Path::new(to).join(relative).to_string_lossy().into_owned(),
        )
      })
    })
    .max_by_key(|(depth, _)| *depth)
    .map(|(_, mapped)| mapped)
}

fn split_compose_short_mount(value: &str) -> Option<(&str, &str)> {
  let mut braces = 0_u32;
  for (index, character) in value.char_indices() {
    match character {
      '{' => braces += 1,
      '}' => braces = braces.saturating_sub(1),
      ':' if braces == 0 => {
        return Some((&value[..index], &value[index + 1..]));
      }
      _ => {}
    }
  }
  None
}

fn compose_mount_target(mount: &BackupComposeMount) -> Option<&str> {
  match mount {
    BackupComposeMount::Long { target, .. } => target.as_deref(),
    BackupComposeMount::Short(value) => {
      split_compose_short_mount(value)
        .map(|(_, suffix)| suffix.split(':').next().unwrap_or(suffix))
    }
  }
}

/// Associate relative sources and expressions with the authenticated original
/// deployment by service and mount target. Neither the recovery host's env nor
/// the already-remapped run directory describes the original bind source.
fn resolve_recovered_bind_expressions(
  document: &mut serde_yaml_ng::Value,
  deployed_config: Option<&str>,
  mappings: &HashMap<String, String>,
  aliases: &HashMap<String, String>,
) -> anyhow::Result<()> {
  use serde_yaml_ng::Value;
  let deployed = deployed_config
    .map(serde_yaml_ng::from_str::<BackupComposeConfig>)
    .transpose()
    .context(
      "Failed to parse snapshot's deployed Compose configuration",
    )?;
  let key = |value: &str| Value::String(value.into());
  let Some(services) =
    document.get_mut("services").and_then(Value::as_mapping_mut)
  else {
    return Ok(());
  };
  for (service_name, service) in services {
    let Some(volumes) =
      service.get_mut("volumes").and_then(Value::as_sequence_mut)
    else {
      continue;
    };
    for volume in volumes {
      let parsed: BackupComposeMount =
        serde_yaml_ng::from_value(volume.clone())?;
      let source = match &parsed {
        BackupComposeMount::Short(value) => {
          split_compose_short_mount(value).map(|(source, _)| source)
        }
        BackupComposeMount::Long {
          mount_type, source, ..
        } => {
          if mount_type.as_deref().is_some_and(|kind| kind != "bind")
          {
            continue;
          }
          source.as_deref()
        }
      };
      let Some(source) = source else {
        continue;
      };
      let relative_bind = !Path::new(source).is_absolute()
        && (source.starts_with('.')
          || matches!(
            &parsed,
            BackupComposeMount::Long { mount_type, .. }
              if mount_type.as_deref() == Some("bind")
          ));
      if !relative_bind && !source.contains('$') {
        continue;
      }
      let target = compose_mount_target(&parsed)
        .context("Cannot identify a relative or environment-expanded Compose mount target")?;
      let deployed_mount = deployed.as_ref()
        .and_then(|config| config.services.get(service_name.as_str()?))
        .and_then(|service| service.volumes.iter().find(|mount| compose_mount_target(mount) == Some(target)))
        .context("Cannot resolve a relative or environment-expanded Compose mount from snapshot deployment metadata")?;
      let Some(expanded) =
        compose_bind_source(deployed_mount.clone())
      else {
        if relative_bind {
          return Err(anyhow!(
            "Snapshot deployment metadata does not identify the relative bind source for mount '{target}'"
          ));
        }
        // The deployed expression names a Docker volume, not a bind source.
        continue;
      };
      if !Path::new(&expanded).is_absolute() {
        return Err(anyhow!(
          "Snapshot deployment metadata has no absolute bind source for mount '{target}'"
        ));
      }
      if remap_absolute_bind_source(&expanded, mappings, aliases)
        .is_none()
      {
        // Intentionally excluded roots have no confirmed mapping.
        continue;
      }
      match volume {
        Value::String(short) => {
          let (_, suffix) = split_compose_short_mount(short).unwrap();
          *short = format!("{expanded}:{suffix}");
        }
        Value::Mapping(long) => {
          long.insert(key("source"), Value::String(expanded));
        }
        _ => unreachable!(),
      }
    }
  }
  Ok(())
}

fn rewrite_compose_bind_mappings(
  document: &mut serde_yaml_ng::Value,
  mappings: &HashMap<String, String>,
  path_aliases: &HashMap<String, String>,
) -> usize {
  use serde_yaml_ng::Value;

  let key = |value: &str| Value::String(value.into());
  let Some(services) = document
    .as_mapping_mut()
    .and_then(|root| root.get_mut(key("services")))
    .and_then(Value::as_mapping_mut)
  else {
    return 0;
  };
  let mut rewritten = 0;
  for service in services.values_mut() {
    let Some(volumes) = service
      .as_mapping_mut()
      .and_then(|service| service.get_mut(key("volumes")))
      .and_then(Value::as_sequence_mut)
    else {
      continue;
    };
    for volume in volumes {
      match volume {
        Value::String(short) => {
          let Some((source, suffix)) =
            split_compose_short_mount(short)
          else {
            continue;
          };
          if let Some(mapped) =
            remap_absolute_bind_source(source, mappings, path_aliases)
          {
            *short = format!("{mapped}:{suffix}");
            rewritten += 1;
          }
        }
        Value::Mapping(long) => {
          let mount_type = long
            .get(key("type"))
            .and_then(Value::as_str)
            .map(str::to_owned);
          let Some(source) = long
            .get_mut(key("source"))
            .and_then(|value| value.as_str())
            .map(str::to_owned)
          else {
            continue;
          };
          if mount_type.as_deref().is_some_and(|kind| kind != "bind")
          {
            continue;
          }
          if let Some(mapped) = remap_absolute_bind_source(
            &source,
            mappings,
            path_aliases,
          ) {
            long.insert(key("source"), Value::String(mapped));
            rewritten += 1;
          }
        }
        _ => {}
      }
    }
  }
  rewritten
}

/// Open only regular files reached through real directories in staging.
/// Use the same descriptor for reading and rewriting, never a truncating
/// pathname open that could follow a restored link into the host.
fn open_staged_compose_file(
  staging: &Path,
  path: &Path,
) -> anyhow::Result<std::fs::File> {
  let relative = path.strip_prefix(staging)?;
  let mut current = staging.to_path_buf();
  if !std::fs::symlink_metadata(&current)?.is_dir() {
    return Err(anyhow!(
      "Compose staging root must be a real directory"
    ));
  }
  let mut components = relative.components().peekable();
  while let Some(component) = components.next() {
    if !matches!(component, std::path::Component::Normal(_)) {
      return Err(anyhow!("Unsafe staged Compose path"));
    }
    current.push(component);
    let metadata = std::fs::symlink_metadata(&current)?;
    let valid = if components.peek().is_some() {
      metadata.is_dir()
    } else {
      metadata.is_file()
    };
    if !valid {
      return Err(anyhow!(
        "Recovered Compose paths must not contain symlinks or special files: {}",
        current.display()
      ));
    }
  }
  let file = OpenOptions::new()
    .read(true)
    .write(true)
    .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
    .open(path)?;
  if !file.metadata()?.is_file() {
    return Err(anyhow!(
      "Recovered Compose file must be a regular file"
    ));
  }
  Ok(file)
}

fn rewrite_recovered_stack_compose_files(
  request: &TransactionalVykarRestore,
  staging: &Path,
) -> anyhow::Result<()> {
  let PeripheryBackupTarget::Stack { stack, .. } = &request.target
  else {
    return Ok(());
  };
  if request.bind_path_mappings.is_empty() {
    return Ok(());
  }
  let run_directory = Path::new(&stack.config.run_directory);
  let run_root = request
    .publish
    .iter()
    .find(|item| Path::new(&item.destination) == run_directory)
    .context(
      "Recovered Stack publish plan has no run-directory root",
    )?;
  let staged_run_directory = staging.join(&run_root.snapshot_path);
  for compose_file in stack.compose_file_paths() {
    let relative = Path::new(compose_file);
    if relative.is_absolute()
      || relative.components().any(|component| {
        matches!(component, std::path::Component::ParentDir)
      })
    {
      return Err(anyhow!(
        "Recovered Stack Compose path is unsafe: {compose_file}"
      ));
    }
    let path = staged_run_directory.join(relative);
    let mut file = open_staged_compose_file(staging, &path)?;
    let mut text = String::new();
    file.read_to_string(&mut text).with_context(|| {
      format!(
        "Failed to read recovered Compose file {}",
        path.display()
      )
    })?;
    let mut document: serde_yaml_ng::Value =
      serde_yaml_ng::from_str(&text).with_context(|| {
        format!(
          "Failed to parse recovered Compose file {}",
          path.display()
        )
      })?;
    resolve_recovered_bind_expressions(
      &mut document,
      stack.info.deployed_config.as_deref(),
      &request.bind_path_mappings,
      &request.bind_path_aliases,
    )?;
    if rewrite_compose_bind_mappings(
      &mut document,
      &request.bind_path_mappings,
      &request.bind_path_aliases,
    ) == 0
    {
      continue;
    }
    let rewritten = serde_yaml_ng::to_string(&document)?;
    use std::io::Seek;
    file.rewind()?;
    file.set_len(0)?;
    file.write_all(rewritten.as_bytes())?;
    file.sync_all()?;
  }
  Ok(())
}

async fn affected_running_containers(
  docker: &crate::docker::DockerClient,
  containers: &[ContainerListItem],
  target: &PeripheryBackupTarget,
  paths: &BTreeSet<PathBuf>,
  protected_paths: &[ProtectedRepositoryPath],
  replacing_entries: bool,
) -> anyhow::Result<Vec<String>> {
  ensure_target_not_control_plane(
    containers,
    target,
    protected_paths,
  )?;
  // The worker mounts Docker's volume root to perform this operation. Its own
  // filesystem gate already excludes other mutations; stopping it would kill
  // the backup/restore before it can restart application containers.
  let own_id = komodo_backup::container::current_container_id();
  let own_mounts = if let Some(own) =
    containers.iter().find(|container| {
      own_id
        .as_deref()
        .is_some_and(|id| container_matches_id(container, id))
    }) {
    docker.inspect_container(&own.name).await?.mounts
  } else {
    Vec::new()
  };
  let mut affected = running_containers_for_target(
    containers,
    target,
    own_id.as_deref(),
    protected_paths,
  )
  .into_iter()
  .collect::<BTreeSet<_>>();
  for container in containers.iter().filter(|container| {
    container_is_quiesce_candidate(container, own_id.as_deref())
      && !is_core_container(container, protected_paths)
  }) {
    if affected.contains(&container.name) {
      continue;
    }
    let inspected = docker.inspect_container(&container.name).await?;
    for mount in inspected.mounts {
      if mount_affects_paths(
        mount.typ.as_deref(),
        mount.source.as_deref(),
        paths,
        &own_mounts,
        replacing_entries,
      )? {
        affected.insert(container.name.clone());
        break;
      }
    }
  }
  Ok(affected.into_iter().collect())
}

fn mount_type_affects_paths(mount_type: Option<&str>) -> bool {
  matches!(mount_type, Some("bind" | "volume"))
}

fn mount_affects_paths(
  mount_type: Option<&str>,
  source: Option<&str>,
  paths: &BTreeSet<PathBuf>,
  own_mounts: &[komodo_client::entities::docker::container::MountPoint],
  replacing_entries: bool,
) -> anyhow::Result<bool> {
  if !mount_type_affects_paths(mount_type) {
    return Ok(false);
  }
  let Some(source) = source else {
    return Ok(false);
  };
  let source = PathBuf::from(source);
  let mut sources = vec![source.clone()];
  // Docker reports host paths. Translate only through this verified worker's
  // mounts, never an unrelated application's container-side namespace.
  for mount in own_mounts {
    if let (Some(host), Some(local)) =
      (&mount.source, &mount.destination)
      && let Some(alias) = map_path_through_mount(
        &source,
        Path::new(host),
        Path::new(local),
      )
    {
      sources.push(alias);
    }
  }
  for source in &sources {
    for path in paths {
      let overlaps = if replacing_entries {
        komodo_backup::filesystem::entry_overlaps_path(path, source)?
      } else {
        paths_overlap(source, path)?
      };
      if overlaps {
        return Ok(true);
      }
    }
  }
  Ok(false)
}

async fn discover_source(
  target: &PeripheryBackupTarget,
  protected_repository_paths: &[ProtectedRepositoryPath],
  filters: &BackupSourceFilters,
) -> anyhow::Result<DiscoverBackupSourceResponse> {
  let docker_guard = docker_client().load();
  let docker = docker_guard
    .as_ref()
    .as_ref()
    .context("Docker is unavailable")?;
  let containers = docker.list_containers().await?;
  let protected_repository_sources =
    resolve_protected_repository_sources(
      docker,
      &containers,
      protected_repository_paths,
      true,
    )
    .await?;
  match target {
    PeripheryBackupTarget::Stack { stack, repo } => {
      if !stack.config.swarm_id.is_empty() {
        return Err(anyhow!(
          "Swarm stacks are not supported by backup v1"
        ));
      }
      let run_directory = validate_source_path(
        &crate::stack::write::resolved_run_directory(
          stack,
          repo.as_deref(),
        ),
      )?;
      let mut bind_paths = compose_bind_paths(stack, &run_directory)?;
      let project_name = stack.project_name(false);
      for container in containers.iter().filter(|container| {
        container.labels.get(COMPOSE_PROJECT_LABEL)
          == Some(&project_name)
      }) {
        let inspected =
          docker.inspect_container(&container.name).await?;
        for mount in inspected
          .mounts
          .into_iter()
          .filter(|mount| mount.typ.as_deref() == Some("bind"))
        {
          let source = mount
            .source
            .context("Bind mount did not report a source path")?;
          bind_paths
            .insert(validate_source_path(Path::new(&source))?);
        }
      }
      let bind_paths = select_bind_backup_roots(
        bind_paths,
        &run_directory,
        filters,
      )?;
      let internal_storage =
        periphery_config().stack_dir().join(".komodo-vykar");
      validate_path_outside_internal_storage(
        &run_directory,
        &internal_storage,
        "Backup source",
      )?;
      validate_path_outside_protected_repositories(
        &run_directory,
        &protected_repository_sources,
        "Backup source",
      )?;
      for bind_path in &bind_paths {
        validate_path_outside_internal_storage(
          bind_path,
          &internal_storage,
          "Backup source",
        )?;
        validate_path_outside_protected_repositories(
          bind_path,
          &protected_repository_sources,
          "Backup source",
        )?;
      }
      let mut affected_paths = bind_paths.clone();
      affected_paths.insert(run_directory.clone());
      let running = affected_running_containers(
        docker,
        &containers,
        target,
        &affected_paths,
        protected_repository_paths,
        false,
      )
      .await?;
      let mut paths =
        vec![run_directory.to_string_lossy().into_owned()];
      paths.extend(
        bind_paths
          .into_iter()
          .map(|path| path.to_string_lossy().into_owned()),
      );
      Ok(DiscoverBackupSourceResponse {
        paths,
        running_containers: running,
      })
    }
    PeripheryBackupTarget::Volume { volume_name } => {
      if volume_name.trim().is_empty() {
        return Err(anyhow!("Volume name cannot be empty"));
      }
      let volume = docker.inspect_volume(volume_name).await?;
      if volume.driver != "local"
        || volume.scope != VolumeScopeEnum::Local
      {
        return Err(anyhow!(
          "Backup v1 supports only local named volumes; '{}' uses driver '{}' with scope {:?}",
          volume.name,
          volume.driver,
          volume.scope
        ));
      }
      validate_local_volume_mount_options(&volume.options)?;
      if !filters.include_anonymous_volumes
        && is_anonymous_volume(&volume.name, &volume.labels)
      {
        return Err(anyhow!(
          "Anonymous Docker volume '{}' is excluded by backup settings",
          volume.name
        ));
      }
      let mountpoint =
        validate_source_path(Path::new(&volume.mountpoint))?;
      validate_path_outside_internal_storage(
        &mountpoint,
        &periphery_config().stack_dir().join(".komodo-vykar"),
        "Backup source",
      )?;
      validate_path_outside_protected_repositories(
        &mountpoint,
        &protected_repository_sources,
        "Backup source",
      )?;
      let running_containers = affected_running_containers(
        docker,
        &containers,
        target,
        &BTreeSet::from([mountpoint.clone()]),
        protected_repository_paths,
        false,
      )
      .await?;
      Ok(DiscoverBackupSourceResponse {
        paths: vec![mountpoint.to_string_lossy().into_owned()],
        running_containers,
      })
    }
  }
}

fn select_bind_backup_roots(
  paths: BTreeSet<PathBuf>,
  run_directory: &Path,
  filters: &BackupSourceFilters,
) -> anyhow::Result<BTreeSet<PathBuf>> {
  let include =
    VykarPatternMatcher::new(&filters.bind_mount_include_patterns)
      .context("Invalid bind-mount include patterns")?;
  let exclude =
    VykarPatternMatcher::new(&filters.bind_mount_exclude_patterns)
      .context("Invalid bind-mount exclude patterns")?;
  let run_device = std::fs::metadata(run_directory)
    .with_context(|| {
      format!(
        "Failed to inspect Stack run-directory filesystem: {}",
        run_directory.display()
      )
    })?
    .dev();
  let mut selected = BTreeSet::new();
  for path in paths {
    let metadata = std::fs::metadata(&path).with_context(|| {
      format!(
        "Failed to inspect bind-mount filesystem: {}",
        path.display()
      )
    })?;
    if !filters.bind_mount_include_patterns.is_empty()
      && !include.matches(&path, metadata.is_dir())
    {
      continue;
    }
    if exclude.matches(&path, metadata.is_dir()) {
      continue;
    }
    if !filters.include_cross_filesystem_mounts
      && metadata.dev() != run_device
    {
      continue;
    }
    insert_bind_backup_root(&mut selected, run_directory, &path)?;
  }
  Ok(selected)
}

fn validate_local_volume_mount_options(
  options: &HashMap<String, String>,
) -> anyhow::Result<()> {
  if ["type", "device"].iter().any(|key| {
    options.get(*key).is_some_and(|value| !value.is_empty())
  }) {
    return Err(ExcludedBackupSource(
      "Mount-backed local Docker volumes (including bind/NFS volumes) are not supported: their inspected mountpoint is not a stable data mount; use an ordinary local volume or back up the underlying storage separately".into(),
    ).into());
  }
  Ok(())
}

fn unfiltered_source_filters() -> BackupSourceFilters {
  BackupSourceFilters {
    include_cross_filesystem_mounts: true,
    include_anonymous_volumes: true,
    ..Default::default()
  }
}

pub(crate) async fn file_manager_protected_sources(
  docker: &crate::docker::DockerClient,
  protected_paths: &[ProtectedRepositoryPath],
) -> anyhow::Result<Vec<PathBuf>> {
  let containers = docker.list_containers().await?;
  resolve_protected_repository_sources(
    docker,
    &containers,
    protected_paths,
    false,
  )
  .await
}

async fn resolve_protected_repository_sources(
  docker: &crate::docker::DockerClient,
  containers: &[ContainerListItem],
  protected_repository_paths: &[ProtectedRepositoryPath],
  include_skipped: bool,
) -> anyhow::Result<Vec<PathBuf>> {
  let mut sources = BTreeSet::new();
  let own_id = komodo_backup::container::current_container_id();
  for container in containers.iter().filter(|container| {
    is_core_container(container, protected_repository_paths)
      || (include_skipped
        && container_backup_is_skipped(container)
        && !own_id
          .as_deref()
          .is_some_and(|id| container_matches_id(container, id)))
  }) {
    let inspected = docker.inspect_container(&container.name).await?;
    for mount in inspected.mounts {
      if !mount_type_affects_paths(mount.typ.as_deref()) {
        continue;
      }
      let Some(destination) = mount.destination.map(PathBuf::from)
      else {
        continue;
      };
      let Some(source) = mount.source.map(PathBuf::from) else {
        continue;
      };
      if container_backup_is_skipped(container)
        && !is_core_container(container, protected_repository_paths)
      {
        // Refusing these sources (even for stopped containers) avoids either
        // stopping the database or taking an inconsistent live database copy.
        sources.insert(source.canonicalize().unwrap_or(source));
        continue;
      }
      for repository in
        protected_repository_paths.iter().filter(|repository| {
          container_matches_id(
            container,
            &repository.core_container_id,
          )
        })
      {
        let Some(mapped) = map_path_through_mount(
          Path::new(&repository.path),
          &destination,
          &source,
        ) else {
          continue;
        };
        sources.insert(mapped.canonicalize().unwrap_or(mapped));
      }
    }
  }
  // Host-side sources must also be protected through Periphery's own mounts
  // (the standard deployment shares /data with Core). Never borrow the
  // container-side namespace of an unrelated application or a remote Core.
  if !sources.is_empty() {
    let own_id = komodo_backup::container::current_container_id()
      .context("Cannot identify the Periphery Docker container for repository protection")?;
    let own = containers
      .iter()
      .find(|container| container_matches_id(container, &own_id))
      .context(
        "Periphery container is not visible in its Docker daemon",
      )?;
    let inspected = docker.inspect_container(&own.name).await?;
    let host_sources = sources.clone();
    for mount in inspected.mounts {
      let (Some(host), Some(local)) =
        (mount.source, mount.destination)
      else {
        continue;
      };
      let host = PathBuf::from(host);
      let host = host.canonicalize().unwrap_or(host);
      for repository in &host_sources {
        if let Some(alias) =
          map_path_through_mount(repository, &host, Path::new(&local))
        {
          sources.insert(alias.canonicalize().unwrap_or(alias));
        }
      }
    }
  }
  if let Some(own) = containers.iter().find(|container| {
    own_id
      .as_deref()
      .is_some_and(|id| container_matches_id(container, id))
  }) {
    // Core and Periphery may share the development container's namespace.
    // Protect its private files even when they are not Docker-mounted paths.
    sources.extend(
      protected_repository_paths
        .iter()
        .filter(|path| {
          container_matches_id(own, &path.core_container_id)
        })
        .map(|path| PathBuf::from(&path.path)),
    );
  }
  Ok(sources.into_iter().collect())
}

fn container_matches_id(
  container: &ContainerListItem,
  id: &str,
) -> bool {
  (id.len() == 12 || id.len() == 64)
    && id.bytes().all(|byte| {
      byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
    })
    && container.id.as_deref().is_some_and(|candidate| {
      candidate.len() == 64 && candidate.starts_with(id)
    })
}

fn map_path_through_mount(
  repository: &Path,
  mount_destination: &Path,
  mount_source: &Path,
) -> Option<PathBuf> {
  if let Ok(relative) = repository.strip_prefix(mount_destination) {
    // The mount contains the repository. Protect only the corresponding
    // subtree on the host so siblings in the same shared volume remain
    // eligible backup and restore roots.
    Some(mount_source.join(relative))
  } else if mount_destination.starts_with(repository) {
    // The entire mounted source is nested beneath the repository.
    Some(mount_source.to_path_buf())
  } else {
    None
  }
}

pub(super) fn validate_path_outside_protected_repositories(
  path: &Path,
  protected_repository_sources: &[PathBuf],
  label: &str,
) -> anyhow::Result<()> {
  for repository in protected_repository_sources {
    if paths_overlap(path, repository)? {
      return Err(ExcludedBackupSource(format!(
        "{label} '{}' overlaps protected Core, repository, or skipped-container storage '{}'",
        path.display(),
        repository.display()
      )).into());
    }
  }
  Ok(())
}

async fn discover_running_containers(
  target: &PeripheryBackupTarget,
  publish: &[RestorePublishPath],
  protected_paths: &[ProtectedRepositoryPath],
) -> anyhow::Result<Vec<String>> {
  let docker_guard = docker_client().load();
  let docker = docker_guard
    .as_ref()
    .as_ref()
    .context("Docker is unavailable")?;
  let containers = docker.list_containers().await?;
  let paths = publish
    .iter()
    .map(|item| PathBuf::from(&item.destination))
    .collect::<BTreeSet<_>>();
  affected_running_containers(
    docker,
    &containers,
    target,
    &paths,
    protected_paths,
    true,
  )
  .await
}

fn is_core_container(
  container: &ContainerListItem,
  protected_paths: &[ProtectedRepositoryPath],
) -> bool {
  protected_paths.iter().any(|path| {
    container_matches_id(container, &path.core_container_id)
  })
}

fn container_backup_is_skipped(
  container: &ContainerListItem,
) -> bool {
  container
    .labels
    .get("komodo.skip")
    .is_some_and(|value| value != "false")
}

fn ensure_target_not_control_plane(
  containers: &[ContainerListItem],
  target: &PeripheryBackupTarget,
  protected_paths: &[ProtectedRepositoryPath],
) -> anyhow::Result<()> {
  for container in containers.iter().filter(|container| {
    is_core_container(container, protected_paths)
      || container_backup_is_skipped(container)
  }) {
    let consumes_target = match target {
      PeripheryBackupTarget::Stack { stack, .. } => {
        container.labels.get(COMPOSE_PROJECT_LABEL)
          == Some(&stack.project_name(false))
      }
      PeripheryBackupTarget::Volume { volume_name } => {
        container.volumes.contains(volume_name)
      }
    };
    if consumes_target {
      return Err(ExcludedBackupSource(format!("Backup/restore target belongs to protected container '{}'; use Core's logical backup and retain its private recovery material separately", container.name)).into());
    }
  }
  Ok(())
}

fn running_containers_for_target(
  containers: &[ContainerListItem],
  target: &PeripheryBackupTarget,
  own_id: Option<&str>,
  protected_paths: &[ProtectedRepositoryPath],
) -> Vec<String> {
  match target {
    PeripheryBackupTarget::Stack { stack, .. } => {
      let project_name = stack.project_name(false);
      containers
        .iter()
        .filter(|container| {
          container_is_quiesce_candidate(container, own_id)
            && !is_core_container(container, protected_paths)
            && container.labels.get(COMPOSE_PROJECT_LABEL)
              == Some(&project_name)
        })
        .map(|container| container.name.clone())
        .collect()
    }
    PeripheryBackupTarget::Volume { volume_name } => containers
      .iter()
      .filter(|container| {
        container_is_quiesce_candidate(container, own_id)
          && !is_core_container(container, protected_paths)
          && container.volumes.contains(volume_name)
      })
      .map(|container| container.name.clone())
      .collect(),
  }
}

fn container_is_quiesce_candidate(
  container: &ContainerListItem,
  own_id: Option<&str>,
) -> bool {
  container.state == ContainerStateStatusEnum::Running
    && !own_id.is_some_and(|id| container_matches_id(container, id))
    && !container_backup_is_skipped(container)
}

fn validate_source_path(path: &Path) -> anyhow::Result<PathBuf> {
  if !path.is_absolute() {
    return Err(anyhow!(
      "Backup source must be absolute: {}",
      path.display()
    ));
  }
  path.canonicalize().with_context(|| {
    format!("Backup source is unavailable: {}", path.display())
  })
}

async fn run_container_command(
  action: &str,
  container: &str,
) -> anyhow::Result<()> {
  let log = run_komodo_standard_command(
    &format!("Backup {action} container {container}"),
    &format!("docker {action} -- {}", escape(container.into())),
    CommandOptions::default(),
  )
  .await;
  if log.success {
    Ok(())
  } else {
    Err(anyhow!("{}", log.stderr))
  }
}

async fn create_restore_volume(
  volume_name: &str,
  journal_id: &str,
) -> anyhow::Result<()> {
  let label = format!("{RESTORE_PLAN_VOLUME_LABEL}={journal_id}");
  let log = run_komodo_standard_command(
    &format!("Backup create restore volume {volume_name}"),
    &format!(
      "docker volume create --label {} -- {}",
      escape(label.into()),
      escape(volume_name.into())
    ),
    CommandOptions::default(),
  )
  .await;
  if log.success {
    Ok(())
  } else {
    Err(anyhow!("{}", log.stderr))
  }
}

async fn remove_restore_volume(
  volume_name: &str,
) -> anyhow::Result<()> {
  let log = run_komodo_standard_command(
    &format!("Backup remove restore volume {volume_name}"),
    &format!("docker volume rm -- {}", escape(volume_name.into())),
    CommandOptions::default(),
  )
  .await;
  if log.success {
    Ok(())
  } else {
    Err(anyhow!("{}", log.stderr))
  }
}

async fn prepare_restore_volume(
  volume_name: &str,
  restore_plan_id: &str,
  journal_id: &str,
  create_if_missing: bool,
  deadline: Instant,
) -> anyhow::Result<Option<PathBuf>> {
  let docker_guard = docker_client().load();
  let docker = docker_guard
    .as_ref()
    .as_ref()
    .context("Docker is unavailable")?;
  let exists = restore_execution_before_deadline(deadline, async {
    let containers = docker.list_containers().await?;
    Ok(
      docker
        .list_volumes(&containers)
        .await?
        .into_iter()
        .any(|volume| volume.name == volume_name),
    )
  })
  .await?;
  if !create_if_missing {
    if exists {
      return Ok(None);
    }
    return Err(anyhow!(
      "Destination volume '{volume_name}' no longer exists; create a new restore preflight"
    ));
  }
  if exists {
    let volume = restore_execution_before_deadline(
      deadline,
      docker.inspect_volume(volume_name),
    )
    .await?;
    if volume
      .labels
      .get(RESTORE_PLAN_VOLUME_LABEL)
      .map(String::as_str)
      != Some(restore_plan_id)
    {
      return Err(anyhow!(
        "Destination volume '{volume_name}' now exists; create a new restore preflight and explicitly confirm overwrite"
      ));
    }
  }
  let journal = persist_restore_volume_journal(
    journal_id,
    volume_name,
    restore_plan_id,
  )?;
  if !exists {
    let created = async {
      create_restore_volume(volume_name, restore_plan_id).await?;
      let volume = restore_execution_before_deadline(
        deadline,
        docker.inspect_volume(volume_name),
      )
      .await?;
      if volume
        .labels
        .get(RESTORE_PLAN_VOLUME_LABEL)
        .map(String::as_str)
        != Some(restore_plan_id)
      {
        return Err(anyhow!(
          "Destination volume '{volume_name}' was created concurrently by another process; restore aborted"
        ));
      }
      Ok(())
    }
    .await;
    if let Err(error) = created {
      let cleanup =
        cleanup_owned_restore_volume_journal(&journal).await;
      return match cleanup {
        Ok(()) => Err(error),
        Err(cleanup) => Err(error.context(format!(
          "Created restore Volume cleanup failed: {cleanup:#}"
        ))),
      };
    }
  }
  Ok(Some(journal))
}

async fn restart_containers(
  containers: &[String],
) -> (Vec<String>, Vec<String>) {
  let mut restarted = Vec::new();
  let mut errors = Vec::new();
  for container in containers {
    match run_container_command("start", container).await {
      Ok(()) => restarted.push(container.clone()),
      Err(error) => errors.push(format!("{container}: {error:#}")),
    }
  }
  (restarted, errors)
}

fn vykar_cache_dir(hostname: &str) -> anyhow::Result<PathBuf> {
  let directory = periphery_config()
    .stack_dir()
    .join(".komodo-vykar")
    .join(hex::encode(Sha256::digest(hostname.as_bytes())));
  std::fs::create_dir_all(&directory).with_context(|| {
    format!("Failed to create Vykar cache at {}", directory.display())
  })?;
  Ok(directory)
}

fn resolve_volume_publish_destinations(
  publish: &mut [RestorePublishPath],
  volume_name: &str,
  mountpoint: &str,
  full_restore: bool,
) -> anyhow::Result<()> {
  let mountpoint = Path::new(mountpoint);
  let logical_root = Path::new("/var/lib/docker/volumes")
    .join(volume_name)
    .join("_data");
  for item in publish {
    let destination = if full_restore {
      mountpoint.to_path_buf()
    } else {
      let relative = Path::new(&item.destination)
        .strip_prefix(&logical_root)
        .with_context(|| {
          format!(
            "Selected Volume destination '{}' is outside logical root '{}'",
            item.destination,
            logical_root.display()
          )
        })?;
      mountpoint.join(relative)
    };
    item.destination = destination.to_string_lossy().into_owned();
    if !full_restore {
      item.destination_root =
        Some(mountpoint.to_string_lossy().into_owned());
    }
  }
  Ok(())
}

impl Resolve<Args> for TransactionalVykarRestore {
  async fn resolve(
    self,
    args: &Args,
  ) -> anyhow::Result<TransactionalVykarRestoreResponse> {
    let directory = backup_completion_dir()?;
    validate_restore_journal_id(&self.journal_id)?;
    if let Some(completion) = claim_dispatch_completion(
      &directory,
      &self.operation_id,
      &args.core,
      &self.run_id,
      BackupDispatchKind::Restore {
        journal_id: self.journal_id.clone(),
        deferred: self.defer_finalize,
      },
    )? {
      return completion.restore_result.ok_or_else(|| {
        anyhow!(completion.error.unwrap_or_else(|| {
          "Restore dispatch has no replayable result".into()
        }))
      });
    }
    let args = Args {
      core: args.core.clone(),
      id: args.id,
    };
    let (_, registration) =
      register_operation_cancellation(&self.journal_id);
    tokio::spawn(async move {
      let operation_id = self.operation_id.clone();
      let run_id = self.run_id.clone();
      let journal_id = self.journal_id.clone();
      let result = self.run_restore(&args).await;
      drop(registration);
      let state = if result
        .as_ref()
        .is_ok_and(|result| result.finalization_pending)
      {
        VykarBackupCompletionState::Prepared
      } else if restore_has_pending_journals(&journal_id)? {
        VykarBackupCompletionState::RecoveryRequired
      } else {
        VykarBackupCompletionState::Complete
      };
      finish_backup_completion(
        &directory,
        &operation_id,
        &args.core,
        &run_id,
        VykarBackupCompletion {
          state,
          restore_result: result.as_ref().ok().cloned(),
          error: result
            .as_ref()
            .err()
            .map(|error| format!("{error:#}")),
          ..Default::default()
        },
      )?;
      result
    })
    .await
    .context("Restore task failed; completion remains uncertain")?
  }
}

impl Resolve<Args> for RunTransactionalVykarRestore {
  async fn resolve(
    self,
    args: &Args,
  ) -> anyhow::Result<TransactionalVykarRestoreResponse> {
    self.0.resolve(args).await
  }
}

trait RunRestoreOperation {
  async fn run_restore(
    self,
    args: &Args,
  ) -> anyhow::Result<TransactionalVykarRestoreResponse>;
}

impl RunRestoreOperation for TransactionalVykarRestore {
  async fn run_restore(
    mut self,
    args: &Args,
  ) -> anyhow::Result<TransactionalVykarRestoreResponse> {
    let _operation = backup_operation_lock().lock().await;
    if operation_cancelled(&self.journal_id) {
      return Err(anyhow!(
        "Restore cancelled before worker admission"
      ));
    }
    let _filesystem = protected_filesystem_guard()?;
    ensure_no_pending_recovery()?;
    let current_preview = PreflightVykarRestore {
      target: self.target.clone(),
      repository: self.repository.clone(),
      protected_repository_paths: self
        .protected_repository_paths
        .clone(),
      advanced: self.advanced.clone(),
      hostname: self.hostname.clone(),
      snapshot_name: self.snapshot_name.clone(),
      selected_paths: self.selected_paths.clone(),
      publish: self.publish.clone(),
    }
    .resolve(args)
    .await?;
    if !self.expected_preview.matches(&current_preview) {
      return Err(anyhow!(
        "Restore preview changed before the destination filesystem was locked; create and review a fresh preflight"
      ));
    }
    let preparation_deadline =
      Instant::now() + RESTORE_PREFLIGHT_TIMEOUT;
    let owned_volume_journal =
      if let PeripheryBackupTarget::Volume { volume_name } =
        &self.target
      {
        let volume_restore_plan_id =
          if self.volume_restore_plan_id.is_empty() {
            &self.journal_id
          } else {
            &self.volume_restore_plan_id
          };
        prepare_restore_volume(
          volume_name,
          volume_restore_plan_id,
          &self.journal_id,
          self.create_volume_if_missing,
          preparation_deadline,
        )
        .await?
      } else {
        None
      };
    let target = self.target.clone();
    let protected = self.protected_repository_paths.clone();
    let mut publish = self.publish.clone();
    let full_restore = self.selected_paths.is_empty();
    let runtime = tokio::runtime::Handle::current();
    let preparation = bounded_restore_execution_read(
      preparation_deadline,
      move || {
        runtime.block_on(restore_execution_before_deadline(
          preparation_deadline,
          async {
            if let PeripheryBackupTarget::Volume { volume_name } =
              &target
            {
              let mountpoint = discover_source(
                &target,
                &protected,
                &unfiltered_source_filters(),
              )
              .await?
              .paths
              .into_iter()
              .next()
              .context("Destination volume has no mountpoint")?;
              resolve_volume_publish_destinations(
                &mut publish,
                volume_name,
                &mountpoint,
                full_restore,
              )?;
            }
            validate_restore_destinations(&publish, &protected)
              .await?;
            let running_containers = discover_running_containers(
              &target, &publish, &protected,
            )
            .await?;
            anyhow::Ok((publish, running_containers))
          },
        ))
      },
    )
    .await;
    let (publish, running_containers) = match preparation {
      Ok(prepared) => prepared,
      Err(error) => {
        if let Some(journal) = owned_volume_journal.as_deref()
          && let Err(cleanup) =
            cleanup_owned_restore_volume_journal(journal).await
        {
          return Err(error.context(format!(
            "Created restore Volume cleanup failed: {cleanup:#}"
          )));
        }
        return Err(error);
      }
    };
    self.publish = publish;
    // Only the confirmed original-running set may be stopped. It remains the
    // restart authority even if a stop acknowledgement is lost.
    let mut expected_running =
      self.expected_preview.containers_to_stop.clone();
    let mut current_running = running_containers.clone();
    expected_running.sort();
    current_running.sort();
    if expected_running != current_running {
      if let Some(journal) = owned_volume_journal.as_deref() {
        cleanup_owned_restore_volume_journal(journal).await?;
      }
      return Err(anyhow!(
        "Affected containers changed after confirmation; create a fresh restore preview"
      ));
    }
    let container_journal = match persist_container_quiesce_journal(
      &self.journal_id,
      &running_containers,
    ) {
      Ok(journal) => journal,
      Err(error) => {
        if let Some(journal) = owned_volume_journal.as_deref() {
          cleanup_owned_restore_volume_journal(journal).await?;
        }
        return Err(error);
      }
    };
    let mut stopped_containers: Vec<String> = Vec::new();
    for container in &running_containers {
      if let Err(stop_error) =
        run_container_command("stop", container).await
      {
        let (restarted, restart_errors) =
          restart_quiesced_containers(
            container_journal.as_deref(),
            &stopped_containers,
          )
          .await?;
        let volume_cleanup_error =
          if let Some(journal) = owned_volume_journal.as_deref() {
            cleanup_owned_restore_volume_journal(journal).await.err()
          } else {
            None
          };
        return Ok(TransactionalVykarRestoreResponse {
          complete: false,
          rolled_back: volume_cleanup_error.is_none(),
          finalization_pending: false,
          containers_restarted: if restart_errors.is_empty()
            && volume_cleanup_error.is_none()
          {
            restarted
          } else {
            Vec::new()
          },
          critical_error: if volume_cleanup_error.is_some()
            || !restart_errors.is_empty()
          {
            Some(format!(
              "Restore quiesce failed ({stop_error:#}); created Volume cleanup: {}; container restart: {}",
              volume_cleanup_error
                .map(|error| format!("failed: {error:#}"))
                .unwrap_or_else(|| "complete".into()),
              if restart_errors.is_empty() {
                "complete".into()
              } else {
                restart_errors.join("; ")
              }
            ))
          } else {
            None
          },
        });
      }
      stopped_containers.push(container.clone());
    }

    let restore_result = match verify_quiesced_restore_preview(
      &self,
      &running_containers,
    )
    .await
    {
      Ok(()) => transactional_restore(&self).await,
      Err(error) => {
        RestoreTransactionResult::FailedBeforePublication(error)
      }
    };
    let rolled_back = match restore_result {
      RestoreTransactionResult::Published {
        rolled_back,
        finalization_pending,
      } => {
        if rolled_back
          && let Some(journal) = owned_volume_journal.as_deref()
          && let Err(error) =
            cleanup_owned_restore_volume_journal(journal).await
        {
          return Ok(TransactionalVykarRestoreResponse {
            complete: false,
            rolled_back: false,
            finalization_pending: false,
            containers_restarted: Vec::new(),
            critical_error: Some(format!(
              "Restore rolled back but its created Volume could not be removed; affected containers remain stopped: {error:#}"
            )),
          });
        }
        if finalization_pending {
          return Ok(TransactionalVykarRestoreResponse {
            complete: true,
            rolled_back: false,
            finalization_pending: true,
            containers_restarted: Vec::new(),
            critical_error: None,
          });
        }
        rolled_back
      }
      RestoreTransactionResult::FailedBeforePublication(error) => {
        warn!(
          "Restore failed before publication; original data is unchanged: {error:#}"
        );
        let cleanup_error =
          if let Some(journal) = owned_volume_journal.as_deref() {
            cleanup_owned_restore_volume_journal(journal).await.err()
          } else {
            None
          };
        let (restarted, restart_errors) =
          restart_quiesced_containers(
            container_journal.as_deref(),
            &stopped_containers,
          )
          .await?;
        return Ok(TransactionalVykarRestoreResponse {
          complete: false,
          rolled_back: cleanup_error.is_none(),
          finalization_pending: false,
          containers_restarted: if restart_errors.is_empty()
            && cleanup_error.is_none()
          {
            restarted
          } else {
            Vec::new()
          },
          critical_error: if cleanup_error.is_some()
            || !restart_errors.is_empty()
          {
            Some(format!(
              "Restore failed before publication ({error:#}); created Volume cleanup: {}; container restart: {}",
              cleanup_error
                .map(|error| format!("failed: {error:#}"))
                .unwrap_or_else(|| "complete".into()),
              if restart_errors.is_empty() {
                "complete".into()
              } else {
                restart_errors.join("; ")
              }
            ))
          } else {
            None
          },
        });
      }
      RestoreTransactionResult::StagingCleanupFailed(error) => {
        return Ok(TransactionalVykarRestoreResponse {
          complete: false,
          rolled_back: false,
          finalization_pending: false,
          containers_restarted: Vec::new(),
          critical_error: Some(format!(
            "Restore failed before publication; original data is unchanged, but staging recovery is required and affected containers remain stopped: {error:#}"
          )),
        });
      }
      RestoreTransactionResult::Indeterminate(error) => {
        return Ok(TransactionalVykarRestoreResponse {
          complete: false,
          rolled_back: false,
          finalization_pending: false,
          containers_restarted: Vec::new(),
          critical_error: Some(format!(
            "Restore state is indeterminate; affected containers remain stopped: {error:#}"
          )),
        });
      }
    };
    let (restarted, restart_errors) = restart_quiesced_containers(
      container_journal.as_deref(),
      &stopped_containers,
    )
    .await?;
    if restart_errors.is_empty() {
      Ok(TransactionalVykarRestoreResponse {
        complete: !rolled_back,
        rolled_back,
        finalization_pending: false,
        containers_restarted: restarted,
        critical_error: None,
      })
    } else {
      for container in &restarted {
        let _ = run_container_command("stop", container).await;
      }
      Ok(TransactionalVykarRestoreResponse {
        complete: false,
        rolled_back,
        finalization_pending: false,
        containers_restarted: Vec::new(),
        critical_error: Some(format!(
          "Container state is indeterminate; keep affected containers stopped: {}",
          restart_errors.join("; ")
        )),
      })
    }
  }
}

impl Resolve<Args> for PreflightVykarRestore {
  async fn resolve(
    mut self,
    _: &Args,
  ) -> anyhow::Result<PreflightVykarRestoreResponse> {
    let deadline = Instant::now() + RESTORE_PREFLIGHT_TIMEOUT;
    restore_preflight_before_deadline(deadline, async move {
    let permit = preflight_slots().clone().try_acquire_owned()
      .context("Another restore preflight is still running on this Periphery; retry after it finishes")?;
    let discovered = match &self.target {
      PeripheryBackupTarget::Stack { .. } => {
        // A missing Stack destination can legitimately be planned as a
        // recovered Stack; execution recreates its mapped filesystem roots.
        discover_source(
          &self.target,
          &self.protected_repository_paths,
          &unfiltered_source_filters(),
        )
        .await
        .ok()
      }
      PeripheryBackupTarget::Volume { volume_name } => {
        let docker_guard = docker_client().load();
        let docker = docker_guard
          .as_ref()
          .as_ref()
          .context("Docker is unavailable")?;
        let containers = docker.list_containers().await?;
        let exists = docker
          .list_volumes(&containers)
          .await?
          .into_iter()
          .any(|volume| volume.name == *volume_name);
        if exists {
          // Once Docker confirms the Volume exists, an unsupported driver or
          // inspect failure is a real preflight error, not evidence that the
          // destination is absent.
          Some(
            discover_source(
              &self.target,
              &self.protected_repository_paths,
              &unfiltered_source_filters(),
            )
            .await?,
          )
        } else {
          None
        }
      }
    };
    let destination_exists = discovered.is_some();
    let missing_volume = match &self.target {
      PeripheryBackupTarget::Volume { volume_name }
        if !destination_exists =>
      {
        Some(volume_name.clone())
      }
      _ => None,
    };
    if let PeripheryBackupTarget::Volume { volume_name } =
      &self.target
      && let Some(mountpoint) =
        discovered.as_ref().and_then(|source| source.paths.first())
    {
      resolve_volume_publish_destinations(
        &mut self.publish,
        volume_name,
        mountpoint,
        self.selected_paths.is_empty(),
      )?;
    }
    // A new Volume has no inspectable mountpoint. Its preview names paths
    // inside that Volume, never guessed host paths. After creation, execution
    // validates the inspected mountpoint before stopping/publishing anything.
    let running_containers = if missing_volume.is_some() {
      Vec::new()
    } else {
      validate_restore_destinations(
        &self.publish,
        &self.protected_repository_paths,
      )
      .await?;
      discover_running_containers(
        &self.target,
        &self.publish,
        &self.protected_repository_paths,
      )
      .await?
    };
    let repository = self.repository.clone();
    let advanced = self.advanced.clone();
    let hostname = self.hostname.clone();
    let snapshot = self.snapshot_name.clone();
    let selected = self.selected_paths;
    let publish = self.publish;
    let worker = tokio::task::spawn_blocking(move || {
      // Keep the slot inside the worker if a caller disconnects or times out.
      // Slow backend reads must not spawn unlimited abandoned inventories.
      let _permit = permit;
      let cache = vykar_cache_dir(&hostname)?;
      let snapshot_paths = VykarRepository::new(
        &repository,
        &hostname,
        &cache,
        &cache,
        &advanced,
      )?
      .snapshot_paths(&snapshot, &selected, deadline)?;
      let (created, overwritten, deleted) = if let Some(volume_name) = missing_volume {
        compare_missing_volume_paths(
          &snapshot_paths,
          &publish,
          &volume_name,
          deadline,
        )?
      } else {
        compare_restore_paths(
          &snapshot_paths,
          &publish,
          &selected,
          deadline,
        )?
      };
      Ok::<_, anyhow::Error>(bounded_restore_preview(created, overwritten, deleted))
    });
    let mut preview = worker
      .await
      .context("Restore preflight worker failed")??;
    preview.destination_exists = destination_exists;
    preview.containers_to_stop = running_containers;
    Ok(preview)
    }).await
  }
}

async fn restore_execution_before_deadline<T>(
  deadline: Instant,
  work: impl std::future::Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
  if Instant::now() >= deadline {
    return Err(anyhow!(
      "Restore execution preparation exceeded its 60-second deadline"
    ));
  }
  tokio::time::timeout_at(
    tokio::time::Instant::from_std(deadline),
    work,
  )
  .await
  .context(
    "Restore execution preparation exceeded its 60-second deadline",
  )?
}

async fn bounded_restore_execution_read<T: Send + 'static>(
  deadline: Instant,
  work: impl FnOnce() -> anyhow::Result<T> + Send + 'static,
) -> anyhow::Result<T> {
  bounded_restore_execution_read_in(
    preflight_slots().clone(),
    deadline,
    work,
  )
  .await
}

async fn bounded_restore_execution_read_in<T: Send + 'static>(
  slots: Arc<tokio::sync::Semaphore>,
  deadline: Instant,
  work: impl FnOnce() -> anyhow::Result<T> + Send + 'static,
) -> anyhow::Result<T> {
  if Instant::now() >= deadline {
    return Err(anyhow!(
      "Restore execution preparation exceeded its deadline"
    ));
  }
  let permit = slots.try_acquire_owned()
    .context("Another restore inventory is still running; retry after it finishes")?;
  let worker = tokio::task::spawn_blocking(move || {
    let _permit = permit;
    work()
  });
  restore_execution_before_deadline(deadline, async {
    worker
      .await
      .context("Restore execution inventory worker failed")?
  })
  .await
}

async fn verify_quiesced_restore_preview(
  request: &TransactionalVykarRestore,
  original_running: &[String],
) -> anyhow::Result<()> {
  let deadline = Instant::now() + RESTORE_PREFLIGHT_TIMEOUT;
  let request = request.clone();
  let original_running = original_running.to_vec();
  let runtime = tokio::runtime::Handle::current();
  bounded_restore_execution_read(deadline, move || {
    runtime.block_on(restore_execution_before_deadline(
      deadline,
      validate_restore_destinations(
        &request.publish,
        &request.protected_repository_paths,
      ),
    ))?;
    let cache = vykar_cache_dir(&request.hostname)?;
    let paths = VykarRepository::new(
      &request.repository,
      &request.hostname,
      &cache,
      &cache,
      &request.advanced,
    )?
    .snapshot_paths(
      &request.snapshot_name,
      &request.selected_paths,
      deadline,
    )?;
    let mut changes = compare_restore_paths(
      &paths,
      &request.publish,
      &request.selected_paths,
      deadline,
    )?;
    if !request.expected_preview.destination_exists
      && request.create_volume_if_missing
      && let PeripheryBackupTarget::Volume { volume_name } =
        &request.target
    {
      let first = request
        .publish
        .first()
        .context("Restore publish plan is empty")?;
      let root = if request.selected_paths.is_empty() {
        first.destination.as_str()
      } else {
        first.destination_root.as_deref().context(
          "New Volume selection has no inspected destination root",
        )?
      };
      normalize_created_volume_preview(
        &mut changes,
        Path::new(root),
        volume_name,
      )?;
    }
    ensure_quiesced_preview_matches(
      &request.expected_preview,
      changes,
      original_running,
    )
  })
  .await
}

fn ensure_quiesced_preview_matches(
  expected: &PreflightVykarRestoreResponse,
  changes: (Vec<String>, Vec<String>, Vec<String>),
  original_running: Vec<String>,
) -> anyhow::Result<()> {
  let mut preview =
    bounded_restore_preview(changes.0, changes.1, changes.2);
  // Resource creation was explicitly confirmed already. These filesystem
  // classifications detect paths created/removed while apps were stopping.
  preview.destination_exists = expected.destination_exists;
  preview.containers_to_stop = original_running;
  if !expected.matches(&preview) {
    return Err(anyhow!(
      "Restore paths changed while containers were stopping; no files were published; create and review a fresh preview"
    ));
  }
  Ok(())
}

fn normalize_created_volume_preview(
  changes: &mut (Vec<String>, Vec<String>, Vec<String>),
  mountpoint: &Path,
  volume_name: &str,
) -> anyhow::Result<()> {
  // Docker created this owned root after the missing-volume preview. Only
  // that root is reclassified; any newly written children still mismatch.
  if !std::fs::symlink_metadata(mountpoint)?.is_dir() {
    return Err(anyhow!(
      "Created Volume mountpoint is not a directory"
    ));
  }
  let (created, overwritten, deleted) = changes;
  overwritten.retain(|path| {
    if Path::new(path) == mountpoint {
      created.push(path.clone());
      false
    } else {
      true
    }
  });
  for paths in [created, overwritten, deleted] {
    for path in paths {
      let relative =
        Path::new(path).strip_prefix(mountpoint).context(
          "Created Volume preview escaped its inspected mountpoint",
        )?;
      *path = format!(
        "volume://{volume_name}/{}",
        restore_preview_path(relative)?
      );
    }
  }
  Ok(())
}

async fn restore_preflight_before_deadline<T>(
  deadline: Instant,
  preflight: impl std::future::Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
  tokio::time::timeout_at(
    tokio::time::Instant::from_std(deadline),
    preflight,
  )
  .await
  .context("Restore preflight exceeded 60 seconds; no restore changes were started")?
}

fn compare_missing_volume_paths(
  snapshot_paths: &[komodo_backup::SnapshotPath],
  publish: &[RestorePublishPath],
  volume_name: &str,
  deadline: Instant,
) -> anyhow::Result<(Vec<String>, Vec<String>, Vec<String>)> {
  let logical_root = Path::new("/var/lib/docker/volumes")
    .join(volume_name)
    .join("_data");
  let mut budget =
    komodo_backup::RestoreInventoryBudget::new(deadline);
  let mut created = Vec::new();
  for item in snapshot_paths {
    let Some((mapping, relative)) =
      map_snapshot_path(&item.path, publish)?
    else {
      continue;
    };
    let destination = Path::new(&mapping.destination).join(relative);
    let relative = destination.strip_prefix(&logical_root).context(
      "New Volume preview destination is outside its logical root",
    )?;
    if relative
      .components()
      .any(|part| !matches!(part, std::path::Component::Normal(_)))
    {
      return Err(anyhow!(
        "New Volume preview contains an unsafe relative path"
      ));
    }
    let display = format!(
      "volume://{volume_name}/{}",
      restore_preview_path(relative)?
    );
    budget.consume(&display)?;
    created.push(display);
  }
  created.sort();
  created.dedup();
  Ok((created, Vec::new(), Vec::new()))
}

fn compare_restore_paths(
  snapshot_paths: &[komodo_backup::SnapshotPath],
  publish: &[RestorePublishPath],
  selected: &[String],
  deadline: Instant,
) -> anyhow::Result<(Vec<String>, Vec<String>, Vec<String>)> {
  let mut budget =
    komodo_backup::RestoreInventoryBudget::new(deadline);
  let mut expected = HashSet::<PathBuf>::new();
  let mut created = Vec::new();
  let mut overwritten = Vec::new();
  for item in snapshot_paths {
    if Instant::now() >= deadline {
      return Err(anyhow!(
        "Restore preflight exceeded its time limit"
      ));
    }
    let Some((mapping, relative)) =
      map_snapshot_path(&item.path, publish)?
    else {
      continue;
    };
    let destination = if relative.as_os_str().is_empty() {
      PathBuf::from(&mapping.destination)
    } else {
      Path::new(&mapping.destination).join(relative)
    };
    let display = restore_preview_path(&destination)?;
    budget.consume(display)?;
    expected.insert(destination.clone());
    match restore_preview_metadata(
      Path::new(&mapping.destination),
      &destination,
    )? {
      None => {
        created.push(display.to_owned());
      }
      Some(_) => {
        // Publication also replaces directory metadata, even for empty dirs.
        overwritten.push(display.to_owned());
      }
    }
  }

  let restore_roots = if selected.is_empty() {
    publish
      .iter()
      .map(|mapping| PathBuf::from(&mapping.destination))
      .collect::<Vec<_>>()
  } else {
    selected
      .iter()
      .map(|selection| {
        map_snapshot_path(selection.trim_matches('/'), publish)
      })
      .collect::<anyhow::Result<Vec<_>>>()?
      .into_iter()
      .flatten()
      .map(|(mapping, relative)| {
        Path::new(&mapping.destination).join(relative)
      })
      .collect()
  };
  let mut deleted = Vec::new();
  for root in restore_roots {
    collect_unexpected_paths(
      &root,
      &expected,
      &mut deleted,
      &mut budget,
    )?;
  }
  created.sort();
  created.dedup();
  overwritten.sort();
  overwritten.dedup();
  deleted.sort();
  deleted.dedup();
  Ok((created, overwritten, deleted))
}

/// Publication replaces symlinks instead of following them. Descendants of
/// a replaced symlink (or file) are therefore created, regardless of what
/// exists beyond that link in the host filesystem.
fn restore_preview_metadata(
  root: &Path,
  destination: &Path,
) -> anyhow::Result<Option<std::fs::Metadata>> {
  let mut path = root.to_path_buf();
  let mut components = destination.strip_prefix(root)?.components();
  loop {
    let metadata = match std::fs::symlink_metadata(&path) {
      Ok(metadata) => metadata,
      Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
        return Ok(None);
      }
      Err(error) => return Err(error.into()),
    };
    let Some(component) = components.next() else {
      return Ok(Some(metadata));
    };
    if !metadata.is_dir() {
      return Ok(None);
    }
    path.push(component.as_os_str());
  }
}

fn map_snapshot_path<'a>(
  snapshot_path: &str,
  publish: &'a [RestorePublishPath],
) -> anyhow::Result<Option<(&'a RestorePublishPath, PathBuf)>> {
  let path = Path::new(snapshot_path);
  if path.is_absolute()
    || path.components().any(|component| {
      matches!(component, std::path::Component::ParentDir)
    })
  {
    return Err(anyhow!("Unsafe snapshot path in restore preflight"));
  }
  let best = publish
    .iter()
    .filter_map(|mapping| {
      let root = Path::new(mapping.snapshot_path.trim_matches('/'));
      path.strip_prefix(root).ok().map(|relative| {
        (mapping, relative.to_path_buf(), root.components().count())
      })
    })
    .max_by_key(|(_, _, depth)| *depth);
  Ok(best.map(|(mapping, relative, _)| (mapping, relative)))
}

fn restore_preview_path(path: &Path) -> anyhow::Result<&str> {
  path.to_str().context(
    "Restore preflight requires lossless UTF-8 filenames; no restore changes were started",
  )
}

fn collect_unexpected_paths(
  root: &Path,
  expected: &HashSet<PathBuf>,
  deleted: &mut Vec<String>,
  budget: &mut komodo_backup::RestoreInventoryBudget,
) -> anyhow::Result<()> {
  let display = restore_preview_path(root)?;
  budget.consume(display)?;
  let metadata = match std::fs::symlink_metadata(root) {
    Ok(metadata) => metadata,
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
      return Ok(());
    }
    Err(error) => return Err(error.into()),
  };
  if !expected.contains(root) {
    deleted.push(display.to_owned());
  }
  if !metadata.is_dir() {
    return Ok(());
  }
  // A bounded iterator stack avoids recursive stack overflow and does not
  // collect a wide directory's entire contents before checking the budget.
  let mut directories = vec![std::fs::read_dir(root)?];
  while let Some(directory) = directories.last_mut() {
    let Some(entry) = directory.next() else {
      directories.pop();
      continue;
    };
    let entry = entry?;
    let path = entry.path();
    let display = restore_preview_path(&path)?;
    budget.consume(display)?;
    if !expected.contains(&path) {
      deleted.push(display.to_owned());
    }
    if entry.file_type()?.is_dir() {
      if directories.len() >= MAX_RESTORE_PREVIEW_DEPTH {
        return Err(anyhow!(
          "Restore preflight destination exceeds 128 directory levels"
        ));
      }
      directories.push(std::fs::read_dir(path)?);
    }
  }
  Ok(())
}

enum RestoreTransactionResult {
  Published {
    rolled_back: bool,
    finalization_pending: bool,
  },
  FailedBeforePublication(anyhow::Error),
  StagingCleanupFailed(anyhow::Error),
  Indeterminate(anyhow::Error),
}

fn finish_restore_before_publication(
  error: Option<anyhow::Error>,
  cleanup: anyhow::Result<()>,
) -> RestoreTransactionResult {
  if let Err(cleanup) = cleanup {
    let reason = error
      .map(|error| format!("Restore failed: {error:#}"))
      .unwrap_or_else(|| "Restore was cancelled".into());
    return RestoreTransactionResult::StagingCleanupFailed(
      cleanup
        .context(format!("{reason}; restore staging cleanup failed")),
    );
  }
  match error {
    Some(error) => {
      RestoreTransactionResult::FailedBeforePublication(error)
    }
    None => RestoreTransactionResult::Published {
      rolled_back: true,
      finalization_pending: false,
    },
  }
}

async fn transactional_restore(
  request: &TransactionalVykarRestore,
) -> RestoreTransactionResult {
  if request.publish.is_empty() {
    return RestoreTransactionResult::FailedBeforePublication(
      anyhow!("Restore publish plan is empty"),
    );
  }
  if operation_cancelled(&request.journal_id) {
    return RestoreTransactionResult::Published {
      rolled_back: true,
      finalization_pending: false,
    };
  }
  // Protection and the complete change set were revalidated under the owned
  // barrier after quiescing, with a bounded execution inventory.
  let first_destination =
    PathBuf::from(&request.publish[0].destination);
  let Some(parent) = first_destination.parent() else {
    return RestoreTransactionResult::FailedBeforePublication(
      anyhow!("Restore destination has no parent"),
    );
  };
  let parent = parent.to_path_buf();
  let staging =
    parent.join(format!(".komodo-restore-{}", request.journal_id));
  if path_lexists(&staging) {
    return RestoreTransactionResult::FailedBeforePublication(
      anyhow!("Restore staging path already exists"),
    );
  }
  let staging_journal = match persist_restore_staging_journal(
    &request.journal_id,
    std::slice::from_ref(&staging),
  ) {
    Ok(path) => path,
    Err(error) => {
      return RestoreTransactionResult::FailedBeforePublication(
        error,
      );
    }
  };

  let repository = request.repository.clone();
  let advanced = request.advanced.clone();
  let hostname = request.hostname.clone();
  let snapshot = request.snapshot_name.clone();
  let selected = request.selected_paths.clone();
  let restore_staging = staging.clone();
  let restore_result = tokio::task::spawn_blocking(move || {
    let cache = vykar_cache_dir(&hostname)?;
    let repository = VykarRepository::new(
      &repository,
      &hostname,
      &cache,
      &cache,
      &advanced,
    )?;
    repository.restore(&snapshot, &restore_staging, &selected)
  })
  .await;
  match restore_result {
    Ok(Ok(())) => {}
    Ok(Err(error)) => {
      return finish_restore_before_publication(
        Some(error),
        cleanup_restore_staging_journal(&staging_journal),
      );
    }
    Err(error) => {
      return finish_restore_before_publication(
        Some(
          anyhow::Error::new(error)
            .context("Vykar restore worker failed"),
        ),
        cleanup_restore_staging_journal(&staging_journal),
      );
    }
  }

  if let Err(error) =
    rewrite_recovered_stack_compose_files(request, &staging)
  {
    return finish_restore_before_publication(
      Some(error),
      cleanup_restore_staging_journal(&staging_journal),
    );
  }

  if operation_cancelled(&request.journal_id) {
    return finish_restore_before_publication(
      None,
      cleanup_restore_staging_journal(&staging_journal),
    );
  }

  let publish = request.publish.clone();
  let journal_id = request.journal_id.clone();
  let publication_started = Arc::new(AtomicBool::new(false));
  let worker_started = publication_started.clone();
  let publish_staging = staging.clone();
  let publication_staging_journal = staging_journal.clone();
  let defer_finalize = request.defer_finalize;
  let result = tokio::task::spawn_blocking(move || {
    publish_restore(
      &publish_staging,
      &publish,
      &journal_id,
      &worker_started,
      Some(&publication_staging_journal),
      defer_finalize,
    )
  })
  .await;
  match result {
    Ok(Ok(rolled_back)) => RestoreTransactionResult::Published {
      rolled_back,
      finalization_pending: request.defer_finalize && !rolled_back,
    },
    Ok(Err(error)) => {
      if publication_started.load(Ordering::SeqCst) {
        RestoreTransactionResult::Indeterminate(error)
      } else {
        finish_restore_before_publication(
          Some(error),
          cleanup_restore_staging_journal(&staging_journal),
        )
      }
    }
    Err(error) => {
      let error = anyhow::Error::new(error)
        .context("Restore publish worker failed");
      if publication_started.load(Ordering::SeqCst) {
        RestoreTransactionResult::Indeterminate(error)
      } else {
        finish_restore_before_publication(
          Some(error),
          cleanup_restore_staging_journal(&staging_journal),
        )
      }
    }
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RestoreJournalEntry {
  source: PathBuf,
  destination: PathBuf,
  rollback: PathBuf,
  /// `None` denotes a legacy journal whose original-destination state is
  /// ambiguous and must be recovered conservatively.
  #[serde(default)]
  original_existed: Option<bool>,
  published: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RestoreJournal {
  staging: PathBuf,
  entries: Vec<RestoreJournalEntry>,
  #[serde(default)]
  committed: bool,
  /// Filesystem commit/rollback completed, but the journal remains durable
  /// until every quiesced container has restarted. This makes finalization
  /// idempotent across transient Docker failures.
  #[serde(default)]
  finalized: bool,
  /// Core must decide deferred recovered-Stack publications. Periphery never
  /// rolls an undecided deferred journal back during startup.
  #[serde(default)]
  deferred: bool,
  /// Filesystem finalization and container recovery both completed. Deferred
  /// journals retain this receipt until Core acknowledges its durable state.
  #[serde(default)]
  completed: bool,
  /// A Volume created specifically for this restore. The same durable
  /// journal owns both filesystem rollback and removal of the side effect
  /// until publication is committed.
  #[serde(default)]
  owned_volume: Option<RestoreOwnedVolume>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RestoreOwnedVolume {
  volume_name: String,
  restore_plan_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RestoreStagingJournal {
  paths: Vec<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ContainerQuiesceJournal {
  containers: Vec<String>,
}

#[derive(Default)]
struct RemovePathsOnDrop(Vec<PathBuf>);

impl Drop for RemovePathsOnDrop {
  fn drop(&mut self) {
    for path in self.0.iter().rev() {
      let _ = remove_path(path);
    }
  }
}

fn restore_journal_dir() -> anyhow::Result<PathBuf> {
  let directory = periphery_config()
    .stack_dir()
    .join(".komodo-vykar")
    .join("restore-journals");
  std::fs::create_dir_all(&directory)?;
  Ok(directory)
}

/// Check durable journals under the operation lock before any new side effect.
/// Even a completed deferred receipt remains owned until Core acknowledges it.
/// Finalize and startup recovery deliberately bypass this gate to repair state.
pub(crate) fn ensure_no_pending_recovery() -> anyhow::Result<()> {
  crate::stack::delete::ensure_no_pending_deletions()?;
  ensure_recovery_directories_empty(&[
    restore_journal_dir()?,
    container_quiesce_journal_dir()?,
    restore_staging_journal_dir()?,
  ])
}

fn ensure_recovery_directories_empty(
  directories: &[PathBuf],
) -> anyhow::Result<()> {
  for directory in directories {
    for entry in std::fs::read_dir(directory)? {
      let path = entry?.path();
      if path.extension().and_then(|value| value.to_str())
        == Some("json")
      {
        return Err(anyhow!(
          "Backup/restore blocked by unresolved recovery journal '{}'; finish Core reconciliation or restart Periphery to recover it before retrying",
          path.display()
        ));
      }
    }
  }
  Ok(())
}

/// Protect the node's filesystem for real worker/process lifetimes, including
/// terminals and background mutations that outlive their request connections.
pub(crate) fn filesystem_barrier()
-> &'static Arc<tokio::sync::RwLock<()>> {
  static BARRIER: OnceLock<Arc<tokio::sync::RwLock<()>>> =
    OnceLock::new();
  BARRIER.get_or_init(|| Arc::new(tokio::sync::RwLock::new(())))
}

/// Mutating jobs and recovery actions retain this lease for their real
/// filesystem lifetime, independently of the request that launched them.
pub(crate) fn filesystem_mutation_guard()
-> anyhow::Result<tokio::sync::OwnedRwLockReadGuard<()>> {
  filesystem_barrier().clone().try_read_owned().context(
    "Filesystem operation blocked by a backup or restore on this Server; retry after it finishes",
  )
}

fn protected_filesystem_guard()
-> anyhow::Result<tokio::sync::OwnedRwLockWriteGuard<()>> {
  filesystem_barrier().clone().try_write_owned().context(
    "Backup/restore blocked by a running terminal, file operation, or Stack deletion on this Server; finish the operation or exit/delete its terminals, then retry",
  )
}

fn restore_journal_path(journal_id: &str) -> anyhow::Result<PathBuf> {
  Ok(restore_journal_dir()?.join(format!("{journal_id}.json")))
}

fn validate_restore_journal_id(
  journal_id: &str,
) -> anyhow::Result<()> {
  if journal_id.is_empty()
    || !journal_id.bytes().all(|byte| {
      byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')
    })
  {
    return Err(anyhow!("Restore journal identity is invalid"));
  }
  Ok(())
}

fn recovered_dispatch_completion(
  receipt: &BackupCompletionReceipt,
  journal: Option<&RestoreJournal>,
  pending: bool,
  original: Option<&BackupCompletionReceipt>,
) -> VykarBackupCompletion {
  let recovery_required = |reason: &str| {
    let mut completion = receipt.completion.clone();
    completion.state = VykarBackupCompletionState::RecoveryRequired;
    completion.error = Some(reason.into());
    completion
  };
  match receipt.kind() {
    Some(BackupDispatchKind::Restore { deferred, .. }) => {
      if pending {
        if receipt.completion.state == VykarBackupCompletionState::Prepared {
          return receipt.completion.clone();
        }
        return recovery_required("Restore worker restarted with an unresolved publication; guarded Core reconciliation is required");
      }
      if let Some(journal) = journal.filter(|journal| journal.finalized && journal.completed) {
        return VykarBackupCompletion {
          state: VykarBackupCompletionState::Complete,
          restore_result: Some(TransactionalVykarRestoreResponse {
            complete: journal.committed, rolled_back: !journal.committed,
            ..Default::default()
          }), ..Default::default()
        };
      }
      if deferred && matches!(receipt.completion.state, VykarBackupCompletionState::Prepared | VykarBackupCompletionState::RecoveryRequired) {
        return recovery_required("Prepared restore journal disappeared without a durable finalization outcome");
      }
      VykarBackupCompletion {
        state: VykarBackupCompletionState::Complete,
        error: Some("Restore worker restarted; journal and container recovery completed, but the interrupted restore outcome is not known".into()),
        ..Default::default()
      }
    }
    Some(BackupDispatchKind::FinalizeRestore { journal_id, commit, .. }) => {
      if pending { return recovery_required("Restore finalization requires guarded journal reconciliation"); }
      let finalized = receipt.finalized.clone().filter(|result| result.complete && result.critical_error.is_none() && result.rolled_back != commit).or_else(|| {
        journal.filter(|journal| journal.finalized && journal.completed && journal.committed == commit).map(|_| FinalizeVykarRestoreResponse {
          complete: true, rolled_back: !commit, ..Default::default()
        })
      }).or_else(|| {
        original.filter(|original| original.core == receipt.core && original.run_id == receipt.run_id
          && match original.kind() {
            Some(BackupDispatchKind::Restore { journal_id: original_id, .. }) => original_id == journal_id,
            None => true,
            _ => false,
          })
          .and_then(|original| finalization_from_origin(Some(original), commit).ok())
      });
      match finalized {
        Some(finalized) => VykarBackupCompletion {
          state: VykarBackupCompletionState::Complete,
          finalize_restore_result: Some(finalized), ..Default::default()
        },
        None => recovery_required("Missing restore journal has no durable matching finalization proof"),
      }
    }
    _ => VykarBackupCompletion {
      state: VykarBackupCompletionState::Complete,
      error: Some("Backup worker restarted; operation interrupted and container recovery completed".into()),
      ..Default::default()
    },
  }
}

fn read_restore_journal(
  path: &Path,
) -> anyhow::Result<Option<RestoreJournal>> {
  match std::fs::read(path) {
    Ok(bytes) => serde_json::from_slice(&bytes)
      .map(Some)
      .context("Invalid restore journal"),
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
      Ok(None)
    }
    Err(error) => Err(error.into()),
  }
}

fn restore_has_pending_journals(
  journal_id: &str,
) -> anyhow::Result<bool> {
  if read_restore_journal(&restore_journal_path(journal_id)?)?
    .is_some_and(|journal| !journal.finalized || !journal.completed)
  {
    return Ok(true);
  }
  for directory in [
    restore_staging_journal_dir()?,
    container_quiesce_journal_dir()?,
  ] {
    match std::fs::symlink_metadata(
      directory.join(format!("{journal_id}.json")),
    ) {
      Ok(_) => return Ok(true),
      Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
      Err(error) => return Err(error.into()),
    }
  }
  Ok(false)
}

fn persist_restore_volume_journal(
  journal_id: &str,
  volume_name: &str,
  restore_plan_id: &str,
) -> anyhow::Result<PathBuf> {
  let path = restore_journal_path(journal_id)?;
  if path_lexists(&path) {
    return Err(anyhow!(
      "A restore journal already exists for operation '{journal_id}'"
    ));
  }
  persist_journal(
    &path,
    &RestoreJournal {
      staging: PathBuf::new(),
      entries: Vec::new(),
      committed: false,
      finalized: false,
      deferred: false,
      completed: false,
      owned_volume: Some(RestoreOwnedVolume {
        volume_name: volume_name.to_string(),
        restore_plan_id: restore_plan_id.to_string(),
      }),
    },
  )?;
  Ok(path)
}

async fn remove_owned_restore_volume(
  owned: &RestoreOwnedVolume,
) -> anyhow::Result<()> {
  let docker_guard = docker_client().load();
  let docker = docker_guard
    .as_ref()
    .as_ref()
    .context("Docker is unavailable")?;
  let deadline = Instant::now() + RESTORE_PREFLIGHT_TIMEOUT;
  let volume = restore_execution_before_deadline(deadline, async {
    let containers = docker.list_containers().await?;
    let exists = docker
      .list_volumes(&containers)
      .await?
      .into_iter()
      .any(|volume| volume.name == owned.volume_name);
    if !exists {
      return Ok(None);
    }
    Ok(Some(docker.inspect_volume(&owned.volume_name).await?))
  })
  .await?;
  let Some(volume) = volume else {
    return Ok(());
  };
  if volume
    .labels
    .get(RESTORE_PLAN_VOLUME_LABEL)
    .map(String::as_str)
    != Some(owned.restore_plan_id.as_str())
  {
    warn!(
      "Restore journal no longer owns Volume '{}'; leaving it untouched",
      owned.volume_name
    );
    return Ok(());
  }
  remove_restore_volume(&owned.volume_name).await
}

async fn cleanup_owned_restore_volume_journal(
  path: &Path,
) -> anyhow::Result<()> {
  let bytes = std::fs::read(path).with_context(|| {
    format!("Failed to read restore journal {}", path.display())
  })?;
  let journal: RestoreJournal = serde_json::from_slice(&bytes)
    .with_context(|| {
      format!("Failed to decode restore journal {}", path.display())
    })?;
  if let Some(owned) = &journal.owned_volume {
    cleanup_volume_staging_journal(path)?;
    remove_owned_restore_volume(owned).await?;
  }
  remove_path(path)?;
  fsync_parent(path)
}

fn restore_staging_journal_dir() -> anyhow::Result<PathBuf> {
  let directory = periphery_config()
    .stack_dir()
    .join(".komodo-vykar")
    .join("restore-staging-journals");
  std::fs::create_dir_all(&directory)?;
  Ok(directory)
}

fn persist_restore_staging_journal(
  journal_id: &str,
  paths: &[PathBuf],
) -> anyhow::Result<PathBuf> {
  let path =
    restore_staging_journal_dir()?.join(format!("{journal_id}.json"));
  persist_journal(
    &path,
    &RestoreStagingJournal {
      paths: paths.to_vec(),
    },
  )?;
  Ok(path)
}

fn cleanup_restore_staging_journal(
  path: &Path,
) -> anyhow::Result<()> {
  let bytes = std::fs::read(path).with_context(|| {
    format!(
      "Failed to read restore staging journal {}",
      path.display()
    )
  })?;
  let journal: RestoreStagingJournal = serde_json::from_slice(&bytes)
    .with_context(|| {
      format!(
        "Failed to decode restore staging journal {}",
        path.display()
      )
    })?;
  for owned in journal.paths.iter().rev() {
    remove_owned_staging_path(owned)?;
  }
  remove_path(path)?;
  fsync_parent(path)
}

/// A created Docker volume owns the parent of its restore staging directory.
/// Reconcile that child journal before Docker can delete the parent.
fn cleanup_volume_staging_journal(
  volume_journal: &Path,
) -> anyhow::Result<()> {
  let path = restore_staging_journal_dir()?.join(
    volume_journal
      .file_name()
      .context("Restore volume journal has no file name")?,
  );
  match std::fs::symlink_metadata(&path) {
    Ok(_) => cleanup_restore_staging_journal(&path),
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
      Ok(())
    }
    Err(error) => Err(error.into()),
  }
}

fn remove_owned_staging_path(path: &Path) -> anyhow::Result<()> {
  match std::fs::symlink_metadata(path) {
    Ok(metadata) if metadata.is_dir() => {
      std::fs::remove_dir_all(path)?
    }
    Ok(_) => std::fs::remove_file(path)?,
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
    Err(error) => return Err(error.into()),
  }
  // Missing staging is idempotent only while its parent is available. A
  // missing parent could instead be unavailable storage; do not erase the
  // journal's ownership evidence or recreate a deleted Docker volume root.
  fsync_parent(path)
}

fn container_quiesce_journal_dir() -> anyhow::Result<PathBuf> {
  let directory = periphery_config()
    .stack_dir()
    .join(".komodo-vykar")
    .join("container-quiesce-journals");
  std::fs::create_dir_all(&directory)?;
  Ok(directory)
}

fn persist_container_quiesce_journal(
  journal_id: &str,
  containers: &[String],
) -> anyhow::Result<Option<PathBuf>> {
  let path = container_quiesce_journal_dir()?
    .join(format!("{journal_id}.json"));
  let existing = if path_lexists(&path) {
    read_container_quiesce_journal(&path)?.containers
  } else {
    Vec::new()
  };
  let containers =
    merge_container_quiesce_sets(&existing, containers);
  if containers.is_empty() {
    return Ok(None);
  }
  persist_journal(&path, &ContainerQuiesceJournal { containers })?;
  Ok(Some(path))
}

fn merge_container_quiesce_sets(
  existing: &[String],
  current: &[String],
) -> Vec<String> {
  existing
    .iter()
    .chain(current)
    .cloned()
    .collect::<BTreeSet<_>>()
    .into_iter()
    .collect()
}

fn read_container_quiesce_journal(
  path: &Path,
) -> anyhow::Result<ContainerQuiesceJournal> {
  serde_json::from_slice(&std::fs::read(path).with_context(|| {
    format!(
      "Failed to read container quiesce journal {}",
      path.display()
    )
  })?)
  .with_context(|| {
    format!(
      "Failed to decode container quiesce journal {}",
      path.display()
    )
  })
}

fn remove_container_quiesce_journal(
  path: Option<&Path>,
) -> anyhow::Result<()> {
  let Some(path) = path else {
    return Ok(());
  };
  remove_path(path)?;
  fsync_parent(path)
}

async fn restart_container_quiesce_journal(
  path: &Path,
) -> anyhow::Result<(Vec<String>, Vec<String>)> {
  if !path_lexists(path) {
    return Ok(Default::default());
  }
  let journal = read_container_quiesce_journal(path)?;
  let result = restart_containers(&journal.containers).await;
  if result.1.is_empty() {
    remove_container_quiesce_journal(Some(path))?;
  }
  Ok(result)
}

async fn restart_quiesced_containers(
  journal: Option<&Path>,
  stopped_this_attempt: &[String],
) -> anyhow::Result<(Vec<String>, Vec<String>)> {
  if let Some(journal) = journal {
    restart_container_quiesce_journal(journal).await
  } else {
    Ok(restart_containers(stopped_this_attempt).await)
  }
}

/// Recover publications with a durable decision, then restart containers
/// quiesced by an interrupted backup or restore. Undecided deferred
/// recovered-Stack publications remain intact for Core reconciliation. This
/// runs before Periphery accepts requests.
pub(crate) async fn recover_restore_journals() -> anyhow::Result<()> {
  let manifest_staging = backup_manifest_staging_dir();
  remove_path(&manifest_staging)?;
  std::fs::create_dir_all(&manifest_staging)?;
  let directory = restore_journal_dir()?;
  let mut deferred_journal_ids = HashSet::new();
  for entry in std::fs::read_dir(&directory)? {
    let path = entry?.path();
    if path.extension().and_then(|value| value.to_str())
      != Some("json")
    {
      continue;
    }
    let bytes = std::fs::read(&path).with_context(|| {
      format!("Failed to read restore journal {}", path.display())
    })?;
    let mut journal: RestoreJournal = serde_json::from_slice(&bytes)
      .with_context(|| {
        format!("Failed to decode restore journal {}", path.display())
      })?;
    let journal_id = path
      .file_stem()
      .and_then(|value| value.to_str())
      .context("Restore journal has an invalid file name")?;
    if journal.deferred {
      deferred_journal_ids.insert(journal_id.to_string());
      // An uncommitted, unfinalized deferred journal belongs to a recovered
      // Stack saga. Only Core can prove whether its resource insert happened,
      // so startup must leave both publication and containers untouched.
      if !journal.committed && !journal.finalized {
        continue;
      }
      if !journal.finalized {
        for entry in &journal.entries {
          remove_path(&entry.rollback)?;
          fsync_parent(&entry.destination)?;
          remove_path(&entry.source)?;
          fsync_parent(&entry.source)?;
        }
        if !journal.staging.as_os_str().is_empty() {
          remove_path(&journal.staging)?;
          fsync_parent(&journal.staging)?;
        }
        journal.finalized = true;
        persist_journal(&path, &journal)?;
      }
      if !journal.completed {
        let container_path = container_quiesce_journal_dir()?
          .join(format!("{journal_id}.json"));
        let (_, errors) =
          restart_container_quiesce_journal(&container_path).await?;
        if !errors.is_empty() {
          return Err(anyhow!(
            "Failed to recover containers from finalized deferred restore {}: {}",
            path.display(),
            errors.join("; ")
          ));
        }
        journal.completed = true;
        persist_journal(&path, &journal)?;
      }
      // Keep the completed receipt until Core durably records and
      // acknowledges the matching recovered Stack outcome.
      continue;
    }
    if journal.committed {
      for entry in &journal.entries {
        remove_path(&entry.rollback)?;
        fsync_parent(&entry.destination)?;
      }
    } else {
      rollback_published(&mut journal, &path)?;
    }
    for entry in &journal.entries {
      remove_path(&entry.source)?;
      fsync_parent(&entry.source)?;
    }
    if !journal.staging.as_os_str().is_empty() {
      remove_path(&journal.staging)?;
      fsync_parent(&journal.staging)?;
    }
    if !journal.committed
      && let Some(owned) = &journal.owned_volume
    {
      cleanup_volume_staging_journal(&path)?;
      remove_owned_restore_volume(owned).await?;
    }
    remove_path(&path)?;
    fsync_parent(&path)?;
    warn!("Recovered interrupted restore journal {}", path.display());
  }
  let directory = restore_staging_journal_dir()?;
  for entry in std::fs::read_dir(&directory)? {
    let path = entry?.path();
    if path.extension().and_then(|value| value.to_str())
      != Some("json")
    {
      continue;
    }
    cleanup_restore_staging_journal(&path)?;
    warn!(
      "Removed staging from interrupted restore journal {}",
      path.display()
    );
  }
  let directory = container_quiesce_journal_dir()?;
  for entry in std::fs::read_dir(&directory)? {
    let path = entry?.path();
    if path.extension().and_then(|value| value.to_str())
      != Some("json")
    {
      continue;
    }
    if path
      .file_stem()
      .and_then(|value| value.to_str())
      .is_some_and(|id| deferred_journal_ids.contains(id))
    {
      continue;
    }
    let (_, errors) =
      restart_container_quiesce_journal(&path).await?;
    if !errors.is_empty() {
      return Err(anyhow!(
        "Failed to recover containers from interrupted backup/restore {}: {}",
        path.display(),
        errors.join("; ")
      ));
    }
    warn!(
      "Restarted containers from interrupted backup/restore journal {}",
      path.display()
    );
  }
  // Only process death plus successful journal/container reconciliation proves
  // that a formerly running backup no longer owns mutation or quiesce work.
  recover_backup_completions_in(&backup_completion_dir()?)
}

fn publish_restore(
  staging: &Path,
  publish: &[RestorePublishPath],
  journal_id: &str,
  publication_started: &AtomicBool,
  staging_journal_path: Option<&Path>,
  defer_finalize: bool,
) -> anyhow::Result<bool> {
  let journal_directory = restore_journal_dir()?;
  publish_restore_in(
    staging,
    publish,
    journal_id,
    publication_started,
    &journal_directory,
    staging_journal_path,
    defer_finalize,
  )
}

fn restore_rollback_path(
  destination: &Path,
  journal_id: &str,
) -> anyhow::Result<PathBuf> {
  let parent = destination
    .parent()
    .context("Restore destination has no parent")?;
  let mut name = destination
    .file_name()
    .context("Restore destination has no file name")?
    .to_os_string();
  name.push(format!(".komodo-rollback-{journal_id}"));
  Ok(parent.join(name))
}

fn validate_restore_rollback_paths(
  publish: &[RestorePublishPath],
  journal_id: &str,
) -> anyhow::Result<()> {
  let mut rollback_paths = Vec::<PathBuf>::new();
  for item in publish {
    let rollback = restore_rollback_path(
      Path::new(&item.destination),
      journal_id,
    )?;
    for other in &rollback_paths {
      if komodo_backup::filesystem::entry_paths_overlap(
        &rollback, other,
      )? {
        return Err(anyhow!(
          "Restore destinations produce overlapping rollback entries"
        ));
      }
    }
    for destination in publish {
      if komodo_backup::filesystem::entry_paths_overlap(
        &rollback,
        Path::new(&destination.destination),
      )? {
        return Err(anyhow!(
          "Restore rollback entry overlaps a publication destination"
        ));
      }
    }
    rollback_paths.push(rollback);
  }
  Ok(())
}

fn publish_restore_in(
  staging: &Path,
  publish: &[RestorePublishPath],
  journal_id: &str,
  publication_started: &AtomicBool,
  journal_directory: &Path,
  staging_journal_path: Option<&Path>,
  defer_finalize: bool,
) -> anyhow::Result<bool> {
  validate_resolved_restore_destinations(publish)?;
  validate_restore_rollback_paths(publish, journal_id)?;
  let mut entries = Vec::new();
  let mut preparation_cleanup = RemovePathsOnDrop::default();
  let mut staging_ownership = RestoreStagingJournal {
    paths: vec![staging.to_path_buf()],
  };
  for (index, item) in publish.iter().enumerate() {
    let relative = Path::new(&item.snapshot_path);
    if relative.is_absolute()
      || relative.components().any(|component| {
        matches!(component, std::path::Component::ParentDir)
      })
    {
      return Err(anyhow!("Unsafe snapshot publish path"));
    }
    let destination = PathBuf::from(&item.destination);
    if !destination.is_absolute() {
      return Err(anyhow!("Restore destination must be absolute"));
    }
    let destination_parent = destination
      .parent()
      .context("Restore destination has no parent")?;
    let original_existed = path_lexists(&destination);
    let rollback = restore_rollback_path(&destination, journal_id)?;
    if path_lexists(&rollback) {
      return Err(anyhow!(
        "Rollback path already exists: {}",
        rollback.display()
      ));
    }
    let restored_source = staging.join(relative);
    if !path_lexists(&restored_source) {
      return Err(anyhow!(
        "Restored snapshot path is missing: {}",
        item.snapshot_path
      ));
    }
    std::fs::create_dir_all(destination_parent)?;
    let source = destination_parent
      .join(format!(".komodo-restore-{journal_id}-{index}"));
    if path_lexists(&source) {
      return Err(anyhow!(
        "Same-filesystem restore staging path already exists: {}",
        source.display()
      ));
    }
    if let Some(staging_journal_path) = staging_journal_path {
      staging_ownership.paths.push(source.clone());
      persist_journal(staging_journal_path, &staging_ownership)?;
    }
    preparation_cleanup.0.push(source.clone());
    let copy = std::process::Command::new("cp")
      .arg("-a")
      .arg("--")
      .arg(&restored_source)
      .arg(&source)
      .output()
      .context("Failed to start metadata-preserving restore copy")?;
    if !copy.status.success() {
      return Err(anyhow!(
        "Failed to stage restore on destination filesystem: {}",
        String::from_utf8_lossy(&copy.stderr)
      ));
    }
    if tree_digest(&restored_source)? != tree_digest(&source)? {
      return Err(anyhow!(
        "Same-filesystem restore staging verification failed"
      ));
    }
    sync_tree(&source)?;
    entries.push(RestoreJournalEntry {
      source,
      destination,
      rollback,
      original_existed: Some(original_existed),
      published: false,
    });
  }

  if entries
    .iter()
    .any(|entry| !destination_existence_matches(entry))
  {
    return Err(anyhow!(
      "Restore destination existence changed during publication preparation"
    ));
  }

  std::fs::create_dir_all(journal_directory)?;
  let journal_path =
    journal_directory.join(format!("{journal_id}.json"));
  let owned_volume = if path_lexists(&journal_path) {
    let existing: RestoreJournal =
      serde_json::from_slice(&std::fs::read(&journal_path)?)
        .with_context(|| {
          format!(
            "Failed to decode pre-publication restore journal {}",
            journal_path.display()
          )
        })?;
    if existing.committed
      || !existing.entries.is_empty()
      || existing.owned_volume.is_none()
    {
      return Err(anyhow!(
        "Restore journal already contains publication state"
      ));
    }
    existing.owned_volume
  } else {
    None
  };
  let mut journal = RestoreJournal {
    staging: staging.to_path_buf(),
    entries,
    committed: false,
    finalized: false,
    deferred: defer_finalize,
    completed: false,
    owned_volume,
  };
  persist_journal(&journal_path, &journal)?;
  // The durable journal owns cleanup from this point onward.
  preparation_cleanup.0.clear();
  publication_started.store(true, Ordering::SeqCst);
  if let Some(staging_journal_path) = staging_journal_path {
    remove_path(staging_journal_path)?;
    fsync_parent(staging_journal_path)?;
  }

  if publish_restore_entries(
    &mut journal,
    &journal_path,
    |from, to| std::fs::rename(from, to),
  )? {
    return Ok(true);
  }

  if defer_finalize {
    // Core creates the recovered Stack only after publication. Preserve the
    // uncommitted durable journal and rollback trees until it explicitly
    // confirms that the database insert succeeded.
    return Ok(false);
  }

  journal.committed = true;
  persist_journal(&journal_path, &journal)?;
  for entry in &journal.entries {
    if path_lexists(&entry.rollback) {
      remove_path(&entry.rollback)?;
    }
    fsync_parent(&entry.destination)?;
  }
  remove_path(staging)?;
  fsync_parent(staging)?;
  std::fs::remove_file(&journal_path)?;
  fsync_parent(&journal_path)?;
  Ok(false)
}

fn publish_restore_entries(
  journal: &mut RestoreJournal,
  journal_path: &Path,
  rename: impl Fn(&Path, &Path) -> std::io::Result<()>,
) -> anyhow::Result<bool> {
  for index in 0..journal.entries.len() {
    if !destination_existence_matches(&journal.entries[index]) {
      rollback_published(journal, journal_path)?;
      cleanup_rolled_back_restore(journal, journal_path)?;
      return Ok(true);
    }
    if path_lexists(&journal.entries[index].destination) {
      if let Err(error) = rename(
        &journal.entries[index].destination,
        &journal.entries[index].rollback,
      ) {
        rollback_published(journal, journal_path)?;
        warn!(
          "Restore rollback preparation failed and earlier publications were rolled back: {error:#}"
        );
        cleanup_rolled_back_restore(journal, journal_path)?;
        return Ok(true);
      }
      // Make destination -> rollback durable before the journal claims this
      // entry was published. Recovery must never remove original data after a
      // power loss that discarded the rename.
      fsync_parent(&journal.entries[index].destination)?;
    }
    // Persist publication intent before source -> destination. On recovery,
    // this distinguishes a newly-created destination (which has no rollback
    // path) from an entry that was never reached.
    journal.entries[index].published = true;
    persist_journal(journal_path, journal)?;
    if let Err(error) = rename(
      &journal.entries[index].source,
      &journal.entries[index].destination,
    ) {
      rollback_published(journal, journal_path)?;
      warn!("Restore publish failed and was rolled back: {error:#}");
      cleanup_rolled_back_restore(journal, journal_path)?;
      return Ok(true);
    }
    fsync_parent(&journal.entries[index].destination)?;
  }

  Ok(false)
}

fn destination_existence_matches(
  entry: &RestoreJournalEntry,
) -> bool {
  entry.original_existed == Some(path_lexists(&entry.destination))
}

fn cleanup_rolled_back_restore(
  journal: &RestoreJournal,
  journal_path: &Path,
) -> anyhow::Result<()> {
  for entry in &journal.entries {
    remove_path(&entry.source)?;
    fsync_parent(&entry.source)?;
  }
  if !journal.staging.as_os_str().is_empty() {
    remove_path(&journal.staging)?;
    fsync_parent(&journal.staging)?;
  }
  if journal.owned_volume.is_some() {
    // Keep only the durable Volume ownership record. The async caller removes
    // that side effect after the synchronous filesystem rollback completes;
    // startup recovery performs the same action after a crash.
    persist_journal(
      journal_path,
      &RestoreJournal {
        staging: PathBuf::new(),
        entries: Vec::new(),
        committed: false,
        finalized: false,
        deferred: journal.deferred,
        completed: false,
        owned_volume: journal.owned_volume.clone(),
      },
    )
  } else {
    remove_path(journal_path)?;
    fsync_parent(journal_path)
  }
}

fn path_lexists(path: &Path) -> bool {
  std::fs::symlink_metadata(path).is_ok()
}

fn remove_path(path: &Path) -> anyhow::Result<()> {
  let Ok(metadata) = std::fs::symlink_metadata(path) else {
    return Ok(());
  };
  if metadata.file_type().is_dir() {
    std::fs::remove_dir_all(path)?;
  } else {
    std::fs::remove_file(path)?;
  }
  Ok(())
}

fn restore_verification_len(is_directory: bool, len: u64) -> u64 {
  // Directory st_size describes filesystem-specific storage, not copied
  // contents. Child entries and their metadata are verified independently.
  if is_directory { 0 } else { len }
}

fn tree_digest(root: &Path) -> anyhow::Result<Vec<u8>> {
  fn update(
    path: &Path,
    relative: &Path,
    digest: &mut Sha256,
  ) -> anyhow::Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    digest.update(relative.to_string_lossy().as_bytes());
    digest.update(metadata.permissions().mode().to_le_bytes());
    digest.update(
      restore_verification_len(metadata.is_dir(), metadata.len())
        .to_le_bytes(),
    );
    digest.update(metadata.uid().to_le_bytes());
    digest.update(metadata.gid().to_le_bytes());
    digest.update(metadata.mtime().to_le_bytes());
    digest.update(metadata.mtime_nsec().to_le_bytes());
    let mut attribute_names = xattr::list(path)?.collect::<Vec<_>>();
    attribute_names.sort();
    for name in attribute_names {
      digest.update(name.as_encoded_bytes());
      if let Some(value) = xattr::get(path, &name)? {
        digest.update(value);
      }
    }
    if metadata.file_type().is_symlink() {
      digest.update(b"symlink");
      digest.update(
        std::fs::read_link(path)?.to_string_lossy().as_bytes(),
      );
    } else if metadata.is_dir() {
      digest.update(b"directory");
      let mut entries =
        std::fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
      entries.sort_by_key(|entry| entry.file_name());
      for entry in entries {
        update(
          &entry.path(),
          &relative.join(entry.file_name()),
          digest,
        )?;
      }
    } else if metadata.is_file() {
      digest.update(b"file");
      let mut file = std::fs::File::open(path)?;
      let mut buffer = [0_u8; 1024 * 128];
      loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
          break;
        }
        digest.update(&buffer[..read]);
      }
    }
    Ok(())
  }
  let mut digest = Sha256::new();
  update(root, Path::new(""), &mut digest)?;
  Ok(digest.finalize().to_vec())
}

fn sync_tree(root: &Path) -> anyhow::Result<()> {
  let metadata = std::fs::symlink_metadata(root)?;
  if metadata.file_type().is_symlink() {
    return Ok(());
  }
  if metadata.is_dir() {
    for entry in std::fs::read_dir(root)? {
      sync_tree(&entry?.path())?;
    }
    std::fs::File::open(root)?.sync_all()?;
  } else if metadata.is_file() {
    std::fs::File::open(root)?.sync_all()?;
  }
  Ok(())
}

fn rollback_published(
  restore: &mut RestoreJournal,
  journal_path: &Path,
) -> anyhow::Result<()> {
  for index in (0..restore.entries.len()).rev() {
    let entry = &restore.entries[index];
    let published = entry.published;
    let rollback = entry.rollback.clone();
    let destination = entry.destination.clone();
    let rollback_exists = path_lexists(&rollback);
    match entry.original_existed {
      Some(true) => {
        // If rollback still exists it is the authoritative original. If it no
        // longer exists, destination is either the untouched original or the
        // already-restored original from a crash after the durable rename.
        if rollback_exists {
          if path_lexists(&destination) {
            remove_path(&destination)?;
          }
          std::fs::rename(&rollback, &destination)?;
          fsync_parent(&destination)?;
        }
      }
      Some(false) => {
        if published && path_lexists(&destination) {
          remove_path(&destination)?;
          fsync_parent(&destination)?;
        }
      }
      None => {
        // A rollback path proves that a legacy entry had an original. Without
        // it, `published = true` is ambiguous, so fail closed rather than risk
        // deleting a restored original.
        if rollback_exists {
          if path_lexists(&destination) {
            remove_path(&destination)?;
          }
          std::fs::rename(&rollback, &destination)?;
          fsync_parent(&destination)?;
        } else if published {
          return Err(anyhow!(
            "Legacy restore journal is ambiguous for destination {}",
            destination.display()
          ));
        }
      }
    }
    restore.entries[index].published = false;
    persist_journal(journal_path, restore)?;
  }
  Ok(())
}

fn persist_journal<T: Serialize>(
  path: &Path,
  journal: &T,
) -> anyhow::Result<()> {
  let temporary = path.with_extension("tmp");
  let bytes = serde_json::to_vec(journal)?;
  let mut file = OpenOptions::new()
    .create(true)
    .truncate(true)
    .write(true)
    .open(&temporary)?;
  file.write_all(&bytes)?;
  file.sync_all()?;
  std::fs::rename(&temporary, path)?;
  fsync_parent(path)
}

fn fsync_parent(path: &Path) -> anyhow::Result<()> {
  let parent = path.parent().context("Path has no parent")?;
  std::fs::File::open(parent)?.sync_all()?;
  Ok(())
}

async fn finalize_restore_publication(
  journal_id: &str,
  commit: bool,
  acknowledge: bool,
) -> anyhow::Result<FinalizeVykarRestoreResponse> {
  let journal_path =
    restore_journal_dir()?.join(format!("{journal_id}.json"));
  let bytes = match std::fs::read(&journal_path) {
    Ok(bytes) => bytes,
    Err(error)
      if acknowledge
        && error.kind() == std::io::ErrorKind::NotFound =>
    {
      return Ok(FinalizeVykarRestoreResponse {
        complete: true,
        rolled_back: !commit,
        ..Default::default()
      });
    }
    Err(error) => {
      return Err(error).with_context(|| {
        format!(
          "Pending restore publication does not exist: {}",
          journal_path.display()
        )
      });
    }
  };
  let mut journal: RestoreJournal = serde_json::from_slice(&bytes)
    .with_context(|| {
      format!(
        "Failed to decode pending restore publication {}",
        journal_path.display()
      )
    })?;
  if journal.finalized {
    if journal.committed != commit {
      return Err(anyhow!(
        "Restore was already finalized with the opposite decision"
      ));
    }
  } else if commit {
    // Make the decision durable before discarding rollback data. Startup
    // recovery will finish a committed cleanup after a power loss.
    journal.committed = true;
    persist_journal(&journal_path, &journal)?;
    for entry in &journal.entries {
      remove_path(&entry.rollback)?;
      fsync_parent(&entry.destination)?;
      remove_path(&entry.source)?;
      fsync_parent(&entry.source)?;
    }
    if !journal.staging.as_os_str().is_empty() {
      remove_path(&journal.staging)?;
      fsync_parent(&journal.staging)?;
    }
    journal.finalized = true;
    persist_journal(&journal_path, &journal)?;
  } else {
    if journal.committed {
      return Err(anyhow!(
        "Restore commit is already durable and cannot be rolled back"
      ));
    }
    rollback_published(&mut journal, &journal_path)?;
    for entry in &journal.entries {
      remove_path(&entry.source)?;
      fsync_parent(&entry.source)?;
    }
    if !journal.staging.as_os_str().is_empty() {
      remove_path(&journal.staging)?;
      fsync_parent(&journal.staging)?;
    }
    if let Some(owned) = &journal.owned_volume {
      remove_owned_restore_volume(owned).await?;
    }
    journal.finalized = true;
    persist_journal(&journal_path, &journal)?;
  }

  if journal.completed {
    if acknowledge {
      remove_path(&journal_path)?;
      fsync_parent(&journal_path)?;
    }
    return Ok(FinalizeVykarRestoreResponse {
      complete: true,
      rolled_back: !commit,
      ..Default::default()
    });
  }

  let container_journal_path = container_quiesce_journal_dir()?
    .join(format!("{journal_id}.json"));
  let (restarted, restart_errors) =
    restart_container_quiesce_journal(&container_journal_path)
      .await?;
  if !restart_errors.is_empty() {
    return Ok(FinalizeVykarRestoreResponse {
      complete: false,
      rolled_back: !commit,
      containers_restarted: Vec::new(),
      critical_error: Some(format!(
        "Restore was finalized but affected containers could not all be restarted: {}",
        restart_errors.join("; ")
      )),
    });
  }
  journal.completed = true;
  persist_journal(&journal_path, &journal)?;
  // Deferred recovered-Stack publications retain a durable receipt until
  // Core records the outcome. Every receipt-backed finalizer persists its
  // proof before acknowledgement makes journal removal idempotent.
  if acknowledge {
    remove_path(&journal_path)?;
    fsync_parent(&journal_path)?;
  }
  Ok(FinalizeVykarRestoreResponse {
    complete: true,
    rolled_back: !commit,
    containers_restarted: restarted,
    critical_error: None,
  })
}

impl Resolve<Args> for FinalizeVykarRestore {
  async fn resolve(
    self,
    args: &Args,
  ) -> anyhow::Result<FinalizeVykarRestoreResponse> {
    let directory = backup_completion_dir()?;
    validate_restore_journal_id(&self.journal_id)?;
    uuid::Uuid::parse_str(&self.restore_operation_id).context(
      "Restore finalization requires a valid original dispatch UUID",
    )?;
    if let Some(completion) = claim_dispatch_completion(
      &directory,
      &self.operation_id,
      &args.core,
      &self.run_id,
      BackupDispatchKind::FinalizeRestore {
        journal_id: self.journal_id.clone(),
        restore_operation_id: self.restore_operation_id.clone(),
        commit: self.commit,
        acknowledge: self.acknowledge,
      },
    )? {
      return completion.finalize_restore_result.ok_or_else(|| {
        anyhow!(completion.error.unwrap_or_else(|| {
          "Restore finalization has no replayable result".into()
        }))
      });
    }
    let args = Args {
      core: args.core.clone(),
      id: args.id,
    };
    tokio::spawn(async move {
      let result = run_finalize_restore(&self, &args, &directory).await;
      let state = if restore_has_pending_journals(&self.journal_id)? {
        VykarBackupCompletionState::RecoveryRequired
      } else {
        VykarBackupCompletionState::Complete
      };
      finish_backup_completion(&directory, &self.operation_id, &args.core, &self.run_id, VykarBackupCompletion {
        state,
        finalize_restore_result: result.as_ref().ok().cloned(),
        error: result.as_ref().err().map(|error| format!("{error:#}")),
        ..Default::default()
      })?;
      result
    }).await.context("Restore finalization task failed; completion remains uncertain")?
  }
}

impl Resolve<Args> for RunFinalizeVykarRestore {
  async fn resolve(
    self,
    args: &Args,
  ) -> anyhow::Result<FinalizeVykarRestoreResponse> {
    self.0.resolve(args).await
  }
}

fn restore_origin_receipt(
  directory: &Path,
  request: &FinalizeVykarRestore,
  core: &str,
) -> anyhow::Result<Option<BackupCompletionReceipt>> {
  if request.restore_operation_id.is_empty() {
    return Err(anyhow!(
      "Restore finalization requires the original tracked dispatch identity; legacy journals need operator reconciliation"
    ));
  }
  let _lock = backup_completion_lock().lock().unwrap();
  let path =
    backup_completion_path(directory, &request.restore_operation_id)?;
  let receipt = read_backup_completion(&path)?.context(
    "Original restore dispatch has not been fenced or claimed",
  )?;
  check_backup_completion_owner(&receipt, core, &request.run_id)?;
  match receipt.kind() {
    Some(BackupDispatchKind::Restore { journal_id, .. })
      if journal_id == request.journal_id => {}
    None
      if receipt.completion.state
        == VykarBackupCompletionState::Complete => {}
    _ => {
      return Err(anyhow!(
        "Finalization does not match the original restore dispatch"
      ));
    }
  }
  if matches!(
    receipt.completion.state,
    VykarBackupCompletionState::Unknown
      | VykarBackupCompletionState::Running
  ) {
    return Err(anyhow!(
      "Original restore dispatch must exit before finalization"
    ));
  }
  Ok(Some(receipt))
}

fn finalization_from_origin(
  origin: Option<&BackupCompletionReceipt>,
  commit: bool,
) -> anyhow::Result<FinalizeVykarRestoreResponse> {
  let origin = origin.context(
    "Missing restore journal has no original dispatch proof",
  )?;
  if origin.completion.state != VykarBackupCompletionState::Complete {
    return Err(anyhow!(
      "Missing restore journal has no durable finalization proof"
    ));
  }
  if let Some(finalized) = &origin.finalized
    && finalized.complete
    && finalized.critical_error.is_none()
    && finalized.rolled_back != commit
  {
    return Ok(finalized.clone());
  }
  if let Some(restored) = &origin.completion.restore_result
    && !restored.finalization_pending
    && restored.critical_error.is_none()
    && ((commit && restored.complete && !restored.rolled_back)
      || (!commit && restored.rolled_back))
  {
    return Ok(FinalizeVykarRestoreResponse {
      complete: true,
      rolled_back: !commit,
      containers_restarted: restored.containers_restarted.clone(),
      critical_error: None,
    });
  }
  if origin.kind().is_none() && !commit {
    return Ok(FinalizeVykarRestoreResponse {
      complete: true,
      rolled_back: true,
      ..Default::default()
    });
  }
  Err(anyhow!(
    "Original restore has no durable matching finalization outcome"
  ))
}

fn persist_finalization_proof(
  directory: &Path,
  request: &FinalizeVykarRestore,
  core: &str,
  finalized: &FinalizeVykarRestoreResponse,
) -> anyhow::Result<()> {
  let _lock = backup_completion_lock().lock().unwrap();
  let path =
    backup_completion_path(directory, &request.operation_id)?;
  let mut receipt = read_backup_completion(&path)?
    .context("Finalization dispatch receipt disappeared")?;
  check_backup_completion_owner(&receipt, core, &request.run_id)?;
  if receipt.completion.state != VykarBackupCompletionState::Running {
    return Err(anyhow!(
      "Finalization dispatch is no longer running"
    ));
  }
  receipt.finalized = Some(finalized.clone());
  persist_journal(&path, &receipt)?;
  if !request.restore_operation_id.is_empty() {
    let path = backup_completion_path(
      directory,
      &request.restore_operation_id,
    )?;
    let mut origin = read_backup_completion(&path)?
      .context("Original restore receipt disappeared")?;
    check_backup_completion_owner(&origin, core, &request.run_id)?;
    origin.finalized = Some(finalized.clone());
    origin.completion = VykarBackupCompletion {
      state: VykarBackupCompletionState::Complete,
      restore_result: Some(TransactionalVykarRestoreResponse {
        complete: !finalized.rolled_back,
        rolled_back: finalized.rolled_back,
        finalization_pending: false,
        containers_restarted: finalized.containers_restarted.clone(),
        critical_error: None,
      }),
      ..Default::default()
    };
    persist_journal(&path, &origin)?;
  }
  Ok(())
}

async fn run_finalize_restore(
  request: &FinalizeVykarRestore,
  args: &Args,
  directory: &Path,
) -> anyhow::Result<FinalizeVykarRestoreResponse> {
  let _operation = backup_operation_lock().lock().await;
  let _filesystem = protected_filesystem_guard()?;
  let origin =
    restore_origin_receipt(directory, request, &args.core)?;
  let journal = read_restore_journal(&restore_journal_path(
    &request.journal_id,
  )?)?;
  // A fenced never-started dispatch cannot authorize another journal's data.
  if journal.is_some()
    && origin
      .as_ref()
      .is_some_and(|receipt| receipt.kind().is_none())
  {
    return Err(anyhow!(
      "Fenced restore dispatch does not own this journal"
    ));
  }
  let finalized = if journal.is_some() {
    finalize_restore_publication(
      &request.journal_id,
      request.commit,
      false,
    )
    .await?
  } else {
    if restore_has_pending_journals(&request.journal_id)? {
      return Err(anyhow!(
        "Missing publication journal still has unreconciled restore work"
      ));
    }
    finalization_from_origin(origin.as_ref(), request.commit)?
  };
  if finalized.complete && finalized.critical_error.is_none() {
    if restore_has_pending_journals(&request.journal_id)? {
      return Err(anyhow!(
        "Finalized publication still has unreconciled child journals"
      ));
    }
    // Persist both proofs before acknowledgement can erase the only journal.
    persist_finalization_proof(
      directory, request, &args.core, &finalized,
    )?;
    if request.acknowledge {
      finalize_restore_publication(
        &request.journal_id,
        request.commit,
        true,
      )
      .await?;
    }
  }
  Ok(finalized)
}

impl Resolve<Args> for CancelVykarOperation {
  async fn resolve(
    self,
    _: &Args,
  ) -> anyhow::Result<CancelVykarOperationResponse> {
    Ok(CancelVykarOperationResponse {
      cancelled: request_operation_cancellation(&self.operation_id),
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn expired_execution_deadline_never_starts_another_preparation_phase()
   {
    let started = AtomicBool::new(false);
    let result =
      restore_execution_before_deadline(Instant::now(), async {
        started.store(true, Ordering::SeqCst);
        Ok(())
      })
      .await;
    assert!(result.is_err());
    assert!(!started.load(Ordering::SeqCst));
    let result = restore_execution_before_deadline(
      Instant::now() + Duration::from_millis(1),
      std::future::pending::<anyhow::Result<()>>(),
    )
    .await;
    assert!(result.is_err());
  }

  #[tokio::test]
  async fn execution_inventory_timeout_retains_the_actual_blocking_slot()
   {
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let (release, waiting) = std::sync::mpsc::channel();
    let result = bounded_restore_execution_read_in(
      slots.clone(),
      Instant::now() + Duration::from_millis(10),
      move || {
        waiting.recv().unwrap();
        Ok(())
      },
    )
    .await;
    assert!(result.is_err());
    assert!(slots.clone().try_acquire_owned().is_err());
    release.send(()).unwrap();
    let permit = slots.acquire_owned().await.unwrap();
    drop(permit);
  }

  #[test]
  fn post_quiesce_preview_detects_new_deletions_and_changed_classifications()
   {
    let root = tempfile::tempdir().unwrap();
    let config = root.path().join("config");
    std::fs::write(&config, b"original").unwrap();
    let snapshot = vec![
      komodo_backup::SnapshotPath {
        path: "snapshot/root".into(),
        directory: true,
      },
      komodo_backup::SnapshotPath {
        path: "snapshot/root/config".into(),
        directory: false,
      },
    ];
    let publish = vec![RestorePublishPath {
      destination_root: None,
      snapshot_path: "snapshot/root".into(),
      destination: root.path().to_string_lossy().into_owned(),
    }];
    let compare = || {
      compare_restore_paths(
        &snapshot,
        &publish,
        &[],
        Instant::now() + RESTORE_PREFLIGHT_TIMEOUT,
      )
      .unwrap()
    };
    let before = compare();
    let mut expected =
      bounded_restore_preview(before.0, before.1, before.2);
    expected.destination_exists = true;
    expected.containers_to_stop = vec!["writer".into()];
    ensure_quiesced_preview_matches(
      &expected,
      compare(),
      vec!["writer".into()],
    )
    .unwrap();
    let extra = root.path().join("created-during-stop");
    std::fs::write(&extra, b"must not silently delete").unwrap();
    assert!(
      ensure_quiesced_preview_matches(
        &expected,
        compare(),
        vec!["writer".into()]
      )
      .is_err()
    );
    assert_eq!(
      std::fs::read(&extra).unwrap(),
      b"must not silently delete"
    );
    std::fs::remove_file(&extra).unwrap();
    std::fs::remove_file(&config).unwrap();
    assert!(
      ensure_quiesced_preview_matches(
        &expected,
        compare(),
        vec!["writer".into()]
      )
      .is_err()
    );
  }

  #[test]
  fn created_volume_post_quiesce_preview_ignores_only_its_owned_root_creation()
   {
    let root = tempfile::tempdir().unwrap();
    let snapshot = vec![
      komodo_backup::SnapshotPath {
        path: "snapshot/root".into(),
        directory: true,
      },
      komodo_backup::SnapshotPath {
        path: "snapshot/root/config".into(),
        directory: false,
      },
    ];
    let logical = vec![RestorePublishPath {
      destination_root: None,
      snapshot_path: "snapshot/root".into(),
      destination: "/var/lib/docker/volumes/new-volume/_data".into(),
    }];
    let before = compare_missing_volume_paths(
      &snapshot,
      &logical,
      "new-volume",
      Instant::now() + RESTORE_PREFLIGHT_TIMEOUT,
    )
    .unwrap();
    let expected =
      bounded_restore_preview(before.0, before.1, before.2);
    let actual = vec![RestorePublishPath {
      destination: root.path().to_string_lossy().into_owned(),
      ..logical[0].clone()
    }];
    let compare = || {
      let mut paths = compare_restore_paths(
        &snapshot,
        &actual,
        &[],
        Instant::now() + RESTORE_PREFLIGHT_TIMEOUT,
      )
      .unwrap();
      normalize_created_volume_preview(
        &mut paths,
        root.path(),
        "new-volume",
      )
      .unwrap();
      paths
    };
    ensure_quiesced_preview_matches(&expected, compare(), Vec::new())
      .unwrap();
    std::fs::write(
      root.path().join("unexpected"),
      b"changed after preview",
    )
    .unwrap();
    assert!(
      ensure_quiesced_preview_matches(
        &expected,
        compare(),
        Vec::new()
      )
      .is_err()
    );
    std::fs::remove_file(root.path().join("unexpected")).unwrap();
    std::fs::write(root.path().join("config"), b"now exists")
      .unwrap();
    assert!(
      ensure_quiesced_preview_matches(
        &expected,
        compare(),
        Vec::new()
      )
      .is_err()
    );
  }

  fn restore_receipt(
    kind: BackupDispatchKind,
    state: VykarBackupCompletionState,
  ) -> BackupCompletionReceipt {
    BackupCompletionReceipt {
      core: "core".into(),
      run_id: "run".into(),
      batch: None,
      kind: Some(kind),
      finalized: None,
      completion: VykarBackupCompletion {
        state,
        ..Default::default()
      },
    }
  }

  fn restore_kind() -> BackupDispatchKind {
    BackupDispatchKind::Restore {
      journal_id: "journal".into(),
      deferred: true,
    }
  }

  fn finalize_request(original: &str) -> FinalizeVykarRestore {
    FinalizeVykarRestore {
      operation_id: uuid::Uuid::new_v4().to_string(),
      run_id: "run".into(),
      restore_operation_id: original.into(),
      journal_id: "journal".into(),
      commit: false,
      acknowledge: true,
    }
  }

  fn finalize_kind(
    request: &FinalizeVykarRestore,
  ) -> BackupDispatchKind {
    BackupDispatchKind::FinalizeRestore {
      journal_id: request.journal_id.clone(),
      restore_operation_id: request.restore_operation_id.clone(),
      commit: request.commit,
      acknowledge: request.acknowledge,
    }
  }

  #[test]
  fn legacy_backup_receipts_keep_their_original_dispatch_kind() {
    let receipt: BackupCompletionReceipt =
      serde_json::from_value(serde_json::json!({
        "core": "core", "run_id": "run", "batch": true,
        "completion": VykarBackupCompletion::default(),
      }))
      .unwrap();
    assert_eq!(receipt.kind(), Some(BackupDispatchKind::BackupBatch));
  }

  #[test]
  fn prepared_restore_replays_without_reexecution_or_acknowledgement()
  {
    let root = tempfile::tempdir().unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    claim_dispatch_completion(
      root.path(),
      &id,
      "core",
      "run",
      restore_kind(),
    )
    .unwrap();
    assert!(
      claim_dispatch_completion(
        root.path(),
        &id,
        "core",
        "run",
        BackupDispatchKind::Backup
      )
      .is_err()
    );
    assert!(
      claim_dispatch_completion(
        root.path(),
        &id,
        "core",
        "run",
        BackupDispatchKind::Restore {
          journal_id: "different".into(),
          deferred: true
        }
      )
      .is_err()
    );
    finish_backup_completion(
      root.path(),
      &id,
      "core",
      "run",
      VykarBackupCompletion {
        state: VykarBackupCompletionState::Prepared,
        restore_result: Some(TransactionalVykarRestoreResponse {
          complete: true,
          finalization_pending: true,
          ..Default::default()
        }),
        ..Default::default()
      },
    )
    .unwrap();
    let replay = claim_dispatch_completion(
      root.path(),
      &id,
      "core",
      "run",
      restore_kind(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(replay.state, VykarBackupCompletionState::Prepared);
    let mut acknowledge = completion_query(&id);
    acknowledge.acknowledge = true;
    query_backup_completion(root.path(), &acknowledge, "core")
      .unwrap();
    assert!(
      read_backup_completion(
        &backup_completion_path(root.path(), &id).unwrap()
      )
      .unwrap()
      .unwrap()
      .completion
      .restore_result
      .unwrap()
      .finalization_pending
    );
  }

  #[test]
  fn interrupted_restore_is_not_terminal_while_recovery_is_pending() {
    for deferred in [false, true] {
      let receipt = restore_receipt(
        BackupDispatchKind::Restore {
          journal_id: "journal".into(),
          deferred,
        },
        VykarBackupCompletionState::Running,
      );
      assert_eq!(
        recovered_dispatch_completion(&receipt, None, true, None)
          .state,
        VykarBackupCompletionState::RecoveryRequired
      );
    }
    let prepared = restore_receipt(
      restore_kind(),
      VykarBackupCompletionState::Prepared,
    );
    assert_eq!(
      recovered_dispatch_completion(&prepared, None, false, None)
        .state,
      VykarBackupCompletionState::RecoveryRequired
    );
    let ordinary = restore_receipt(
      BackupDispatchKind::Restore {
        journal_id: "journal".into(),
        deferred: false,
      },
      VykarBackupCompletionState::Running,
    );
    let recovered =
      recovered_dispatch_completion(&ordinary, None, false, None);
    assert_eq!(recovered.state, VykarBackupCompletionState::Complete);
    assert!(recovered.error.is_some());
    assert!(recovered.restore_result.is_none());
  }

  #[test]
  fn completed_restore_journal_provides_a_recovered_final_outcome() {
    let receipt = restore_receipt(
      restore_kind(),
      VykarBackupCompletionState::RecoveryRequired,
    );
    let mut journal = RestoreJournal {
      staging: PathBuf::new(),
      entries: Vec::new(),
      committed: true,
      finalized: true,
      deferred: true,
      completed: true,
      owned_volume: None,
    };
    for commit in [false, true] {
      journal.committed = commit;
      let result = recovered_dispatch_completion(
        &receipt,
        Some(&journal),
        false,
        None,
      )
      .restore_result
      .unwrap();
      assert_eq!(result.complete, commit);
      assert_eq!(result.rolled_back, !commit);
      assert!(!result.finalization_pending);
    }
  }

  #[test]
  fn finalization_requires_matching_nonrunning_original_authority() {
    let root = tempfile::tempdir().unwrap();
    let original = uuid::Uuid::new_v4().to_string();
    claim_dispatch_completion(
      root.path(),
      &original,
      "core",
      "run",
      restore_kind(),
    )
    .unwrap();
    let request = finalize_request(&original);
    assert!(
      restore_origin_receipt(root.path(), &request, "core").is_err()
    );
    finish_backup_completion(
      root.path(),
      &original,
      "core",
      "run",
      VykarBackupCompletion {
        state: VykarBackupCompletionState::Prepared,
        ..Default::default()
      },
    )
    .unwrap();
    let path =
      backup_completion_path(root.path(), &original).unwrap();
    let unchanged = std::fs::read(&path).unwrap();
    for (invalid, core) in [
      (request.clone(), "other-core"),
      (
        {
          let mut value = request.clone();
          value.run_id = "other-run".into();
          value
        },
        "core",
      ),
      (
        {
          let mut value = request.clone();
          value.journal_id = "other-journal".into();
          value
        },
        "core",
      ),
      (finalize_request(""), "core"),
    ] {
      assert!(
        restore_origin_receipt(root.path(), &invalid, core).is_err()
      );
      assert_eq!(std::fs::read(&path).unwrap(), unchanged);
    }
    assert!(
      restore_origin_receipt(root.path(), &request, "core")
        .unwrap()
        .is_some()
    );
  }

  #[test]
  fn finalization_proof_survives_journal_erasure_and_receipt_acknowledgement()
   {
    let root = tempfile::tempdir().unwrap();
    let original = uuid::Uuid::new_v4().to_string();
    claim_dispatch_completion(
      root.path(),
      &original,
      "core",
      "run",
      restore_kind(),
    )
    .unwrap();
    finish_backup_completion(
      root.path(),
      &original,
      "core",
      "run",
      VykarBackupCompletion {
        state: VykarBackupCompletionState::Prepared,
        ..Default::default()
      },
    )
    .unwrap();
    let request = finalize_request(&original);
    claim_dispatch_completion(
      root.path(),
      &request.operation_id,
      "core",
      "run",
      finalize_kind(&request),
    )
    .unwrap();
    let outcome = FinalizeVykarRestoreResponse {
      complete: true,
      rolled_back: true,
      containers_restarted: vec!["writer".into()],
      critical_error: None,
    };
    persist_finalization_proof(
      root.path(),
      &request,
      "core",
      &outcome,
    )
    .unwrap();
    let finalizer = read_backup_completion(
      &backup_completion_path(root.path(), &request.operation_id)
        .unwrap(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
      finalizer.completion.state,
      VykarBackupCompletionState::Running
    );
    assert!(finalizer.finalized.is_some());
    let original = read_backup_completion(
      &backup_completion_path(root.path(), &original).unwrap(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
      original.completion.state,
      VykarBackupCompletionState::Complete
    );
    let restored =
      original.completion.restore_result.as_ref().unwrap();
    assert!(restored.rolled_back);
    assert!(!restored.finalization_pending);
    assert_eq!(restored.containers_restarted, ["writer"]);
    let recovered = recovered_dispatch_completion(
      &finalizer,
      None,
      false,
      Some(&original),
    );
    assert_eq!(recovered.state, VykarBackupCompletionState::Complete);
    assert!(recovered.finalize_restore_result.unwrap().rolled_back);
    let mut acknowledged =
      completion_query(&request.restore_operation_id);
    acknowledged.acknowledge = true;
    query_backup_completion(root.path(), &acknowledged, "core")
      .unwrap();
    let original =
      restore_origin_receipt(root.path(), &request, "core")
        .unwrap()
        .unwrap();
    assert!(original.completion.restore_result.is_none());
    assert!(
      finalization_from_origin(Some(&original), false)
        .unwrap()
        .rolled_back
    );
    assert!(finalization_from_origin(Some(&original), true).is_err());
  }

  #[test]
  fn missing_journal_without_finalization_proof_stays_unresolved() {
    let original = uuid::Uuid::new_v4().to_string();
    let request = finalize_request(&original);
    let receipt = restore_receipt(
      finalize_kind(&request),
      VykarBackupCompletionState::Running,
    );
    assert_eq!(
      recovered_dispatch_completion(&receipt, None, false, None)
        .state,
      VykarBackupCompletionState::RecoveryRequired
    );
    assert!(finalization_from_origin(None, false).is_err());
    let prepared = restore_receipt(
      restore_kind(),
      VykarBackupCompletionState::Prepared,
    );
    assert!(
      finalization_from_origin(Some(&prepared), false).is_err()
    );
  }

  #[test]
  fn fenced_restore_never_starts_and_cannot_be_committed() {
    let root = tempfile::tempdir().unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let mut fence = completion_query(&id);
    fence.cancel_if_unknown = true;
    query_backup_completion(root.path(), &fence, "core").unwrap();
    assert!(
      claim_dispatch_completion(
        root.path(),
        &id,
        "core",
        "run",
        restore_kind()
      )
      .unwrap()
      .unwrap()
      .error
      .is_some()
    );
    let origin = read_backup_completion(
      &backup_completion_path(root.path(), &id).unwrap(),
    )
    .unwrap()
    .unwrap();
    assert!(
      finalization_from_origin(Some(&origin), false)
        .unwrap()
        .rolled_back
    );
    assert!(finalization_from_origin(Some(&origin), true).is_err());
  }

  #[test]
  fn tracked_finalize_uses_a_wire_name_legacy_workers_cannot_execute()
  {
    #[derive(Deserialize)]
    #[serde(tag = "type", content = "params")]
    enum LegacyRequest {
      FinalizeVykarRestore(serde_json::Value),
    }
    let request = finalize_request(&uuid::Uuid::new_v4().to_string());
    let value = serde_json::to_value(
      crate::api::PeripheryRequest::RunFinalizeVykarRestore(
        RunFinalizeVykarRestore(request),
      ),
    )
    .unwrap();
    assert_eq!(value["type"], "RunFinalizeVykarRestore");
    match serde_json::from_value::<LegacyRequest>(value) {
      Err(_) => {}
      Ok(LegacyRequest::FinalizeVykarRestore(params)) => {
        panic!("Legacy worker accepted tracked mutation: {params}")
      }
    }
  }

  #[test]
  fn finalize_dispatch_identity_binds_the_decision_and_acknowledgement()
   {
    let root = tempfile::tempdir().unwrap();
    let mut request =
      finalize_request(&uuid::Uuid::new_v4().to_string());
    claim_dispatch_completion(
      root.path(),
      &request.operation_id,
      "core",
      "run",
      finalize_kind(&request),
    )
    .unwrap();
    request.commit = !request.commit;
    assert!(
      claim_dispatch_completion(
        root.path(),
        &request.operation_id,
        "core",
        "run",
        finalize_kind(&request)
      )
      .is_err()
    );
    request.commit = !request.commit;
    request.acknowledge = !request.acknowledge;
    assert!(
      claim_dispatch_completion(
        root.path(),
        &request.operation_id,
        "core",
        "run",
        finalize_kind(&request)
      )
      .is_err()
    );
    assert!(
      claim_dispatch_completion(
        root.path(),
        "",
        "core",
        "run",
        restore_kind()
      )
      .is_err()
    );
    assert!(
      claim_dispatch_completion(
        root.path(),
        &uuid::Uuid::new_v4().to_string(),
        "core",
        "",
        restore_kind()
      )
      .is_err()
    );
  }

  fn completion_query(
    operation_id: &str,
  ) -> GetVykarBackupCompletion {
    GetVykarBackupCompletion {
      operation_id: operation_id.into(),
      run_id: "run".into(),
      cancel_if_unknown: false,
      acknowledge: false,
    }
  }

  #[test]
  fn durable_backup_claim_replays_completion_and_acknowledgement_fences_reuse()
   {
    let root = tempfile::tempdir().unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    assert!(
      claim_backup_completion(root.path(), &id, "core", "run", true)
        .unwrap()
        .is_none()
    );
    assert!(
      claim_backup_completion(root.path(), &id, "core", "run", true)
        .is_err()
    );
    assert_eq!(
      query_backup_completion(
        root.path(),
        &completion_query(&id),
        "core"
      )
      .unwrap()
      .state,
      VykarBackupCompletionState::Running
    );
    assert!(
      query_backup_completion(
        root.path(),
        &completion_query(&id),
        "other-core"
      )
      .is_err()
    );
    let mut wrong_run = completion_query(&id);
    wrong_run.run_id = "different-run".into();
    assert!(
      query_backup_completion(root.path(), &wrong_run, "core")
        .is_err()
    );
    finish_backup_completion(
      root.path(),
      &id,
      "core",
      "run",
      VykarBackupCompletion {
        state: VykarBackupCompletionState::Complete,
        batch_result: Some(RunVykarBackupBatchResponse::default()),
        ..Default::default()
      },
    )
    .unwrap();
    assert!(
      claim_backup_completion(root.path(), &id, "core", "run", false)
        .is_err()
    );
    assert!(
      claim_backup_completion(root.path(), &id, "core", "run", true)
        .unwrap()
        .unwrap()
        .batch_result
        .is_some()
    );
    let mut acknowledge = completion_query(&id);
    acknowledge.acknowledge = true;
    assert!(
      query_backup_completion(root.path(), &acknowledge, "core")
        .unwrap()
        .batch_result
        .is_some()
    );
    let replay =
      claim_backup_completion(root.path(), &id, "core", "run", true)
        .unwrap()
        .unwrap();
    assert_eq!(replay.state, VykarBackupCompletionState::Complete);
    assert!(replay.batch_result.is_none());
    assert!(replay.error.unwrap().contains("acknowledged"));
  }

  #[test]
  fn unknown_backup_fence_prevents_a_late_dispatch() {
    let root = tempfile::tempdir().unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let mut query = completion_query(&id);
    assert_eq!(
      query_backup_completion(root.path(), &query, "core")
        .unwrap()
        .state,
      VykarBackupCompletionState::Unknown
    );
    query.cancel_if_unknown = true;
    let fenced =
      query_backup_completion(root.path(), &query, "core").unwrap();
    assert_eq!(fenced.state, VykarBackupCompletionState::Complete);
    assert!(
      claim_backup_completion(root.path(), &id, "core", "run", false)
        .unwrap()
        .unwrap()
        .error
        .unwrap()
        .contains("before it started")
    );
    assert!(
      claim_backup_completion(
        root.path(),
        "../invalid",
        "core",
        "run",
        false
      )
      .is_err()
    );
  }

  #[test]
  fn startup_reconciles_running_receipts_without_overwriting_completed_results()
   {
    let root = tempfile::tempdir().unwrap();
    let running = uuid::Uuid::new_v4().to_string();
    let done = uuid::Uuid::new_v4().to_string();
    for id in [&running, &done] {
      claim_backup_completion(root.path(), id, "core", "run", false)
        .unwrap();
    }
    finish_backup_completion(
      root.path(),
      &done,
      "core",
      "run",
      VykarBackupCompletion {
        state: VykarBackupCompletionState::Complete,
        result: Some(RunVykarBackupResponse::default()),
        ..Default::default()
      },
    )
    .unwrap();
    recover_backup_completions_in(root.path()).unwrap();
    assert!(
      query_backup_completion(
        root.path(),
        &completion_query(&running),
        "core"
      )
      .unwrap()
      .error
      .unwrap()
      .contains("interrupted")
    );
    assert!(
      query_backup_completion(
        root.path(),
        &completion_query(&done),
        "core"
      )
      .unwrap()
      .result
      .is_some()
    );
  }

  #[test]
  fn overlapping_dispatch_registrations_share_cancellation_until_the_last_finishes()
   {
    let id = uuid::Uuid::new_v4().to_string();
    let (first, first_registration) =
      register_operation_cancellation(&id);
    let (second, second_registration) =
      register_operation_cancellation(&id);
    assert!(Arc::ptr_eq(&first, &second));
    drop(first_registration);
    request_operation_cancellation(&id);
    assert!(second.load(Ordering::SeqCst));
    drop(second_registration);
    assert!(
      !cancellation_registry()
        .lock()
        .unwrap()
        .active
        .contains_key(&id)
    );
  }

  #[tokio::test]
  async fn expired_discovery_retains_its_nonqueued_slot_until_blocking_work_finishes()
   {
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let (release, waiting) = std::sync::mpsc::channel();
    let expired = bounded_backup_discovery(
      slots.clone(),
      Duration::ZERO,
      move || {
        waiting.recv().unwrap();
        Ok(())
      },
    )
    .await;
    assert!(expired.is_err());
    assert!(
      bounded_backup_discovery(
        slots.clone(),
        Duration::from_secs(60),
        || Ok(())
      )
      .await
      .is_err()
    );
    release.send(()).unwrap();
    // Acquiring this permit joins the actual worker lifetime, not its expired
    // HTTP wait. The next request may enter only after that worker exits.
    let permit = slots.clone().acquire_owned().await.unwrap();
    drop(permit);
    bounded_backup_discovery(slots, Duration::from_secs(60), || {
      Ok(())
    })
    .await
    .unwrap();
  }

  #[tokio::test]
  async fn backup_inventory_distinguishes_empty_success_from_docker_errors()
   {
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let empty = bounded_backup_inventory(
      &slots,
      Duration::from_secs(60),
      async { Ok(Vec::<VolumeListItem>::new()) },
    )
    .await
    .unwrap();
    assert!(empty.is_empty());
    for message in [
      "Docker is not connected",
      "Backup container inventory failed",
      "Backup volume inventory failed",
    ] {
      let error = bounded_backup_inventory::<Vec<VolumeListItem>>(
        &slots,
        Duration::from_secs(60),
        async { Err(anyhow!(message)) },
      )
      .await
      .unwrap_err();
      assert_eq!(error.to_string(), message);
      assert_eq!(slots.available_permits(), 1);
    }
  }

  #[tokio::test]
  async fn backup_inventory_refuses_overlaps_and_releases_expired_work()
   {
    use futures_util::FutureExt;

    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let work = bounded_backup_inventory(
      &slots,
      Duration::from_secs(60),
      std::future::pending::<anyhow::Result<()>>(),
    );
    tokio::pin!(work);
    assert!(work.as_mut().now_or_never().is_none());
    assert_eq!(slots.available_permits(), 0);
    let polled = AtomicBool::new(false);
    let refused =
      bounded_backup_inventory(&slots, Duration::ZERO, async {
        polled.store(true, Ordering::SeqCst);
        Ok(())
      })
      .await;
    assert!(
      refused.unwrap_err().to_string().contains("already running")
    );
    assert!(!polled.load(Ordering::SeqCst));

    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let expired = bounded_backup_inventory(
      &slots,
      Duration::ZERO,
      std::future::pending::<anyhow::Result<()>>(),
    )
    .await;
    assert!(expired.unwrap_err().to_string().contains("deadline"));
    assert_eq!(slots.available_permits(), 1);
  }

  #[test]
  fn pending_journals_gate_new_work_until_recovery_removes_them() {
    let directory = tempfile::tempdir().unwrap();
    let directories = vec![directory.path().to_path_buf()];
    assert!(ensure_recovery_directories_empty(&directories).is_ok());
    for contents in
      ["indeterminate", r#"{"completed":true,"deferred":true}"#]
    {
      let journal = directory.path().join("earlier-operation.json");
      std::fs::write(&journal, contents).unwrap();
      // No in-memory latch or current operation ID: a new process sees the
      // same gate, and even an unreadable journal fails closed.
      assert!(
        ensure_recovery_directories_empty(&directories).is_err()
      );
      std::fs::remove_file(journal).unwrap();
      assert!(
        ensure_recovery_directories_empty(&directories).is_ok()
      );
    }
    assert!(
      ensure_recovery_directories_empty(&[directory
        .path()
        .join("missing")])
      .is_err()
    );
  }

  #[test]
  fn terminal_and_protected_filesystem_leases_exclude_each_other() {
    let barrier = Arc::new(tokio::sync::RwLock::new(()));
    let terminal = barrier.clone().try_read_owned().unwrap();
    assert!(barrier.clone().try_write_owned().is_err());
    drop(terminal);
    let backup = barrier.clone().try_write_owned().unwrap();
    assert!(barrier.clone().try_read_owned().is_err());
    drop(backup);
    assert!(barrier.try_read_owned().is_ok());
  }

  #[test]
  fn locked_preview_rejects_changed_paths_and_containers() {
    let preview = PreflightVykarRestoreResponse {
      destination_exists: true,
      created_paths: vec!["b".into(), "a".into()],
      containers_to_stop: vec!["application".into()],
      ..Default::default()
    };
    let mut reordered = preview.clone();
    reordered.created_paths.reverse();
    assert!(preview.matches(&reordered));
    reordered.deleted_paths.push("new-file".into());
    assert!(!preview.matches(&reordered));
    let mut changed = preview.clone();
    changed.containers_to_stop.clear();
    assert!(!preview.matches(&changed));
    changed = preview.clone();
    changed.destination_exists = false;
    assert!(!preview.matches(&changed));
  }

  #[test]
  fn cancellation_registration_shares_and_cleans_up_token() {
    let id = "cancellable-backup-test";
    let (worker, registration) = register_operation_cancellation(id);
    assert!(request_operation_cancellation(id));
    assert!(worker.load(Ordering::SeqCst));
    drop(registration);
    assert!(!operation_cancelled(id));
  }

  #[test]
  fn cancellation_before_registration_is_bounded_and_consumed() {
    let id = "early-cancellable-backup-test";
    assert!(request_operation_cancellation(id));
    {
      let registry = cancellation_registry().lock().unwrap();
      assert!(!registry.active.contains_key(id));
      assert!(registry.pending.contains_key(id));
      assert!(registry.pending.len() <= MAX_PENDING_CANCELLATIONS);
    }
    let (worker, registration) = register_operation_cancellation(id);
    assert!(worker.load(Ordering::SeqCst));
    assert!(
      !cancellation_registry()
        .lock()
        .unwrap()
        .pending
        .contains_key(id)
    );
    drop(registration);
  }

  #[test]
  fn backup_sources_cannot_capture_internal_backup_storage() {
    let root = tempfile::tempdir().unwrap();
    let internal = root.path().join(".komodo-vykar");
    let stack = root.path().join("stack");
    std::fs::create_dir_all(&stack).unwrap();

    assert!(
      validate_path_outside_internal_storage(
        root.path(),
        &internal,
        "Backup source",
      )
      .is_err()
    );
    validate_path_outside_internal_storage(
      &stack,
      &internal,
      "Backup source",
    )
    .unwrap();
  }

  #[test]
  fn backup_sources_cannot_capture_core_repository_mounts() {
    let root = tempfile::tempdir().unwrap();
    let repository = root.path().join("core-repository");
    let application = root.path().join("application");
    std::fs::create_dir_all(&repository).unwrap();
    std::fs::create_dir_all(&application).unwrap();

    assert!(
      validate_path_outside_protected_repositories(
        root.path(),
        std::slice::from_ref(&repository),
        "Backup source",
      )
      .is_err()
    );
    assert!(
      validate_path_outside_protected_repositories(
        &repository.join("packs"),
        std::slice::from_ref(&repository),
        "Backup source",
      )
      .is_err()
    );
    validate_path_outside_protected_repositories(
      &application,
      &[repository],
      "Backup source",
    )
    .unwrap();
  }

  #[test]
  fn restore_destinations_cannot_replace_core_repository_mounts() {
    let root = tempfile::tempdir().unwrap();
    let repository = root.path().join("core-repository");
    let alias = root.path().join("repository-alias");
    std::fs::create_dir_all(&repository).unwrap();
    std::os::unix::fs::symlink(&repository, &alias).unwrap();

    assert!(
      validate_path_outside_protected_repositories(
        &alias.join("packs"),
        std::slice::from_ref(&repository),
        "Restore destination",
      )
      .is_err()
    );
  }

  #[test]
  fn protected_repository_mapping_preserves_the_mount_subpath() {
    assert_eq!(
      map_path_through_mount(
        Path::new("/data/backups/vykar"),
        Path::new("/data"),
        Path::new("/var/lib/docker/volumes/komodo_data/_data"),
      ),
      Some(PathBuf::from(
        "/var/lib/docker/volumes/komodo_data/_data/backups/vykar"
      ))
    );
    assert_eq!(
      map_path_through_mount(
        Path::new("/data/backups"),
        Path::new("/data/backups/vykar"),
        Path::new("/repository-volume"),
      ),
      Some(PathBuf::from("/repository-volume"))
    );
    let mapped = map_path_through_mount(
      Path::new("/data/backups/vykar"),
      Path::new("/data"),
      Path::new("/host/data"),
    )
    .unwrap();
    assert!(
      !paths_overlap(Path::new("/host/data/stacks"), &mapped)
        .unwrap()
    );
  }

  #[test]
  fn private_core_mounts_and_worker_aliases_are_safety_exclusions() {
    let root = tempfile::tempdir().unwrap();
    let private = root.path().join("core-secrets");
    let alias = root.path().join("worker-alias");
    std::fs::create_dir_all(&private).unwrap();
    std::os::unix::fs::symlink(&private, &alias).unwrap();
    let mapped = map_path_through_mount(
      Path::new("/core-secrets"),
      Path::new("/core-secrets"),
      &private,
    )
    .unwrap();
    for path in [
      private.as_path(),
      alias.as_path(),
      &alias.join("backup.key"),
      root.path(),
    ] {
      for operation in ["Backup source", "Restore destination"] {
        let error = validate_path_outside_protected_repositories(
          path,
          std::slice::from_ref(&mapped),
          operation,
        )
        .unwrap_err();
        assert!(error.is::<ExcludedBackupSource>());
      }
    }
    validate_path_outside_protected_repositories(
      &root.path().join("application"),
      &[mapped],
      "Backup source",
    )
    .unwrap();
  }

  #[test]
  fn core_and_skipped_database_targets_are_refused_even_when_stopped()
  {
    let core_id = "a".repeat(64);
    let protected = vec![ProtectedRepositoryPath {
      path: "/core-secrets".into(),
      core_container_id: core_id.clone(),
    }];
    let stack = komodo_client::entities::stack::Stack {
      name: "control-plane".into(),
      ..Default::default()
    };
    let core = ContainerListItem {
      id: Some(core_id),
      name: "custom-core-name".into(),
      state: ContainerStateStatusEnum::Exited,
      volumes: vec!["private-secrets".into()],
      labels: HashMap::from([(
        COMPOSE_PROJECT_LABEL.into(),
        stack.project_name(false),
      )]),
      ..Default::default()
    };
    let database = ContainerListItem {
      name: "database".into(),
      state: ContainerStateStatusEnum::Exited,
      volumes: vec!["database-data".into()],
      labels: HashMap::from([("komodo.skip".into(), "true".into())]),
      ..Default::default()
    };
    let containers = vec![core, database];
    for volume_name in ["private-secrets", "database-data"] {
      let error = ensure_target_not_control_plane(
        &containers,
        &PeripheryBackupTarget::Volume {
          volume_name: volume_name.into(),
        },
        &protected,
      )
      .unwrap_err();
      assert!(error.is::<ExcludedBackupSource>());
    }
    assert!(
      ensure_target_not_control_plane(
        &containers,
        &PeripheryBackupTarget::Stack {
          stack: Box::new(stack),
          repo: None
        },
        &protected
      )
      .is_err()
    );
    ensure_target_not_control_plane(
      &containers,
      &PeripheryBackupTarget::Volume {
        volume_name: "application-data".into(),
      },
      &protected,
    )
    .unwrap();
  }

  #[test]
  fn quiescing_keeps_core_and_skip_label_containers_running() {
    let protected = vec![ProtectedRepositoryPath {
      path: "/core-secrets".into(),
      core_container_id: "a".repeat(64),
    }];
    let core = ContainerListItem {
      id: Some("a".repeat(64)),
      name: "core".into(),
      state: ContainerStateStatusEnum::Running,
      volumes: vec!["shared".into()],
      ..Default::default()
    };
    let database = ContainerListItem {
      id: Some("b".repeat(64)),
      name: "database".into(),
      labels: HashMap::from([("komodo.skip".into(), "true".into())]),
      ..core.clone()
    };
    let application = ContainerListItem {
      id: Some("c".repeat(64)),
      name: "application".into(),
      labels: HashMap::from([("komodo.skip".into(), "false".into())]),
      ..core.clone()
    };
    assert_eq!(
      running_containers_for_target(
        &[core, database, application],
        &PeripheryBackupTarget::Volume {
          volume_name: "shared".into()
        },
        Some(&"d".repeat(64)),
        &protected
      ),
      vec!["application"]
    );
  }

  #[test]
  fn repository_aliases_require_the_actual_core_container_identity() {
    let core = ContainerListItem {
      id: Some("a".repeat(64)),
      ..Default::default()
    };
    let mut unrelated = core.clone();
    unrelated.id = Some("b".repeat(64));
    unrelated.name = "komodo".into();
    assert!(container_matches_id(&core, &"a".repeat(64)));
    assert!(container_matches_id(&core, &"a".repeat(12)));
    assert!(!container_matches_id(&unrelated, &"a".repeat(64)));
    assert!(!container_matches_id(&unrelated, "komodo"));
    assert!(!container_matches_id(&core, ""));
    // Translate host protection into Periphery's namespace, not /data in
    // every application container on that Docker daemon.
    assert_eq!(
      map_path_through_mount(
        Path::new("/host/core/backups/vykar"),
        Path::new("/host/core"),
        Path::new("/periphery/data"),
      ),
      Some(PathBuf::from("/periphery/data/backups/vykar"))
    );
    assert_eq!(
      map_path_through_mount(
        Path::new("/host/core/backups/vykar"),
        Path::new("/srv/app"),
        Path::new("/data"),
      ),
      None
    );
  }

  #[test]
  fn bind_root_filters_use_vykar_path_rules() {
    let root = tempfile::tempdir().unwrap();
    let run = root.path().join("run");
    let included = root.path().join("binds/application");
    let excluded = root.path().join("binds/cache");
    let outside = root.path().join("other/data");
    for path in [&run, &included, &excluded, &outside] {
      std::fs::create_dir_all(path).unwrap();
    }
    let root_pattern =
      root.path().to_string_lossy().replace('\\', "/");
    let selected = select_bind_backup_roots(
      [included.clone(), excluded, outside].into_iter().collect(),
      &run,
      &BackupSourceFilters {
        bind_mount_include_patterns: vec![format!(
          "{root_pattern}/binds/**"
        )],
        bind_mount_exclude_patterns: vec!["**/cache".into()],
        ..Default::default()
      },
    )
    .unwrap();
    assert_eq!(selected, [included].into_iter().collect());
  }

  #[test]
  fn bind_candidates_are_filtered_before_nested_roots_are_coalesced()
  {
    let root = tempfile::tempdir().unwrap();
    let run = root.path().join("run");
    let parent = root.path().join("data");
    let child = parent.join("selected");
    std::fs::create_dir_all(&run).unwrap();
    std::fs::create_dir_all(&child).unwrap();
    let mut stack = komodo_client::entities::stack::Stack::default();
    stack.info.deployed_config = Some(format!(
      "services:\n  app:\n    volumes:\n      - '{}:/parent'\n      - '{}:/child'\n",
      parent.display(),
      child.display(),
    ));
    let candidates = compose_bind_paths(&stack, &run).unwrap();
    assert_eq!(candidates.len(), 2);
    let selected = select_bind_backup_roots(
      candidates.clone(),
      &run,
      &BackupSourceFilters {
        bind_mount_include_patterns: vec![
          child.to_string_lossy().into_owned(),
        ],
        ..Default::default()
      },
    )
    .unwrap();
    assert_eq!(selected, BTreeSet::from([child]));
    assert_eq!(
      select_bind_backup_roots(
        candidates,
        &run,
        &BackupSourceFilters::default()
      )
      .unwrap(),
      BTreeSet::from([parent])
    );
  }

  #[test]
  fn options_backed_volumes_are_excluded_without_rejecting_quotas() {
    for options in [
      HashMap::from([("type".into(), "nfs".into())]),
      HashMap::from([
        ("device".into(), "/srv/data".into()),
        ("o".into(), "bind".into()),
      ]),
    ] {
      let error =
        validate_local_volume_mount_options(&options).unwrap_err();
      assert!(error.is::<ExcludedBackupSource>());
    }
    validate_local_volume_mount_options(&HashMap::new()).unwrap();
    validate_local_volume_mount_options(&HashMap::from([(
      "size".into(),
      "10G".into(),
    )]))
    .unwrap();
  }

  fn container(
    name: &str,
    state: ContainerStateStatusEnum,
    project: Option<&str>,
    volumes: &[&str],
  ) -> ContainerListItem {
    let mut labels = std::collections::HashMap::new();
    if let Some(project) = project {
      labels.insert(COMPOSE_PROJECT_LABEL.into(), project.into());
    }
    ContainerListItem {
      name: name.into(),
      state,
      volumes: volumes
        .iter()
        .map(|volume| (*volume).into())
        .collect(),
      labels,
      ..Default::default()
    }
  }

  #[test]
  fn stack_restore_stops_the_whole_running_deployed_project() {
    let stack = komodo_client::entities::stack::Stack {
      name: "configured-stack-name".into(),
      config: komodo_client::entities::stack::StackConfig {
        project_name: "new-project-name".into(),
        ..Default::default()
      },
      info: komodo_client::entities::stack::StackInfo {
        deployed_project_name: Some("deployed-project-name".into()),
        ..Default::default()
      },
      ..Default::default()
    };
    let target = PeripheryBackupTarget::Stack {
      stack: Box::new(stack),
      repo: None,
    };
    let containers = vec![
      container(
        "web",
        ContainerStateStatusEnum::Running,
        Some("deployed-project-name"),
        &[],
      ),
      container(
        "worker",
        ContainerStateStatusEnum::Running,
        Some("deployed-project-name"),
        &[],
      ),
      container(
        "already-stopped",
        ContainerStateStatusEnum::Exited,
        Some("deployed-project-name"),
        &[],
      ),
      container(
        "unrelated",
        ContainerStateStatusEnum::Running,
        Some("other-project"),
        &[],
      ),
    ];

    assert_eq!(
      running_containers_for_target(&containers, &target, None, &[]),
      ["web", "worker"]
    );
  }

  #[test]
  fn volume_restore_stops_every_running_container_with_access() {
    let target = PeripheryBackupTarget::Volume {
      volume_name: "shared-data".into(),
    };
    let containers = vec![
      container(
        "stack-a-web",
        ContainerStateStatusEnum::Running,
        Some("stack-a"),
        &["shared-data"],
      ),
      container(
        "stack-b-worker",
        ContainerStateStatusEnum::Running,
        Some("stack-b"),
        &["shared-data", "other-data"],
      ),
      container(
        "already-stopped",
        ContainerStateStatusEnum::Exited,
        Some("stack-c"),
        &["shared-data"],
      ),
      container(
        "unrelated",
        ContainerStateStatusEnum::Running,
        None,
        &["other-data"],
      ),
    ];

    assert_eq!(
      running_containers_for_target(&containers, &target, None, &[]),
      ["stack-a-web", "stack-b-worker"]
    );
  }

  #[test]
  fn source_validation_rejects_relative_paths() {
    assert!(
      validate_source_path(Path::new("relative/path")).is_err()
    );
  }

  #[test]
  fn restore_destinations_resolve_symlinked_existing_ancestors() {
    let root = tempfile::tempdir().unwrap();
    let real = root.path().join("real");
    let alias = root.path().join("alias");
    std::fs::create_dir(&real).unwrap();
    std::os::unix::fs::symlink(&real, &alias).unwrap();
    assert_eq!(
      komodo_backup::filesystem::resolve_existing_ancestor(
        &alias.join("new/child"),
      )
      .unwrap(),
      real.canonicalize().unwrap().join("new/child")
    );
  }

  #[test]
  fn restore_destinations_reject_overlap_through_symlinks() {
    let root = tempfile::tempdir().unwrap();
    let real = root.path().join("real");
    let alias = root.path().join("alias");
    std::fs::create_dir(&real).unwrap();
    std::os::unix::fs::symlink(&real, &alias).unwrap();
    let publish = vec![
      RestorePublishPath {
        destination_root: None,
        snapshot_path: "source/one".into(),
        destination: real.join("app").to_string_lossy().into_owned(),
      },
      RestorePublishPath {
        destination_root: None,
        snapshot_path: "source/two".into(),
        destination: alias
          .join("app/data")
          .to_string_lossy()
          .into_owned(),
      },
    ];
    assert!(
      validate_resolved_restore_destinations(&publish).is_err()
    );
  }

  #[test]
  fn restore_destinations_cannot_replace_internal_backup_storage() {
    let root = tempfile::tempdir().unwrap();
    let internal = root.path().join(".komodo-vykar");
    let publish = vec![RestorePublishPath {
      destination_root: None,
      snapshot_path: "source/root".into(),
      destination: root.path().to_string_lossy().into_owned(),
    }];
    assert!(
      validate_resolved_restore_destinations_against(
        &publish, &internal,
      )
      .is_err()
    );
  }

  #[test]
  fn distinct_symlink_leaf_destinations_can_replace_entries_sharing_a_target()
   {
    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("target");
    std::fs::write(&target, b"original").unwrap();
    let publish = ["first", "second"]
      .into_iter()
      .map(|name| {
        let path = root.path().join(name);
        std::os::unix::fs::symlink(&target, &path).unwrap();
        RestorePublishPath {
          destination_root: None,
          snapshot_path: name.into(),
          destination: path.to_string_lossy().into_owned(),
        }
      })
      .collect::<Vec<_>>();
    validate_resolved_restore_destinations_against(
      &publish,
      &root.path().join("private"),
    )
    .unwrap();
    validate_restore_rollback_paths(&publish, "id").unwrap();
  }

  #[test]
  fn rollback_names_must_not_alias_each_other_or_another_destination()
  {
    let root = tempfile::tempdir().unwrap();
    let real = root.path().join("real");
    let alias = root.path().join("alias");
    std::fs::create_dir(&real).unwrap();
    std::os::unix::fs::symlink(&real, &alias).unwrap();
    let item = |destination: PathBuf| RestorePublishPath {
      destination_root: None,
      snapshot_path: "source/config".into(),
      destination: destination.to_string_lossy().into_owned(),
    };
    assert!(
      validate_restore_rollback_paths(
        &[item(real.join("config")), item(alias.join("config"))],
        "id"
      )
      .is_err()
    );
    assert!(
      validate_restore_rollback_paths(
        &[
          item(real.join("config")),
          item(real.join("config.komodo-rollback-id"))
        ],
        "id"
      )
      .is_err()
    );
  }

  #[test]
  fn selected_restore_rejects_symlink_ancestors_but_can_replace_the_leaf()
   {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let link = root.path().join("link");
    std::os::unix::fs::symlink(outside.path(), &link).unwrap();
    let mut item = RestorePublishPath {
      destination_root: Some(
        root.path().to_string_lossy().into_owned(),
      ),
      snapshot_path: "source/link/config".into(),
      destination: link.join("config").to_string_lossy().into_owned(),
    };
    assert!(validate_restore_destination_ancestors(&item).is_err());
    item.destination = link.to_string_lossy().into_owned();
    validate_restore_destination_ancestors(&item).unwrap();
    item.destination_root = Some(link.to_string_lossy().into_owned());
    item.destination =
      link.join("config").to_string_lossy().into_owned();
    assert!(validate_restore_destination_ancestors(&item).is_err());
    item.destination_root =
      Some(root.path().to_string_lossy().into_owned());
    item.destination = root
      .path()
      .join("missing/child")
      .to_string_lossy()
      .into_owned();
    validate_restore_destination_ancestors(&item).unwrap();
    item.destination =
      outside.path().join("config").to_string_lossy().into_owned();
    assert!(validate_restore_destination_ancestors(&item).is_err());
  }

  #[test]
  fn full_restore_rejects_linked_parents_but_can_replace_the_leaf() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let link = root.path().join("link");
    std::os::unix::fs::symlink(outside.path(), &link).unwrap();
    let mut item = RestorePublishPath {
      destination_root: None,
      snapshot_path: "source/config".into(),
      destination: link.join("config").to_string_lossy().into_owned(),
    };
    assert!(validate_restore_destination_ancestors(&item).is_err());
    item.destination = link.to_string_lossy().into_owned();
    validate_restore_destination_ancestors(&item).unwrap();
    item.destination = root
      .path()
      .join("missing/child")
      .to_string_lossy()
      .into_owned();
    validate_restore_destination_ancestors(&item).unwrap();
    let file = root.path().join("file");
    std::fs::write(&file, "not a directory").unwrap();
    item.destination =
      file.join("child").to_string_lossy().into_owned();
    assert!(validate_restore_destination_ancestors(&item).is_err());
    item.destination = "relative/path".into();
    assert!(validate_restore_destination_ancestors(&item).is_err());
  }

  #[test]
  fn selected_restore_also_checks_ancestors_above_its_root() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let link = root.path().join("link");
    std::fs::create_dir(outside.path().join("nested")).unwrap();
    std::os::unix::fs::symlink(outside.path(), &link).unwrap();
    let item = RestorePublishPath {
      destination_root: Some(
        link.join("nested").to_string_lossy().into_owned(),
      ),
      snapshot_path: "source/config".into(),
      destination: link
        .join("nested/config")
        .to_string_lossy()
        .into_owned(),
    };
    assert!(validate_restore_destination_ancestors(&item).is_err());
  }

  #[test]
  fn selected_volume_destinations_use_the_inspected_mountpoint() {
    let mut publish = vec![RestorePublishPath {
      destination_root: None,
      snapshot_path: "source/_data/config/app.toml".into(),
      destination:
        "/var/lib/docker/volumes/app-data/_data/config/app.toml"
          .into(),
    }];
    resolve_volume_publish_destinations(
      &mut publish,
      "app-data",
      "/custom/docker/volumes/app-data/data",
      false,
    )
    .unwrap();
    assert_eq!(
      publish[0].destination,
      "/custom/docker/volumes/app-data/data/config/app.toml"
    );
    assert_eq!(
      publish[0].destination_root.as_deref(),
      Some("/custom/docker/volumes/app-data/data")
    );
  }

  #[test]
  fn deployed_compose_config_discovers_bind_roots_without_containers()
  {
    let run_directory = tempfile::tempdir().unwrap();
    let bind_directory = tempfile::tempdir().unwrap();
    let mut stack = komodo_client::entities::stack::Stack::default();
    stack.info.deployed_config = Some(format!(
      "services:\n  app:\n    volumes:\n      - type: bind\n        source: '{}'\n        target: /data\n",
      bind_directory.path().display()
    ));
    let paths =
      compose_bind_paths(&stack, run_directory.path()).unwrap();
    assert_eq!(
      paths,
      [bind_directory.path().canonicalize().unwrap()]
        .into_iter()
        .collect()
    );
  }

  #[test]
  fn recovered_compose_rewrites_long_and_short_absolute_binds() {
    let mut document: serde_yaml_ng::Value = serde_yaml_ng::from_str(
      "services:\n  app:\n    volumes:\n      - type: bind\n        source: /srv/old/data\n        target: /data\n      - /srv/old/cache:/cache:ro\n      - named-data:/named\n",
    )
    .unwrap();
    let mappings = HashMap::from([(
      "/srv/old".to_string(),
      "/srv/recovered".to_string(),
    )]);
    assert_eq!(
      rewrite_compose_bind_mappings(
        &mut document,
        &mappings,
        &HashMap::new(),
      ),
      2
    );
    let rewritten = serde_yaml_ng::to_string(&document).unwrap();
    assert!(rewritten.contains("/srv/recovered/data"));
    assert!(rewritten.contains("/srv/recovered/cache:/cache:ro"));
    assert!(rewritten.contains("named-data:/named"));
    assert!(!rewritten.contains("/srv/old"));
  }

  #[test]
  fn recovered_compose_rewrites_a_recorded_symlink_alias() {
    let mut document: serde_yaml_ng::Value = serde_yaml_ng::from_str(
      "services:\n  app:\n    volumes:\n      - /srv/link/cache:/cache:ro\n",
    )
    .unwrap();
    let mappings = HashMap::from([(
      "/srv/real".to_string(),
      "/srv/recovered".to_string(),
    )]);
    let aliases = HashMap::from([(
      "/srv/link/cache".to_string(),
      "/srv/real/cache".to_string(),
    )]);
    assert_eq!(
      rewrite_compose_bind_mappings(
        &mut document,
        &mappings,
        &aliases,
      ),
      1
    );
    let rewritten = serde_yaml_ng::to_string(&document).unwrap();
    assert!(rewritten.contains("/srv/recovered/cache:/cache:ro"));
    assert!(!rewritten.contains("/srv/link"));
  }

  #[test]
  fn recovered_compose_resolves_bind_expressions_from_snapshot_metadata()
   {
    let mut document: serde_yaml_ng::Value = serde_yaml_ng::from_str(
      "services:\n  app:\n    volumes:\n      - ${DATA_ROOT:-/fallback}/cache:/cache:ro\n      - type: bind\n        source: ${DATA_ROOT}/data\n        target: /data\n      - ${VOLUME_NAME}:/named\n",
    ).unwrap();
    let deployed = "services:\n  app:\n    volumes:\n      - type: bind\n        source: /srv/link/cache\n        target: /cache\n      - type: bind\n        source: /srv/old/data\n        target: /data\n      - type: volume\n        source: named-data\n        target: /named\n";
    let mappings =
      HashMap::from([("/srv/old".into(), "/srv/recovered".into())]);
    let aliases = HashMap::from([(
      "/srv/link/cache".into(),
      "/srv/old/cache".into(),
    )]);
    resolve_recovered_bind_expressions(
      &mut document,
      Some(deployed),
      &mappings,
      &aliases,
    )
    .unwrap();
    assert_eq!(
      rewrite_compose_bind_mappings(
        &mut document,
        &mappings,
        &aliases
      ),
      2
    );
    let rewritten = serde_yaml_ng::to_string(&document).unwrap();
    assert!(rewritten.contains("/srv/recovered/cache:/cache:ro"));
    assert!(rewritten.contains("/srv/recovered/data"));
    assert!(rewritten.contains("${VOLUME_NAME}:/named"));
    assert!(!rewritten.contains("${DATA_ROOT"));
  }

  #[test]
  fn recovered_bind_expressions_fail_closed_without_deployment_metadata()
   {
    let mut document: serde_yaml_ng::Value = serde_yaml_ng::from_str(
      "services:\n  app:\n    volumes:\n      - ${DATA_ROOT}/cache:/cache\n",
    ).unwrap();
    assert!(
      resolve_recovered_bind_expressions(
        &mut document,
        None,
        &HashMap::new(),
        &HashMap::new()
      )
      .is_err()
    );
  }

  #[test]
  fn recovered_relative_binds_use_original_deployment_and_alias_metadata()
   {
    let mut document: serde_yaml_ng::Value = serde_yaml_ng::from_str(
      "services:\n  app:\n    volumes:\n      - ../data:/data:ro\n      - type: bind\n        source: ../link\n        target: /cache\n      - ./config:/config\n      - named-data:/named\n",
    ).unwrap();
    let deployed = "services:\n  app:\n    volumes:\n      - type: bind\n        source: /original/data\n        target: /data\n      - type: bind\n        source: /original/link\n        target: /cache\n      - type: bind\n        source: /original/run/config\n        target: /config\n";
    let mappings = HashMap::from([
      ("/original/data".into(), "/recovery/data".into()),
      ("/original/real".into(), "/recovery/cache".into()),
      ("/original/run".into(), "/recovery/run".into()),
    ]);
    let aliases = HashMap::from([(
      "/original/link".into(),
      "/original/real".into(),
    )]);
    resolve_recovered_bind_expressions(
      &mut document,
      Some(deployed),
      &mappings,
      &aliases,
    )
    .unwrap();
    assert_eq!(
      rewrite_compose_bind_mappings(
        &mut document,
        &mappings,
        &aliases
      ),
      3
    );
    let volumes = document["services"]["app"]["volumes"]
      .as_sequence()
      .unwrap();
    for (index, expected_source, expected_target) in [
      (0, "/recovery/data", "/data:ro"),
      (2, "/recovery/run/config", "/config"),
      (3, "named-data", "/named"),
    ] {
      let (source, target) =
        split_compose_short_mount(volumes[index].as_str().unwrap())
          .unwrap();
      assert_eq!(Path::new(source), Path::new(expected_source));
      assert_eq!(target, expected_target);
    }
    assert_eq!(volumes[1]["type"].as_str().unwrap(), "bind");
    assert_eq!(
      Path::new(volumes[1]["source"].as_str().unwrap()),
      Path::new("/recovery/cache"),
    );
    assert_eq!(volumes[1]["target"].as_str().unwrap(), "/cache");
  }

  #[test]
  fn relative_binds_fail_closed_without_absolute_original_metadata() {
    for deployed in [
      None,
      Some(
        "services:\n  app:\n    volumes:\n      - type: bind\n        source: ../data\n        target: /data\n",
      ),
    ] {
      let mut document = serde_yaml_ng::from_str(
        "services:\n  app:\n    volumes:\n      - ../data:/data\n",
      )
      .unwrap();
      assert!(
        resolve_recovered_bind_expressions(
          &mut document,
          deployed,
          &HashMap::new(),
          &HashMap::new()
        )
        .is_err()
      );
    }
  }

  #[test]
  fn nested_stack_bind_roots_collapse_to_the_ancestor() {
    let run_directory = tempfile::tempdir().unwrap();
    let binds = tempfile::tempdir().unwrap();
    let child = binds.path().join("cache");
    std::fs::create_dir(&child).unwrap();
    let mut paths = BTreeSet::new();
    insert_bind_backup_root(&mut paths, run_directory.path(), &child)
      .unwrap();
    insert_bind_backup_root(
      &mut paths,
      run_directory.path(),
      binds.path(),
    )
    .unwrap();
    assert_eq!(
      paths,
      [binds.path().canonicalize().unwrap()].into_iter().collect()
    );
    insert_bind_backup_root(&mut paths, run_directory.path(), &child)
      .unwrap();
    assert_eq!(paths.len(), 1);
  }

  #[test]
  fn recovered_compose_rewrite_rejects_leaf_and_parent_symlinks() {
    let staging = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let original = outside.path().join("compose.yaml");
    std::fs::write(&original, b"unrelated host file").unwrap();
    let leaf = staging.path().join("compose.yaml");
    std::os::unix::fs::symlink(&original, &leaf).unwrap();
    assert!(open_staged_compose_file(staging.path(), &leaf).is_err());
    let parent = staging.path().join("linked-directory");
    std::os::unix::fs::symlink(outside.path(), &parent).unwrap();
    assert!(
      open_staged_compose_file(
        staging.path(),
        &parent.join("compose.yaml")
      )
      .is_err()
    );
    assert_eq!(
      std::fs::read(&original).unwrap(),
      b"unrelated host file"
    );
    let regular = staging.path().join("regular.yaml");
    std::fs::write(&regular, b"services: {}").unwrap();
    assert!(
      open_staged_compose_file(staging.path(), &regular).is_ok()
    );
  }

  #[test]
  fn large_previews_keep_complete_counts_and_hash_omitted_changes() {
    let created = (0..MAX_RESTORE_PREVIEW_ROWS + 1)
      .map(|index| format!("/root/{index:06}"))
      .collect::<Vec<_>>();
    let preview = bounded_restore_preview(
      created.clone(),
      vec!["/overwrite".into()],
      vec!["/delete".into()],
    );
    let summary = preview.path_summary.as_ref().unwrap();
    assert_eq!(
      summary.created,
      (MAX_RESTORE_PREVIEW_ROWS + 1) as u32
    );
    assert_eq!(summary.overwritten, 1);
    assert_eq!(summary.deleted, 1);
    assert_eq!(preview.created_paths.len(), MAX_RESTORE_PREVIEW_ROWS);
    assert!(preview.overwritten_paths.is_empty());
    assert!(preview.deleted_paths.is_empty());
    let mut changed = created;
    *changed.last_mut().unwrap() = "/root/999999".into();
    let changed = bounded_restore_preview(
      changed,
      vec!["/overwrite".into()],
      vec!["/delete".into()],
    );
    assert_eq!(preview.created_paths, changed.created_paths);
    assert!(!preview.matches(&changed));
    let reclassified = bounded_restore_preview(
      Vec::new(),
      preview.created_paths.clone(),
      Vec::new(),
    );
    assert_ne!(
      summary.sha256,
      reclassified.path_summary.unwrap().sha256
    );
  }

  #[test]
  fn preview_digest_is_order_independent_but_category_sensitive() {
    let expected = bounded_restore_preview(
      vec!["a".into(), "b".into()],
      Vec::new(),
      Vec::new(),
    );
    let reordered = bounded_restore_preview(
      vec!["b".into(), "a".into(), "a".into()],
      Vec::new(),
      Vec::new(),
    );
    assert!(expected.matches(&reordered));
    let reclassified = bounded_restore_preview(
      vec!["a".into()],
      vec!["b".into()],
      Vec::new(),
    );
    assert_ne!(
      expected.path_summary.unwrap().sha256,
      reclassified.path_summary.unwrap().sha256
    );
  }

  #[test]
  fn preview_text_limit_only_bounds_display_not_confirmation() {
    let paths = (0..600)
      .map(|index| format!("/{index:04}/{}", "x".repeat(2048)))
      .collect::<Vec<_>>();
    let preview = bounded_restore_preview(
      paths,
      vec!["/overwrite".into()],
      vec!["/delete".into()],
    );
    let summary = preview.path_summary.as_ref().unwrap();
    assert_eq!(summary.created, 600);
    assert!(preview.created_paths.len() < 600);
    let displayed_bytes: usize = preview
      .created_paths
      .iter()
      .chain(&preview.overwritten_paths)
      .chain(&preview.deleted_paths)
      .map(String::len)
      .sum();
    assert!(displayed_bytes <= MAX_RESTORE_PREVIEW_BYTES);
    assert_eq!(summary.overwritten, 1);
    assert_eq!(summary.deleted, 1);
  }

  #[test]
  fn complete_recovered_stack_can_exceed_display_row_limit() {
    let parent = tempfile::tempdir().unwrap();
    let destination = parent.path().join("recovered");
    let paths = (0..MAX_RESTORE_PREVIEW_ROWS + 1)
      .map(|index| komodo_backup::SnapshotPath {
        path: format!("root/{index:06}"),
        directory: false,
      })
      .collect::<Vec<_>>();
    let (created, overwritten, deleted) = compare_restore_paths(
      &paths,
      &[RestorePublishPath {
        destination_root: None,
        snapshot_path: "root".into(),
        destination: destination.to_string_lossy().into_owned(),
      }],
      &[],
      Instant::now() + RESTORE_PREFLIGHT_TIMEOUT,
    )
    .unwrap();
    let preview =
      bounded_restore_preview(created, overwritten, deleted);
    assert_eq!(
      preview.path_summary.unwrap().created,
      (MAX_RESTORE_PREVIEW_ROWS + 1) as u32
    );
    assert_eq!(preview.created_paths.len(), MAX_RESTORE_PREVIEW_ROWS);
    assert!(!destination.exists());
  }

  #[test]
  fn expired_preflight_fails_without_returning_a_partial_inventory() {
    let destination = tempfile::tempdir().unwrap();
    let publish = [RestorePublishPath {
      destination_root: None,
      snapshot_path: "root".into(),
      destination: destination.path().to_string_lossy().into_owned(),
    }];
    assert!(
      compare_restore_paths(&[], &publish, &[], Instant::now())
        .is_err()
    );
  }

  #[tokio::test]
  async fn preflight_deadline_covers_pending_discovery_and_releases_guards()
   {
    let filesystem = Arc::new(tokio::sync::RwLock::new(()));
    let guard = filesystem.clone().write_owned().await;
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let permit = slots.clone().try_acquire_owned().unwrap();
    let result =
      restore_preflight_before_deadline(Instant::now(), async move {
        let _guard = guard;
        let _permit = permit;
        // Model a Docker discovery future that never responds, before the
        // inventory worker exists.
        std::future::pending::<anyhow::Result<()>>().await
      })
      .await;
    assert!(
      result
        .unwrap_err()
        .to_string()
        .contains("exceeded 60 seconds")
    );
    assert!(filesystem.try_read().is_ok());
    assert_eq!(slots.available_permits(), 1);
  }

  #[tokio::test]
  async fn preflight_timeout_keeps_admission_until_inventory_worker_exits()
   {
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let permit = slots.clone().try_acquire_owned().unwrap();
    let (finish, finished) = tokio::sync::oneshot::channel::<()>();
    let mut worker = tokio::spawn(async move {
      let _permit = permit;
      finished.await.unwrap();
    });
    let result =
      restore_preflight_before_deadline(Instant::now(), async {
        (&mut worker).await?;
        Ok(())
      })
      .await;
    assert!(result.is_err());
    assert_eq!(slots.available_permits(), 0);
    finish.send(()).unwrap();
    worker.await.unwrap();
    assert_eq!(slots.available_permits(), 1);
  }

  #[test]
  fn failed_staging_cleanup_requires_recovery_even_after_cancellation()
   {
    for error in [Some(anyhow!("snapshot restore failed")), None] {
      let result = finish_restore_before_publication(
        error,
        Err(anyhow!("cannot remove staging journal")),
      );
      let RestoreTransactionResult::StagingCleanupFailed(error) =
        result
      else {
        panic!("A surviving staging journal must require recovery");
      };
      let message = format!("{error:#}");
      assert!(message.contains("restore staging cleanup failed"));
      assert!(message.contains("cannot remove staging journal"));
      assert!(
        message.contains("snapshot restore failed")
          || message.contains("cancelled")
      );
    }
  }

  #[test]
  fn successful_staging_cleanup_preserves_failure_or_proves_cancellation()
   {
    assert!(matches!(
      finish_restore_before_publication(
        Some(anyhow!("restore failed")),
        Ok(())
      ),
      RestoreTransactionResult::FailedBeforePublication(_)
    ));
    assert!(matches!(
      finish_restore_before_publication(None, Ok(())),
      RestoreTransactionResult::Published {
        rolled_back: true,
        finalization_pending: false
      }
    ));
  }

  #[test]
  fn destination_inventory_still_fails_closed_when_deadline_expires()
  {
    let destination = tempfile::tempdir().unwrap();
    std::fs::write(destination.path().join("extra"), b"extra")
      .unwrap();
    let mut budget =
      komodo_backup::RestoreInventoryBudget::new(Instant::now());
    let mut deleted = Vec::new();
    assert!(
      collect_unexpected_paths(
        destination.path(),
        &HashSet::new(),
        &mut deleted,
        &mut budget
      )
      .is_err()
    );
    assert!(deleted.is_empty());
    assert_eq!(
      std::fs::read(destination.path().join("extra")).unwrap(),
      b"extra"
    );
  }

  #[test]
  fn exact_restore_preflight_reports_create_overwrite_and_delete() {
    let destination = tempfile::tempdir().unwrap();
    std::fs::write(destination.path().join("old.txt"), b"old")
      .unwrap();
    std::fs::write(destination.path().join("extra.txt"), b"extra")
      .unwrap();
    let root = "source/root";
    let paths = vec![
      komodo_backup::SnapshotPath {
        path: root.into(),
        directory: true,
      },
      komodo_backup::SnapshotPath {
        path: format!("{root}/old.txt"),
        directory: false,
      },
      komodo_backup::SnapshotPath {
        path: format!("{root}/new.txt"),
        directory: false,
      },
    ];
    let publish = vec![RestorePublishPath {
      destination_root: None,
      snapshot_path: root.into(),
      destination: destination.path().to_string_lossy().into_owned(),
    }];
    let (created, overwritten, deleted) = compare_restore_paths(
      &paths,
      &publish,
      &[],
      Instant::now() + RESTORE_PREFLIGHT_TIMEOUT,
    )
    .unwrap();
    assert!(created.iter().any(|path| path.ends_with("new.txt")));
    assert!(overwritten.iter().any(|path| path.ends_with("old.txt")));
    assert!(deleted.iter().any(|path| path.ends_with("extra.txt")));
  }

  #[test]
  fn existing_empty_directory_is_an_overwrite() {
    let destination = tempfile::tempdir().unwrap();
    let (created, overwritten, deleted) = compare_restore_paths(
      &[komodo_backup::SnapshotPath {
        path: "root".into(),
        directory: true,
      }],
      &[RestorePublishPath {
        destination_root: None,
        snapshot_path: "root".into(),
        destination: destination.path().to_str().unwrap().into(),
      }],
      &[],
      Instant::now() + RESTORE_PREFLIGHT_TIMEOUT,
    )
    .unwrap();
    assert!(created.is_empty());
    assert_eq!(
      overwritten,
      vec![destination.path().to_str().unwrap()]
    );
    assert!(deleted.is_empty());
    assert_eq!(
      bounded_restore_preview(created, overwritten, deleted)
        .path_summary
        .unwrap()
        .overwritten,
      1
    );
  }

  #[cfg(unix)]
  #[test]
  fn non_utf8_destination_names_fail_before_returning_a_preview() {
    use std::os::unix::ffi::OsStringExt;
    let destination = tempfile::tempdir().unwrap();
    for byte in [0xfe, 0xff] {
      let name = std::ffi::OsString::from_vec(vec![b'x', byte]);
      std::fs::write(destination.path().join(name), b"keep").unwrap();
    }
    let result = compare_restore_paths(
      &[],
      &[RestorePublishPath {
        destination_root: None,
        snapshot_path: "root".into(),
        destination: destination.path().to_str().unwrap().into(),
      }],
      &[],
      Instant::now() + RESTORE_PREFLIGHT_TIMEOUT,
    );
    assert!(
      result.unwrap_err().to_string().contains("lossless UTF-8")
    );
    assert_eq!(
      std::fs::read_dir(destination.path()).unwrap().count(),
      2
    );
  }

  #[cfg(unix)]
  #[test]
  fn restore_preflight_never_follows_destination_symlinks() {
    let destination = tempfile::tempdir().unwrap();
    let unrelated = tempfile::tempdir().unwrap();
    std::fs::write(unrelated.path().join("private.txt"), b"private")
      .unwrap();
    std::fs::write(unrelated.path().join("restored.txt"), b"old")
      .unwrap();
    for nested in [false, true] {
      let publish_root = destination.path().join(if nested {
        "directory"
      } else {
        "root-link"
      });
      let link = if nested {
        std::fs::create_dir(&publish_root).unwrap();
        publish_root.join("nested-link")
      } else {
        publish_root.clone()
      };
      std::os::unix::fs::symlink(unrelated.path(), &link).unwrap();
      let snapshot_root = if nested {
        "source/root/nested-link"
      } else {
        "source/root"
      };
      let mut paths = vec![
        komodo_backup::SnapshotPath {
          path: snapshot_root.into(),
          directory: true,
        },
        komodo_backup::SnapshotPath {
          path: format!("{snapshot_root}/restored.txt"),
          directory: false,
        },
      ];
      if nested {
        paths.push(komodo_backup::SnapshotPath {
          path: "source/root".into(),
          directory: true,
        });
      }
      let publish = vec![RestorePublishPath {
        destination_root: None,
        snapshot_path: "source/root".into(),
        destination: publish_root.to_string_lossy().into_owned(),
      }];
      let (created, overwritten, deleted) = compare_restore_paths(
        &paths,
        &publish,
        &[],
        Instant::now() + RESTORE_PREFLIGHT_TIMEOUT,
      )
      .unwrap();
      assert_eq!(
        created,
        vec![
          link.join("restored.txt").to_string_lossy().into_owned()
        ]
      );
      let mut expected_overwritten =
        vec![link.to_str().unwrap().to_owned()];
      if nested {
        expected_overwritten
          .push(publish_root.to_str().unwrap().to_owned());
      }
      expected_overwritten.sort();
      assert_eq!(overwritten, expected_overwritten);
      assert!(deleted.is_empty());
    }
  }

  #[test]
  fn restore_verification_ignores_only_directory_storage_length() {
    for len in [0, 6, 4096, u64::MAX] {
      assert_eq!(restore_verification_len(true, len), 0);
      assert_eq!(restore_verification_len(false, len), len);
    }
  }

  #[test]
  fn publish_failure_restores_original_destination() {
    let root = tempfile::tempdir().unwrap();
    let download = root.path().join("download");
    std::fs::create_dir_all(download.join("one")).unwrap();
    std::fs::write(download.join("one/new.txt"), b"new").unwrap();
    std::fs::write(download.join("two.txt"), b"two").unwrap();
    let first = root.path().join("destination");
    std::fs::create_dir_all(&first).unwrap();
    std::fs::write(first.join("original.txt"), b"original").unwrap();
    let second = root.path().join("second");
    let publish = vec![
      RestorePublishPath {
        destination_root: None,
        snapshot_path: "one".into(),
        destination: first.to_string_lossy().into_owned(),
      },
      RestorePublishPath {
        destination_root: None,
        snapshot_path: "two.txt".into(),
        destination: second.to_string_lossy().into_owned(),
      },
    ];
    let journals = root.path().join("journals");
    std::fs::create_dir(&journals).unwrap();
    let journal_path = journals.join("rollback-test.json");
    let entries = publish
      .iter()
      .enumerate()
      .map(|(index, item)| {
        let source = root
          .path()
          .join(format!(".komodo-restore-rollback-test-{index}"));
        std::fs::rename(download.join(&item.snapshot_path), &source)
          .unwrap();
        let destination = PathBuf::from(&item.destination);
        RestoreJournalEntry {
          source,
          rollback: restore_rollback_path(
            &destination,
            "rollback-test",
          )
          .unwrap(),
          original_existed: Some(path_lexists(&destination)),
          destination,
          published: false,
        }
      })
      .collect();
    let mut journal = RestoreJournal {
      staging: download.clone(),
      entries,
      committed: false,
      finalized: false,
      deferred: false,
      completed: false,
      owned_volume: None,
    };
    validate_resolved_restore_destinations(&publish).unwrap();
    validate_restore_rollback_paths(&publish, "rollback-test")
      .unwrap();
    persist_journal(&journal_path, &journal).unwrap();
    let failure_injected = AtomicBool::new(false);
    assert!(
      publish_restore_entries(
        &mut journal,
        &journal_path,
        |from, to| {
          if to == second {
            assert!(first.join("new.txt").exists());
            assert!(!first.join("original.txt").exists());
            failure_injected.store(true, Ordering::SeqCst);
            return Err(std::io::Error::other(
              "injected publish failure",
            ));
          }
          std::fs::rename(from, to)
        }
      )
      .unwrap()
    );
    assert!(failure_injected.load(Ordering::SeqCst));
    assert_eq!(
      std::fs::read(first.join("original.txt")).unwrap(),
      b"original"
    );
    assert!(!first.join("new.txt").exists());
    assert!(!second.exists());
    assert!(!download.exists());
    assert!(!journals.join("rollback-test.json").exists());
    for index in 0..publish.len() {
      assert!(
        !root
          .path()
          .join(format!(".komodo-restore-rollback-test-{index}"))
          .exists()
      );
    }
  }

  #[test]
  fn overlapping_destinations_fail_before_publication() {
    let root = tempfile::tempdir().unwrap();
    let download = root.path().join("download");
    std::fs::create_dir_all(download.join("one")).unwrap();
    std::fs::write(download.join("one/new.txt"), b"new").unwrap();
    std::fs::write(download.join("two.txt"), b"two").unwrap();
    let first = root.path().join("destination");
    std::fs::create_dir(&first).unwrap();
    std::fs::write(first.join("original.txt"), b"original").unwrap();
    let publish = [
      RestorePublishPath {
        destination_root: None,
        snapshot_path: "one".into(),
        destination: first.to_string_lossy().into_owned(),
      },
      RestorePublishPath {
        destination_root: None,
        snapshot_path: "two.txt".into(),
        destination: first
          .join("child.txt")
          .to_string_lossy()
          .into_owned(),
      },
    ];
    let publication_started = AtomicBool::new(false);
    let journals = root.path().join("journals");
    let error = publish_restore_in(
      &download,
      &publish,
      "overlap-test",
      &publication_started,
      &journals,
      None,
      false,
    )
    .unwrap_err();
    assert!(
      error.to_string().contains("Restore destinations overlap")
    );
    assert!(!publication_started.load(Ordering::SeqCst));
    assert_eq!(
      std::fs::read(first.join("original.txt")).unwrap(),
      b"original"
    );
    assert!(!first.join("new.txt").exists());
    assert!(!first.join("child.txt").exists());
    assert!(download.join("one/new.txt").exists());
    assert!(!journals.exists());
    assert!(root.path().read_dir().unwrap().all(|entry| {
      !entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with(".komodo-restore-")
    }));
  }

  #[test]
  fn deferred_publication_retains_rollback_until_finalized() {
    let root = tempfile::tempdir().unwrap();
    let download = root.path().join("download");
    std::fs::create_dir(&download).unwrap();
    std::fs::write(download.join("new.txt"), b"new").unwrap();
    let destination = root.path().join("destination.txt");
    std::fs::write(&destination, b"original").unwrap();
    let publish = [RestorePublishPath {
      destination_root: None,
      snapshot_path: "new.txt".into(),
      destination: destination.to_string_lossy().into_owned(),
    }];
    let journal_directory = root.path().join("journals");
    assert!(
      !publish_restore_in(
        &download,
        &publish,
        "deferred-test",
        &AtomicBool::new(false),
        &journal_directory,
        None,
        true,
      )
      .unwrap()
    );
    assert_eq!(std::fs::read(&destination).unwrap(), b"new");
    let journal_path = journal_directory.join("deferred-test.json");
    let mut journal: RestoreJournal =
      serde_json::from_slice(&std::fs::read(&journal_path).unwrap())
        .unwrap();
    assert!(!journal.committed);
    assert!(journal.deferred);
    assert!(!journal.completed);
    assert!(journal.entries[0].rollback.exists());
    rollback_published(&mut journal, &journal_path).unwrap();
    assert_eq!(std::fs::read(&destination).unwrap(), b"original");
  }

  #[test]
  fn repeated_recovery_preserves_an_already_restored_original() {
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("destination");
    std::fs::write(&destination, b"original").unwrap();
    let journal_path = root.path().join("journal.json");
    let mut journal = RestoreJournal {
      staging: root.path().join("staging"),
      entries: vec![RestoreJournalEntry {
        source: root.path().join("source"),
        destination: destination.clone(),
        rollback: root.path().join("rollback"),
        original_existed: Some(true),
        // Simulate a crash after rollback -> destination was synced but before
        // this flag was durably cleared.
        published: true,
      }],
      committed: false,
      finalized: false,
      deferred: false,
      completed: false,
      owned_volume: None,
    };
    rollback_published(&mut journal, &journal_path).unwrap();
    assert_eq!(std::fs::read(destination).unwrap(), b"original");
    assert!(!journal.entries[0].published);
  }

  #[test]
  fn rolled_back_restore_retains_created_volume_ownership() {
    let root = tempfile::tempdir().unwrap();
    let journal_path = root.path().join("journal.json");
    let journal = RestoreJournal {
      staging: root.path().join("staging"),
      entries: Vec::new(),
      committed: false,
      finalized: false,
      deferred: false,
      completed: false,
      owned_volume: Some(RestoreOwnedVolume {
        volume_name: "recovered-data".into(),
        restore_plan_id: "plan-id".into(),
      }),
    };
    std::fs::create_dir(&journal.staging).unwrap();

    cleanup_rolled_back_restore(&journal, &journal_path).unwrap();

    let retained: RestoreJournal =
      serde_json::from_slice(&std::fs::read(&journal_path).unwrap())
        .unwrap();
    assert!(retained.staging.as_os_str().is_empty());
    assert!(retained.entries.is_empty());
    assert_eq!(
      retained
        .owned_volume
        .as_ref()
        .map(|owned| owned.volume_name.as_str()),
      Some("recovered-data")
    );
  }

  #[test]
  fn rollback_names_preserve_complete_destination_filenames() {
    let root = tempfile::tempdir().unwrap();
    let download = root.path().join("download");
    std::fs::create_dir_all(&download).unwrap();
    std::fs::write(download.join("new-json"), b"new-json").unwrap();
    std::fs::write(download.join("new-yaml"), b"new-yaml").unwrap();
    let json = root.path().join("app.json");
    let yaml = root.path().join("app.yaml");
    assert_ne!(
      restore_rollback_path(&json, "unique-rollback-test").unwrap(),
      restore_rollback_path(&yaml, "unique-rollback-test").unwrap()
    );
    std::fs::write(&json, b"old-json").unwrap();
    std::fs::write(&yaml, b"old-yaml").unwrap();
    let publish = vec![
      RestorePublishPath {
        destination_root: None,
        snapshot_path: "new-json".into(),
        destination: json.to_string_lossy().into_owned(),
      },
      RestorePublishPath {
        destination_root: None,
        snapshot_path: "new-yaml".into(),
        destination: yaml.to_string_lossy().into_owned(),
      },
    ];
    assert!(
      !publish_restore_in(
        &download,
        &publish,
        "unique-rollback-test",
        &AtomicBool::new(false),
        &root.path().join("journals"),
        None,
        false,
      )
      .unwrap()
    );
    assert_eq!(std::fs::read(json).unwrap(), b"new-json");
    assert_eq!(std::fs::read(yaml).unwrap(), b"new-yaml");
  }

  #[test]
  fn preparation_errors_remove_same_filesystem_staging() {
    let root = tempfile::tempdir().unwrap();
    let download = root.path().join("download");
    std::fs::create_dir_all(&download).unwrap();
    std::fs::write(download.join("present"), b"present").unwrap();
    let publish = vec![
      RestorePublishPath {
        destination_root: None,
        snapshot_path: "present".into(),
        destination: root
          .path()
          .join("first")
          .to_string_lossy()
          .into_owned(),
      },
      RestorePublishPath {
        destination_root: None,
        snapshot_path: "missing".into(),
        destination: root
          .path()
          .join("second")
          .to_string_lossy()
          .into_owned(),
      },
    ];
    assert!(
      publish_restore_in(
        &download,
        &publish,
        "prepare-cleanup-test",
        &AtomicBool::new(false),
        &root.path().join("journals"),
        None,
        false,
      )
      .is_err()
    );
    assert!(
      !root
        .path()
        .join(".komodo-restore-prepare-cleanup-test-0")
        .exists()
    );
  }

  #[test]
  fn destination_existence_changes_are_detected_before_publish() {
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("destination");
    let entry = RestoreJournalEntry {
      source: root.path().join("source"),
      destination: destination.clone(),
      rollback: root.path().join("rollback"),
      original_existed: Some(false),
      published: false,
    };
    assert!(destination_existence_matches(&entry));
    std::fs::write(destination, b"concurrent data").unwrap();
    assert!(!destination_existence_matches(&entry));
  }

  #[test]
  fn staging_journal_removes_all_owned_restore_paths() {
    let root = tempfile::tempdir().unwrap();
    let staging = root.path().join("staging");
    let destination_copy = root.path().join("destination-copy");
    std::fs::create_dir_all(&staging).unwrap();
    std::fs::write(&destination_copy, b"prepared").unwrap();
    let journal_path = root.path().join("staging.json");
    persist_journal(
      &journal_path,
      &RestoreStagingJournal {
        paths: vec![staging.clone(), destination_copy.clone()],
      },
    )
    .unwrap();
    cleanup_restore_staging_journal(&journal_path).unwrap();
    assert!(!staging.exists());
    assert!(!destination_copy.exists());
    assert!(!journal_path.exists());
  }

  #[test]
  fn staging_recovery_preserves_ownership_if_its_parent_is_unavailable()
   {
    let root = tempfile::tempdir().unwrap();
    let volume = root.path().join("volumes/created-volume");
    let staging = volume.join(".komodo-restore-id");
    std::fs::create_dir_all(&staging).unwrap();
    let journal = root.path().join("staging.json");
    persist_journal(
      &journal,
      &RestoreStagingJournal {
        paths: vec![staging.clone()],
      },
    )
    .unwrap();
    std::fs::remove_dir_all(&volume).unwrap();
    assert!(cleanup_restore_staging_journal(&journal).is_err());
    assert!(journal.exists());
    assert!(!volume.exists());
  }

  #[test]
  fn child_staging_cleanup_completes_before_its_volume_parent_is_removed()
   {
    let root = tempfile::tempdir().unwrap();
    let volume = root.path().join("created-volume");
    let staging = volume.join(".komodo-restore-id");
    std::fs::create_dir_all(&staging).unwrap();
    let journal = root.path().join("staging.json");
    persist_journal(
      &journal,
      &RestoreStagingJournal {
        paths: vec![staging.clone()],
      },
    )
    .unwrap();
    cleanup_restore_staging_journal(&journal).unwrap();
    remove_owned_staging_path(&staging).unwrap();
    assert!(!journal.exists());
    std::fs::remove_dir_all(&volume).unwrap();
  }

  #[test]
  fn staging_cleanup_does_not_swallow_non_missing_parent_errors() {
    let root = tempfile::tempdir().unwrap();
    let parent = root.path().join("not-a-directory");
    std::fs::write(&parent, b"owned by someone else").unwrap();
    assert!(
      remove_owned_staging_path(&parent.join("staging")).is_err()
    );
    assert_eq!(
      std::fs::read(parent).unwrap(),
      b"owned by someone else"
    );
  }

  #[test]
  fn repeated_quiesce_attempts_preserve_every_pending_container() {
    assert_eq!(
      merge_container_quiesce_sets(
        &["original".into(), "shared".into()],
        &["retry".into(), "shared".into()],
      ),
      vec![
        "original".to_string(),
        "retry".to_string(),
        "shared".to_string(),
      ]
    );
  }

  #[test]
  fn backup_and_restore_quiescing_include_named_volume_mounts() {
    assert!(mount_type_affects_paths(Some("bind")));
    assert!(mount_type_affects_paths(Some("volume")));
    assert!(!mount_type_affects_paths(Some("tmpfs")));
    assert!(!mount_type_affects_paths(None));
  }

  #[test]
  fn volume_quiescing_includes_bind_sources_above_at_and_below_storage()
   {
    let paths = BTreeSet::from([PathBuf::from(
      "/docker-data/volumes/data/_data",
    )]);
    for source in [
      "/docker-data/volumes/data/_data",
      "/docker-data/volumes/data/_data/database",
      "/docker-data/volumes/data",
    ] {
      assert!(
        mount_affects_paths(
          Some("bind"),
          Some(source),
          &paths,
          &[],
          false,
        )
        .unwrap()
      );
      assert!(
        mount_affects_paths(
          Some("volume"),
          Some(source),
          &paths,
          &[],
          false,
        )
        .unwrap()
      );
    }
    for source in [
      "/docker-data/volumes/other/_data",
      "/docker-data/volumes/data/_data-other",
    ] {
      assert!(
        !mount_affects_paths(
          Some("bind"),
          Some(source),
          &paths,
          &[],
          false,
        )
        .unwrap()
      );
    }
    assert!(
      !mount_affects_paths(
        Some("tmpfs"),
        Some("/docker-data"),
        &paths,
        &[],
        false,
      )
      .unwrap()
    );
    assert!(
      !mount_affects_paths(Some("bind"), None, &paths, &[], false)
        .unwrap()
    );
  }

  #[test]
  fn quiesce_translates_docker_paths_through_the_verified_worker_mounts()
   {
    use komodo_client::entities::docker::container::MountPoint;
    let root = tempfile::tempdir().unwrap();
    let local = root.path().join("stacks");
    std::fs::create_dir_all(local.join("app")).unwrap();
    let mounts = [MountPoint {
      source: Some("/host/docker/volumes/stacks/_data".into()),
      destination: Some(local.to_string_lossy().into_owned()),
      ..Default::default()
    }];
    assert!(
      mount_affects_paths(
        Some("volume"),
        Some("/host/docker/volumes/stacks/_data"),
        &BTreeSet::from([local.join("app")]),
        &mounts,
        false
      )
      .unwrap()
    );
  }

  #[test]
  fn restore_quiesces_the_symlink_entry_writer_not_just_its_referent()
  {
    let root = tempfile::tempdir().unwrap();
    let app = root.path().join("app");
    let external = root.path().join("external");
    std::fs::create_dir_all(&app).unwrap();
    std::fs::create_dir_all(&external).unwrap();
    std::fs::write(external.join("config"), b"original").unwrap();
    std::os::unix::fs::symlink(
      external.join("config"),
      app.join("config"),
    )
    .unwrap();
    let paths = BTreeSet::from([app.join("config")]);
    assert!(
      mount_affects_paths(
        Some("bind"),
        app.to_str(),
        &paths,
        &[],
        true
      )
      .unwrap()
    );
    assert!(
      !mount_affects_paths(
        Some("bind"),
        external.to_str(),
        &paths,
        &[],
        true
      )
      .unwrap()
    );
    assert!(
      mount_affects_paths(
        Some("bind"),
        external.to_str(),
        &paths,
        &[],
        false
      )
      .unwrap()
    );
  }

  #[test]
  fn quiescing_excludes_only_the_current_worker_identity() {
    let worker = ContainerListItem {
      id: Some("a".repeat(64)),
      state: ContainerStateStatusEnum::Running,
      ..Default::default()
    };
    assert!(!container_is_quiesce_candidate(
      &worker,
      Some(&"a".repeat(64))
    ));
    assert!(container_is_quiesce_candidate(
      &worker,
      Some(&"b".repeat(64))
    ));
    assert!(container_is_quiesce_candidate(&worker, None));
    assert!(container_is_quiesce_candidate(
      &worker,
      Some("periphery")
    ));
    let stopped = ContainerListItem {
      state: ContainerStateStatusEnum::Exited,
      ..worker
    };
    assert!(!container_is_quiesce_candidate(&stopped, None));
  }

  #[test]
  fn missing_volume_preview_is_relative_and_does_not_inspect_host_paths()
   {
    let publish = vec![RestorePublishPath {
      destination_root: None,
      snapshot_path: "snapshot/data".into(),
      destination: "/var/lib/docker/volumes/new-volume/_data".into(),
    }];
    let items = vec![komodo_backup::SnapshotPath {
      path: "snapshot/data/config.toml".into(),
      directory: false,
    }];
    let (created, overwritten, deleted) =
      compare_missing_volume_paths(
        &items,
        &publish,
        "new-volume",
        Instant::now() + RESTORE_PREFLIGHT_TIMEOUT,
      )
      .unwrap();
    assert_eq!(created, ["volume://new-volume/config.toml"]);
    assert!(overwritten.is_empty());
    assert!(deleted.is_empty());
    let outside = vec![RestorePublishPath {
      destination: "/etc".into(),
      ..publish[0].clone()
    }];
    assert!(
      compare_missing_volume_paths(
        &items,
        &outside,
        "new-volume",
        Instant::now() + RESTORE_PREFLIGHT_TIMEOUT
      )
      .is_err()
    );
  }
}
