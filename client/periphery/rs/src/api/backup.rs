use komodo_client::entities::{
  backup::{
    BackupAdvancedSettings, BackupRepository,
    BackupRestorePathSummary,
  },
  docker::volume::VolumeListItem,
  repo::Repo,
  stack::Stack,
};
use mogh_resolver::Resolve;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Strict, read-only Docker inventory for backups. Unlike dashboard polling,
/// missing Docker or failed container/volume lists are errors, never empty lists.
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[response(Vec<VolumeListItem>)]
#[error(anyhow::Error)]
pub struct GetBackupVolumeInventory {}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProtectedRepositoryPath {
  /// Core-private or repository storage that no worker may capture or restore.
  pub path: String,
  /// Identity read inside Core, not an inferred application container name.
  pub core_container_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", content = "params")]
pub enum PeripheryBackupTarget {
  Stack {
    stack: Box<Stack>,
    repo: Option<Box<Repo>>,
  },
  Volume {
    volume_name: String,
  },
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct BackupSourceFilters {
  /// Traverse mount boundaries beneath a source and include Stack bind roots
  /// stored on a different filesystem. Disabled by default.
  #[serde(default)]
  pub include_cross_filesystem_mounts: bool,
  /// Include Docker volumes with daemon-generated anonymous names.
  #[serde(default)]
  pub include_anonymous_volumes: bool,
  /// Vykar/gitignore-style rules selecting absolute Stack bind source paths.
  #[serde(default)]
  pub bind_mount_include_patterns: Vec<String>,
  /// Vykar/gitignore-style rules excluding absolute Stack bind source paths.
  #[serde(default)]
  pub bind_mount_exclude_patterns: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[response(DiscoverBackupSourceResponse)]
#[error(anyhow::Error)]
pub struct DiscoverBackupSource {
  pub target: PeripheryBackupTarget,
  #[serde(default)]
  pub filters: BackupSourceFilters,
  /// Core-private and repository paths inside Core. Periphery resolves
  /// their Docker mount sources and refuses to capture those sources.
  #[serde(default)]
  pub protected_repository_paths: Vec<ProtectedRepositoryPath>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct DiscoverBackupSourceResponse {
  pub paths: Vec<String>,
  pub running_containers: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[response(RunVykarBackupResponse)]
#[error(anyhow::Error)]
pub struct RunVykarBackup {
  /// Unique dispatch identity. Replays never start the operation again.
  #[serde(default)]
  pub operation_id: String,
  pub target: PeripheryBackupTarget,
  pub primary: BackupRepository,
  pub mirror: Option<BackupRepository>,
  pub advanced: BackupAdvancedSettings,
  pub hostname: String,
  pub source_label: String,
  pub snapshot_name: String,
  pub run_id: String,
  pub komodo_version: String,
  /// Core-private and repository paths inside Core. Periphery resolves
  /// their Docker mount sources and refuses to capture those sources.
  #[serde(default)]
  pub protected_repository_paths: Vec<ProtectedRepositoryPath>,
  #[serde(default)]
  pub filters: BackupSourceFilters,
  #[serde(default)]
  pub stop_containers: bool,
  /// Retry only the mirror for a snapshot already committed to primary.
  #[serde(default)]
  pub mirror_only: bool,
  /// Retry only the primary for a snapshot already committed to the mirror.
  #[serde(default)]
  pub primary_only: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct VykarBackupRepositoryResult {
  pub complete: bool,
  pub partial: bool,
  pub files: u64,
  pub original_size: u64,
  pub stored_size: u64,
  pub error: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct RunVykarBackupResponse {
  /// A deliberate safety exclusion, not a failed write or a retryable error.
  #[serde(default)]
  pub excluded: Option<String>,
  pub primary: VykarBackupRepositoryResult,
  pub mirror: Option<VykarBackupRepositoryResult>,
  pub stopped_containers: Vec<String>,
  pub restarted_containers: Vec<String>,
  pub restart_errors: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VykarBackupTask {
  pub target: PeripheryBackupTarget,
  pub source_label: String,
  pub snapshot_name: String,
  /// Retry only the mirror for a snapshot already committed to primary.
  #[serde(default)]
  pub mirror_only: bool,
  /// Retry only the primary for a snapshot already committed to the mirror.
  #[serde(default)]
  pub primary_only: bool,
  /// A prior asymmetric attempt that this fresh, node-quiesced attempt
  /// replaces once at least one repository commits the new snapshot.
  #[serde(default)]
  pub superseded_snapshot_names: Vec<String>,
  /// Prior attempts that may still be the newest complete or diagnostic partial
  /// copy for a repository. Copies are retired independently once that role
  /// commits a complete replacement, or by normal retention after retries end.
  #[serde(default)]
  pub retained_snapshots: Vec<VykarRetainedSnapshot>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct VykarRetainedSnapshot {
  pub snapshot_name: String,
  #[serde(default)]
  pub retain_primary: bool,
  #[serde(default)]
  pub retain_mirror: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[response(RunVykarBackupBatchResponse)]
#[error(anyhow::Error)]
pub struct RunVykarBackupBatch {
  /// Unique dispatch identity, independent of the fleet cancellation run ID.
  #[serde(default)]
  pub operation_id: String,
  pub tasks: Vec<VykarBackupTask>,
  pub primary: BackupRepository,
  pub mirror: Option<BackupRepository>,
  pub advanced: BackupAdvancedSettings,
  pub hostname: String,
  pub run_id: String,
  pub komodo_version: String,
  /// Core-private and repository paths inside Core. Periphery resolves
  /// their Docker mount sources and refuses to capture those sources.
  #[serde(default)]
  pub protected_repository_paths: Vec<ProtectedRepositoryPath>,
  #[serde(default)]
  pub filters: BackupSourceFilters,
  #[serde(default)]
  pub stop_containers: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VykarBackupTaskResult {
  pub source_label: String,
  pub result: RunVykarBackupResponse,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct RunVykarBackupBatchResponse {
  pub results: Vec<VykarBackupTaskResult>,
  pub discovery_errors: Vec<String>,
  pub restart_errors: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[response(VykarBackupCompletion)]
#[error(anyhow::Error)]
pub struct GetVykarBackupCompletion {
  pub operation_id: String,
  /// Must match the original dispatch and authenticated Core owner.
  pub run_id: String,
  /// Fence a not-yet-started dispatch after its transport result was lost.
  #[serde(default)]
  pub cancel_if_unknown: bool,
  /// Discard the bulky result, but retain a terminal replay-prevention marker.
  #[serde(default)]
  pub acknowledge: bool,
}

#[derive(
  Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq,
)]
pub enum VykarBackupCompletionState {
  #[default]
  Unknown,
  Running,
  /// Resolver exited, but Core must commit or roll back its prepared saga.
  Prepared,
  /// Original work exited; unresolved journals still require guarded recovery.
  RecoveryRequired,
  Complete,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct VykarBackupCompletion {
  pub state: VykarBackupCompletionState,
  pub result: Option<RunVykarBackupResponse>,
  pub batch_result: Option<RunVykarBackupBatchResponse>,
  #[serde(default)]
  pub restore_result: Option<TransactionalVykarRestoreResponse>,
  #[serde(default)]
  pub finalize_restore_result: Option<FinalizeVykarRestoreResponse>,
  pub error: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[response(TransactionalVykarRestoreResponse)]
#[error(anyhow::Error)]
pub struct TransactionalVykarRestore {
  #[serde(default)]
  pub operation_id: String,
  #[serde(default)]
  pub run_id: String,
  pub target: PeripheryBackupTarget,
  pub repository: BackupRepository,
  /// Core-private and repository paths inside Core. Periphery resolves
  /// their Docker mount sources and refuses to publish into those sources.
  #[serde(default)]
  pub protected_repository_paths: Vec<ProtectedRepositoryPath>,
  pub advanced: BackupAdvancedSettings,
  pub hostname: String,
  pub snapshot_name: String,
  #[serde(default)]
  pub selected_paths: Vec<String>,
  /// Snapshot-relative path to absolute destination path.
  pub publish: Vec<RestorePublishPath>,
  /// Confirmed preview, revalidated under the destination's filesystem guard
  /// before creating volumes, stopping containers, or publishing files.
  pub expected_preview: PreflightVykarRestoreResponse,
  pub journal_id: String,
  /// Stable restore-plan identity used to recognize a Volume created by an
  /// earlier execution attempt of the same plan.
  #[serde(default)]
  pub volume_restore_plan_id: String,
  /// Create a local destination volume when it does not exist.
  #[serde(default)]
  pub create_volume_if_missing: bool,
  /// Source absolute bind path to destination absolute bind path. Recovered
  /// Stack Compose files are rewritten in staging before publication.
  #[serde(default)]
  pub bind_path_mappings: HashMap<String, String>,
  /// Original absolute Compose bind source to its canonical snapshot path.
  /// This preserves symlink aliases when recovery happens on another node.
  #[serde(default)]
  pub bind_path_aliases: HashMap<String, String>,
  /// Keep the durable filesystem rollback journal until Core confirms the
  /// recovered Stack resource was inserted successfully.
  #[serde(default)]
  pub defer_finalize: bool,
}

/// Separate wire name prevents older workers from executing an unreceipted
/// restore and then reporting the new operation identity as unknown.
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[serde(transparent)]
#[response(TransactionalVykarRestoreResponse)]
#[error(anyhow::Error)]
pub struct RunTransactionalVykarRestore(
  pub TransactionalVykarRestore,
);

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RestorePublishPath {
  /// Original confirmed root for a selected child. Ancestors beneath this
  /// boundary must not be followed through symlinks during publication.
  #[serde(default)]
  pub destination_root: Option<String>,
  pub snapshot_path: String,
  pub destination: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[response(PreflightVykarRestoreResponse)]
#[error(anyhow::Error)]
pub struct PreflightVykarRestore {
  pub target: PeripheryBackupTarget,
  pub repository: BackupRepository,
  /// Core-private and repository paths inside Core. Periphery resolves
  /// their Docker mount sources and refuses to publish into those sources.
  #[serde(default)]
  pub protected_repository_paths: Vec<ProtectedRepositoryPath>,
  pub advanced: BackupAdvancedSettings,
  pub hostname: String,
  pub snapshot_name: String,
  #[serde(default)]
  pub selected_paths: Vec<String>,
  pub publish: Vec<RestorePublishPath>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct PreflightVykarRestoreResponse {
  /// Whether the requested resource already exists at the destination.
  pub destination_exists: bool,
  pub created_paths: Vec<String>,
  pub overwritten_paths: Vec<String>,
  pub deleted_paths: Vec<String>,
  /// Complete change-set confirmation; the path lists are bounded samples.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub path_summary: Option<BackupRestorePathSummary>,
  pub containers_to_stop: Vec<String>,
}

impl PreflightVykarRestoreResponse {
  pub fn matches(&self, other: &Self) -> bool {
    fn same(left: &[String], right: &[String]) -> bool {
      let mut left = left.to_vec();
      let mut right = right.to_vec();
      left.sort();
      right.sort();
      left == right
    }
    self.destination_exists == other.destination_exists
      && self.path_summary == other.path_summary
      && same(&self.created_paths, &other.created_paths)
      && same(&self.overwritten_paths, &other.overwritten_paths)
      && same(&self.deleted_paths, &other.deleted_paths)
      && same(&self.containers_to_stop, &other.containers_to_stop)
  }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct TransactionalVykarRestoreResponse {
  pub complete: bool,
  pub rolled_back: bool,
  /// Publication succeeded, but its rollback data is intentionally retained
  /// until `FinalizeVykarRestore` commits or rolls it back.
  #[serde(default)]
  pub finalization_pending: bool,
  pub containers_restarted: Vec<String>,
  pub critical_error: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[response(FinalizeVykarRestoreResponse)]
#[error(anyhow::Error)]
pub struct FinalizeVykarRestore {
  #[serde(default)]
  pub operation_id: String,
  #[serde(default)]
  pub run_id: String,
  /// Identity of the original restore dispatch, not this finalization RPC.
  #[serde(default)]
  pub restore_operation_id: String,
  pub journal_id: String,
  /// Commit the publication when true; restore the original filesystem when
  /// false.
  pub commit: bool,
  /// Remove the durable finalization receipt after Core has persisted the
  /// matching cross-system outcome. Repeating an acknowledgement is safe.
  #[serde(default)]
  pub acknowledge: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[serde(transparent)]
#[response(FinalizeVykarRestoreResponse)]
#[error(anyhow::Error)]
pub struct RunFinalizeVykarRestore(pub FinalizeVykarRestore);

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct FinalizeVykarRestoreResponse {
  pub complete: bool,
  pub rolled_back: bool,
  pub containers_restarted: Vec<String>,
  pub critical_error: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[response(CancelVykarOperationResponse)]
#[error(anyhow::Error)]
pub struct CancelVykarOperation {
  pub operation_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct CancelVykarOperationResponse {
  pub cancelled: bool,
}
