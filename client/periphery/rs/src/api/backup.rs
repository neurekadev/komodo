use komodo_client::entities::{
  backup::{BackupAdvancedSettings, BackupRepository},
  repo::Repo,
  stack::Stack,
};
use mogh_resolver::Resolve;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProtectedRepositoryPath {
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
  /// Core-local repository paths as mounted inside Core. Periphery resolves
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
  pub target: PeripheryBackupTarget,
  pub primary: BackupRepository,
  pub mirror: Option<BackupRepository>,
  pub advanced: BackupAdvancedSettings,
  pub hostname: String,
  pub source_label: String,
  pub snapshot_name: String,
  pub run_id: String,
  pub komodo_version: String,
  /// Core-local repository paths as mounted inside Core. Periphery resolves
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
  /// Prior attempts that may still be the newest complete copy for an
  /// individual repository. Copies are retired independently as that role
  /// commits a newer attempt.
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
  pub tasks: Vec<VykarBackupTask>,
  pub primary: BackupRepository,
  pub mirror: Option<BackupRepository>,
  pub advanced: BackupAdvancedSettings,
  pub hostname: String,
  pub run_id: String,
  pub komodo_version: String,
  /// Core-local repository paths as mounted inside Core. Periphery resolves
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
#[response(TransactionalVykarRestoreResponse)]
#[error(anyhow::Error)]
pub struct TransactionalVykarRestore {
  pub target: PeripheryBackupTarget,
  pub repository: BackupRepository,
  /// Core-local repository paths as mounted inside Core. Periphery resolves
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
  /// Core-local repository paths as mounted inside Core. Periphery resolves
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
  pub journal_id: String,
  /// Commit the publication when true; restore the original filesystem when
  /// false.
  pub commit: bool,
  /// Remove the durable finalization receipt after Core has persisted the
  /// matching cross-system outcome. Repeating an acknowledgement is safe.
  #[serde(default)]
  pub acknowledge: bool,
}

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
