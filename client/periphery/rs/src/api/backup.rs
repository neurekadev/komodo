use komodo_client::entities::{
  backup::{BackupAdvancedSettings, BackupRepository},
  repo::Repo,
  stack::Stack,
};
use mogh_resolver::Resolve;
use serde::{Deserialize, Serialize};

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

#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[response(DiscoverBackupSourceResponse)]
#[error(anyhow::Error)]
pub struct DiscoverBackupSource {
  pub target: PeripheryBackupTarget,
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
  #[serde(default)]
  pub stop_containers: bool,
  /// Retry only the mirror for a snapshot already committed to primary.
  #[serde(default)]
  pub mirror_only: bool,
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
  pub advanced: BackupAdvancedSettings,
  pub hostname: String,
  pub snapshot_name: String,
  #[serde(default)]
  pub selected_paths: Vec<String>,
  /// Snapshot-relative path to absolute destination path.
  pub publish: Vec<RestorePublishPath>,
  pub journal_id: String,
  /// Stable restore-plan identity used to recognize a Volume created by an
  /// earlier execution attempt of the same plan.
  #[serde(default)]
  pub volume_restore_plan_id: String,
  /// Create a local destination volume when it does not exist.
  #[serde(default)]
  pub create_volume_if_missing: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RestorePublishPath {
  pub snapshot_path: String,
  pub destination: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[response(PreflightVykarRestoreResponse)]
#[error(anyhow::Error)]
pub struct PreflightVykarRestore {
  pub target: PeripheryBackupTarget,
  pub repository: BackupRepository,
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

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct TransactionalVykarRestoreResponse {
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
