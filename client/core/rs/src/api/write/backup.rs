use mogh_resolver::Resolve;
use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::entities::backup::{
  BackupRestorePlan, BackupRun, BackupSettings, BackupTarget,
  CoreRecoveryPlan,
};

use super::KomodoWriteRequest;

/// Admin-only settings update. Empty submitted secret values preserve the
/// existing sealed value.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoWriteRequest)]
#[response(BackupSettings)]
#[error(mogh_error::Error)]
pub struct UpdateBackupSettings {
  pub settings: BackupSettings,
}

/// Admin-only repository initialization and connectivity check.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoWriteRequest)]
#[response(BackupRun)]
#[error(mogh_error::Error)]
pub struct InitializeBackupRepositories {}

/// Start one resource backup, or a full fleet cycle when target is omitted.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoWriteRequest)]
#[response(BackupRun)]
#[error(mogh_error::Error)]
pub struct RunBackup {
  pub target: Option<BackupTarget>,
}

/// Produce an exact-restore preflight. No files or containers are changed.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoWriteRequest)]
#[response(BackupRestorePlan)]
#[error(mogh_error::Error)]
pub struct PlanBackupRestore {
  pub snapshot: String,
  pub destination_server_id: Option<String>,
  /// Empty selects the full snapshot.
  #[serde(default)]
  pub selected_paths: Vec<String>,
  /// Required for cross-node stack restore.
  pub recovered_stack_name: Option<String>,
  /// Source absolute bind path to destination absolute bind path.
  #[serde(default)]
  pub bind_path_mappings: std::collections::HashMap<String, String>,
  /// Required for cross-node volume restore.
  pub destination_volume_name: Option<String>,
  #[serde(default)]
  pub confirm_existing_volume: bool,
}

/// Execute a non-expired, previously confirmed restore plan.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoWriteRequest)]
#[response(BackupRun)]
#[error(mogh_error::Error)]
pub struct ExecuteBackupRestore {
  pub plan_id: String,
}

#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoWriteRequest)]
#[response(BackupRun)]
#[error(mogh_error::Error)]
pub struct VerifyBackupRepository {
  #[serde(default)]
  pub mirror: bool,
  #[serde(default)]
  pub full: bool,
}

/// Admin-only. Performs full verification before changing the active primary.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoWriteRequest)]
#[response(BackupSettings)]
#[error(mogh_error::Error)]
pub struct PromoteBackupMirror {}

#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoWriteRequest)]
#[response(BackupRun)]
#[error(mogh_error::Error)]
pub struct CancelBackupRun {
  pub run_id: String,
}

/// Admin-only. Restore and validate a Core snapshot without switching databases.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoWriteRequest)]
#[response(CoreRecoveryPlan)]
#[error(mogh_error::Error)]
pub struct PlanCoreRecovery {
  pub snapshot: String,
}

/// Admin-only. Persist the validated database pointer and restart Core.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoWriteRequest)]
#[response(BackupRun)]
#[error(mogh_error::Error)]
pub struct ExecuteCoreRecovery {
  pub plan_id: String,
}
