use mogh_resolver::Resolve;
use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::entities::U64;
use crate::entities::backup::{
  BackupSettings, BackupSnapshot, BackupSnapshotItem, BackupStatus,
  BackupTarget,
};

use super::KomodoReadRequest;

/// Get the singleton configuration. Repository secrets are always redacted.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoReadRequest)]
#[response(BackupSettings)]
#[error(mogh_error::Error)]
pub struct GetBackupSettings {}

#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoReadRequest)]
#[response(BackupStatus)]
#[error(mogh_error::Error)]
pub struct GetBackupStatus {}

#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct BackupSnapshotList {
  pub snapshots: Vec<BackupSnapshot>,
  pub total: U64,
  /// Any non-zero value means Vykar could not decode part of the inventory.
  pub hidden: U64,
}

/// List snapshots from the active primary repository. The target is optional
/// for administrators and required for resource-scoped users.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoReadRequest)]
#[response(BackupSnapshotList)]
#[error(mogh_error::Error)]
pub struct ListBackupSnapshots {
  pub target: Option<BackupTarget>,
  #[serde(default)]
  pub page: U64,
  #[serde(default = "default_page_limit")]
  pub limit: U64,
}

fn default_page_limit() -> U64 {
  100
}

#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct BackupSnapshotDirectory {
  pub entries: Vec<BackupSnapshotItem>,
  pub total: U64,
  pub page: U64,
  pub has_more: bool,
}

/// Lazily load one directory from a snapshot. The picker starts collapsed by
/// requesting only the root, then loads children as folders are expanded.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoReadRequest)]
#[response(BackupSnapshotDirectory)]
#[error(mogh_error::Error)]
pub struct ListBackupSnapshotDirectory {
  pub snapshot: String,
  #[serde(default)]
  pub parent: String,
  #[serde(default)]
  pub search: String,
  #[serde(default)]
  pub page: U64,
  #[serde(default = "default_page_limit")]
  pub limit: U64,
}
