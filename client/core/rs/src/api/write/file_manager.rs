use mogh_resolver::Resolve;
use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::entities::{
  U64,
  file_manager::{
    FileManagerConflictDecision, FileManagerOperation,
    FileManagerOperationTicket, FileManagerPreflight,
    FileManagerRevision, FileManagerTarget,
    FileManagerTransferTicket,
  },
};

use super::KomodoWriteRequest;

#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoWriteRequest)]
#[response(FileManagerPreflight)]
#[error(mogh_error::Error)]
pub struct PreflightFileManagerOperation {
  pub target: FileManagerTarget,
  pub operation: FileManagerOperation,
}

#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoWriteRequest)]
#[response(FileManagerOperationTicket)]
#[error(mogh_error::Error)]
pub struct CommitFileManagerOperation {
  pub target: FileManagerTarget,
  pub plan_id: String,
  #[serde(default)]
  pub decisions: Vec<FileManagerConflictDecision>,
  #[serde(default)]
  pub confirmed: bool,
}

#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoWriteRequest)]
#[response(FileManagerTransferTicket)]
#[error(mogh_error::Error)]
pub struct PrepareFileManagerUpload {
  pub target: FileManagerTarget,
  pub destination: String,
  pub file_names: Vec<String>,
  pub total_bytes: Option<U64>,
  #[serde(default)]
  pub overwrite: bool,
  #[serde(default)]
  pub confirmed: bool,
  pub expected_revision: Option<FileManagerRevision>,
}

#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoWriteRequest)]
#[response(FileManagerTransferTicket)]
#[error(mogh_error::Error)]
pub struct PrepareFileManagerDownload {
  pub target: FileManagerTarget,
  pub paths: Vec<String>,
}

#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoWriteRequest)]
#[response(FileManagerOperationTicket)]
#[error(mogh_error::Error)]
pub struct UndoFileManagerOperation {
  pub target: FileManagerTarget,
  #[serde(default)]
  pub confirmed: bool,
}

#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoWriteRequest)]
#[response(FileManagerOperationTicket)]
#[error(mogh_error::Error)]
pub struct RedoFileManagerOperation {
  pub target: FileManagerTarget,
  #[serde(default)]
  pub confirmed: bool,
}
