use mogh_resolver::Resolve;
use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::entities::file_manager::{
  FileManagerActiveOperations, FileManagerCapabilities,
  FileManagerDirectory, FileManagerJournalStatus,
  FileManagerOperationStatus, FileManagerTarget, FileManagerTextFile,
};

use super::KomodoReadRequest;

#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoReadRequest)]
#[response(FileManagerCapabilities)]
#[error(mogh_error::Error)]
pub struct GetFileManagerCapabilities {
  pub target: FileManagerTarget,
}

#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoReadRequest)]
#[response(FileManagerDirectory)]
#[error(mogh_error::Error)]
pub struct ListFileManagerDirectory {
  pub target: FileManagerTarget,
  #[serde(default)]
  pub path: String,
}

#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoReadRequest)]
#[response(FileManagerTextFile)]
#[error(mogh_error::Error)]
pub struct ReadFileManagerText {
  pub target: FileManagerTarget,
  pub path: String,
}

#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoReadRequest)]
#[response(FileManagerTextFile)]
#[error(mogh_error::Error)]
pub struct ReadManagedFileManagerRenderedText {
  pub target: FileManagerTarget,
}

#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoReadRequest)]
#[response(FileManagerOperationStatus)]
#[error(mogh_error::Error)]
pub struct GetFileManagerOperationStatus {
  pub target: FileManagerTarget,
  pub operation_id: String,
}

#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoReadRequest)]
#[response(FileManagerActiveOperations)]
#[error(mogh_error::Error)]
pub struct ListActiveFileManagerOperations {
  pub target: FileManagerTarget,
}

#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Resolve)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[empty_traits(KomodoReadRequest)]
#[response(FileManagerJournalStatus)]
#[error(mogh_error::Error)]
pub struct GetFileManagerJournalStatus {
  pub target: FileManagerTarget,
}
