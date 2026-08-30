use komodo_client::entities::{
  file_manager::{
    FileManagerActiveOperations, FileManagerCapabilities,
    FileManagerConflictAction, FileManagerConflictDecision,
    FileManagerDirectory, FileManagerJournalStatus,
    FileManagerOperation, FileManagerOperationStatus,
    FileManagerPreflight, FileManagerRevision, FileManagerTextFile,
  },
  repo::Repo,
  stack::Stack,
};
use mogh_resolver::Resolve;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "params")]
pub enum PeripheryFileManagerTarget {
  Stack {
    stack: Box<Stack>,
    repo: Option<Box<Repo>>,
  },
  Volume {
    volume: String,
  },
}

#[derive(Debug, Clone, Serialize, Deserialize, Resolve)]
#[response(FileManagerCapabilities)]
#[error(anyhow::Error)]
pub struct GetFileManagerCapabilities {
  pub target: PeripheryFileManagerTarget,
}

#[derive(Debug, Clone, Serialize, Deserialize, Resolve)]
#[response(FileManagerDirectory)]
#[error(anyhow::Error)]
pub struct ListFileManagerDirectory {
  pub target: PeripheryFileManagerTarget,
  pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Resolve)]
#[response(FileManagerTextFile)]
#[error(anyhow::Error)]
pub struct ReadFileManagerText {
  pub target: PeripheryFileManagerTarget,
  pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Resolve)]
#[response(FileManagerPreflight)]
#[error(anyhow::Error)]
pub struct PreflightFileManagerOperation {
  pub target: PeripheryFileManagerTarget,
  pub actor: String,
  pub operation: FileManagerOperation,
}

#[derive(Debug, Clone, Serialize, Deserialize, Resolve)]
#[response(FileManagerCommitResponse)]
#[error(anyhow::Error)]
pub struct CommitFileManagerOperation {
  pub target: PeripheryFileManagerTarget,
  pub actor: String,
  pub operation_id: String,
  pub plan_id: String,
  pub decisions: Vec<FileManagerConflictDecision>,
  pub confirmed: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FileManagerCommitResponse {
  pub operation_id: String,
  pub affected_paths: Vec<String>,
  #[serde(default)]
  pub undoable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Resolve)]
#[response(FileManagerOperationStatus)]
#[error(anyhow::Error)]
pub struct GetFileManagerOperationStatus {
  pub target: PeripheryFileManagerTarget,
  pub actor: String,
  pub operation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Resolve)]
#[response(FileManagerActiveOperations)]
#[error(anyhow::Error)]
pub struct ListActiveFileManagerOperations {
  pub target: PeripheryFileManagerTarget,
  pub actor: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Resolve)]
#[response(FileManagerOperationStatus)]
#[error(anyhow::Error)]
pub struct ResolveFileManagerOperationConflict {
  pub target: PeripheryFileManagerTarget,
  pub actor: String,
  pub operation_id: String,
  pub decision_id: String,
  pub action: FileManagerConflictAction,
  #[serde(default)]
  pub apply_to_all: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Resolve)]
#[response(FileManagerOperationStatus)]
#[error(anyhow::Error)]
pub struct CancelFileManagerOperation {
  pub target: PeripheryFileManagerTarget,
  pub actor: String,
  pub operation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Resolve)]
#[response(FileManagerJournalStatus)]
#[error(anyhow::Error)]
pub struct GetFileManagerJournalStatus {
  pub target: PeripheryFileManagerTarget,
  pub actor: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Resolve)]
#[response(FileManagerCommitResponse)]
#[error(anyhow::Error)]
pub struct UndoFileManagerOperation {
  pub target: PeripheryFileManagerTarget,
  pub actor: String,
  pub operation_id: String,
  pub confirmed: bool,
  /// Internal source operation to roll back. Public undo leaves this empty.
  #[serde(default)]
  pub rollback_operation_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Resolve)]
#[response(FileManagerCommitResponse)]
#[error(anyhow::Error)]
pub struct RedoFileManagerOperation {
  pub target: PeripheryFileManagerTarget,
  pub actor: String,
  pub operation_id: String,
  pub confirmed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Resolve)]
#[response(Uuid)]
#[error(anyhow::Error)]
pub struct StartFileManagerUpload {
  pub target: PeripheryFileManagerTarget,
  pub actor: String,
  pub operation_id: String,
  pub destination: String,
  pub file_name: String,
  pub total_bytes: u64,
  pub overwrite: bool,
  pub expected_revision: Option<FileManagerRevision>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Resolve)]
#[response(StartFileManagerDownloadResponse)]
#[error(anyhow::Error)]
pub struct StartFileManagerDownload {
  pub target: PeripheryFileManagerTarget,
  pub actor: String,
  pub operation_id: String,
  pub paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartFileManagerDownloadResponse {
  pub channel: Uuid,
  pub file_name: String,
  pub total_bytes: u64,
  pub sha256: String,
}
