use komodo_client::entities::{
  file_manager::{
    FileManagerActiveOperations, FileManagerCapabilities,
    FileManagerConflictAction, FileManagerConflictDecision,
    FileManagerDirectory, FileManagerExecutionMode,
    FileManagerJournalStatus, FileManagerOperation,
    FileManagerOperationStatus, FileManagerPreflight,
    FileManagerRevision, FileManagerTextFile,
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
    protected_paths: Vec<super::backup::ProtectedRepositoryPath>,
  },
  Volume {
    volume: String,
    protected_paths: Vec<super::backup::ProtectedRepositoryPath>,
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
  #[serde(default)]
  pub execution_mode: FileManagerExecutionMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, Resolve)]
#[response(FileManagerManagedTransactionStatus)]
#[error(anyhow::Error)]
pub struct BeginManagedFileManagerTransaction {
  pub target: PeripheryFileManagerTarget,
  pub actor: String,
  pub operation_id: String,
  pub plan_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Resolve)]
#[response(Option<FileManagerManagedTransactionStatus>)]
#[error(anyhow::Error)]
pub struct GetManagedFileManagerTransaction {
  pub target: PeripheryFileManagerTarget,
  pub actor: String,
  pub operation_id: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileManagerManagedTransactionFinalizeAction {
  Commit,
  Rollback,
}

#[derive(
  Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FileManagerManagedTransactionState {
  #[default]
  Prepared,
  Applying,
  Applied,
  CommitRequested,
  RollbackRequested,
  RolledBack,
  Committed,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FileManagerManagedTransactionStatus {
  pub operation_id: String,
  pub state: FileManagerManagedTransactionState,
}

#[derive(Debug, Clone, Serialize, Deserialize, Resolve)]
#[response(FileManagerManagedTransactionStatus)]
#[error(anyhow::Error)]
pub struct FinalizeManagedFileManagerTransaction {
  pub target: PeripheryFileManagerTarget,
  pub actor: String,
  pub operation_id: String,
  pub action: FileManagerManagedTransactionFinalizeAction,
}

/// Atomically move a UI-managed stack environment file before Core changes
/// its configured path.
#[derive(Debug, Clone, Serialize, Deserialize, Resolve)]
#[response(FileManagerManagedTransactionStatus)]
#[error(anyhow::Error)]
pub struct PrepareManagedEnvironmentFileMigration {
  pub target: PeripheryFileManagerTarget,
  pub operation_id: String,
  pub old_path: String,
  pub new_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Resolve)]
#[response(Option<FileManagerManagedTransactionStatus>)]
#[error(anyhow::Error)]
pub struct GetManagedEnvironmentFileMigration {
  pub target: PeripheryFileManagerTarget,
  pub operation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Resolve)]
#[response(FileManagerManagedTransactionStatus)]
#[error(anyhow::Error)]
pub struct FinalizeManagedEnvironmentFileMigration {
  pub target: PeripheryFileManagerTarget,
  pub operation_id: String,
  pub action: FileManagerManagedTransactionFinalizeAction,
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
  /// Enables the crash-durable managed-file protocol after a successful
  /// `BeginManagedFileManagerTransaction` handshake. Older Core versions omit
  /// this field and keep the legacy behavior.
  #[serde(default)]
  pub durable_managed: bool,
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
  #[serde(default)]
  pub allow_managed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartFileManagerDownloadResponse {
  pub channel: Uuid,
  pub file_name: String,
  pub total_bytes: u64,
  pub sha256: String,
  #[serde(default)]
  pub supports_download_credit: bool,
  #[serde(default)]
  pub supports_download_heartbeat: bool,
}
