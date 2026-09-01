use komodo_client::entities::{
  repo::Repo, stack::Stack, update::Log,
};
use mogh_resolver::Resolve;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StackDeletionMode {
  Compose,
  Swarm,
}

/// Validate stack file ownership and all candidate Swarm volumes without
/// mutating either. Core runs this on every Swarm host before teardown.
#[derive(Debug, Clone, Serialize, Deserialize, Resolve)]
#[response(Log)]
#[error(anyhow::Error)]
pub struct ValidateStackDeletion {
  pub stack: Stack,
  #[serde(default)]
  pub remove_volumes: bool,
}

/// Tear down a stack, optionally remove its owned volumes, and atomically
/// retire its Komodo-owned files into a recoverable quarantine.
#[derive(Debug, Clone, Serialize, Deserialize, Resolve)]
#[response(Vec<Log>)]
#[error(anyhow::Error)]
pub struct PrepareStackDeletion {
  pub transaction_id: String,
  pub stack: Stack,
  pub repo: Option<Repo>,
  pub mode: StackDeletionMode,
  #[serde(default)]
  pub remove_volumes: bool,
}

/// Restore files retired by a prepared stack deletion.
#[derive(Debug, Clone, Serialize, Deserialize, Resolve)]
#[response(Log)]
#[error(anyhow::Error)]
pub struct RollbackStackDeletion {
  pub transaction_id: String,
  pub stack_name: String,
}

/// Commit a prepared stack deletion to protected asynchronous cleanup.
#[derive(Debug, Clone, Serialize, Deserialize, Resolve)]
#[response(Log)]
#[error(anyhow::Error)]
pub struct CommitStackDeletion {
  pub transaction_id: String,
  pub stack_name: String,
}
