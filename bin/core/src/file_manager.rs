use std::{
  collections::HashMap,
  sync::{OnceLock, RwLock},
};

use anyhow::{Context as _, anyhow};
use komodo_client::entities::{
  ResourceTarget,
  file_manager::{
    FileManagerActiveOperations, FileManagerEntry,
    FileManagerEntryKind, FileManagerOperationPhase,
    FileManagerOperationState, FileManagerOperationStatus,
    FileManagerRevision, FileManagerTarget, FileManagerTextFile,
  },
  permission::PermissionLevel,
  repo::Repo,
  server::Server,
  stack::Stack,
  user::User,
};
use periphery_client::api::file_manager::PeripheryFileManagerTarget;
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use komodo_client::entities::komodo_timestamp;

use crate::{permission::get_check_permissions, resource};

const TRANSFER_TTL_MS: i64 = 5 * 60 * 1_000;
const OPERATION_TTL_MS: i64 = 24 * 60 * 60 * 1_000;

#[derive(Clone)]
struct CoreOperationRecord {
  actor: String,
  target: FileManagerTarget,
  expires_at: i64,
  status: FileManagerOperationStatus,
}

fn operation_statuses()
-> &'static RwLock<HashMap<String, CoreOperationRecord>> {
  static STATUSES: OnceLock<
    RwLock<HashMap<String, CoreOperationRecord>>,
  > = OnceLock::new();
  STATUSES.get_or_init(Default::default)
}

pub fn create_operation_status(
  actor: String,
  target: FileManagerTarget,
  description: impl Into<String>,
) -> String {
  let operation_id = Uuid::new_v4().to_string();
  insert_operation_status(
    operation_id.clone(),
    actor,
    target,
    description,
  );
  operation_id
}

fn insert_operation_status(
  operation_id: String,
  actor: String,
  target: FileManagerTarget,
  description: impl Into<String>,
) {
  let now = komodo_timestamp();
  let mut statuses = operation_statuses().write().unwrap();
  statuses.retain(|_, status| status.expires_at > now);
  statuses.insert(
    operation_id.clone(),
    CoreOperationRecord {
      actor,
      target,
      expires_at: now + OPERATION_TTL_MS,
      status: FileManagerOperationStatus {
        operation_id: operation_id.clone(),
        state: FileManagerOperationState::Pending,
        phase: FileManagerOperationPhase::Queued,
        description: description.into(),
        started_at: now,
        updated_at: now,
        phase_started_at: now,
        cancellable: true,
        ..Default::default()
      },
    },
  );
}

pub fn set_operation_finalizing(operation_id: &str) {
  if let Some(record) =
    operation_statuses().write().unwrap().get_mut(operation_id)
  {
    record.status.state = FileManagerOperationState::Running;
    record.status.phase = FileManagerOperationPhase::Finalizing;
    record.status.updated_at = komodo_timestamp();
  }
}

pub fn complete_operation(operation_id: &str) {
  if let Some(record) =
    operation_statuses().write().unwrap().get_mut(operation_id)
  {
    record.status.state = FileManagerOperationState::Complete;
    record.status.phase = FileManagerOperationPhase::Finalizing;
    record.status.completed_entries = record.status.total_entries;
    record.status.completed_bytes = record.status.total_bytes;
    record.status.error = None;
    record.status.cancellable = false;
    record.status.pending_conflict = None;
    record.status.temporary_storage_bytes = 0;
    record.status.updated_at = komodo_timestamp();
  }
}

pub fn fail_operation(operation_id: &str, error: impl Into<String>) {
  if let Some(record) =
    operation_statuses().write().unwrap().get_mut(operation_id)
  {
    record.status.state = FileManagerOperationState::Failed;
    record.status.error = Some(error.into());
    record.status.cancellable = false;
    record.status.pending_conflict = None;
    record.status.updated_at = komodo_timestamp();
  }
}

pub fn cancel_operation(
  operation_id: &str,
  message: impl Into<String>,
) {
  if let Some(record) =
    operation_statuses().write().unwrap().get_mut(operation_id)
  {
    set_operation_cancelled(
      &mut record.status,
      message.into(),
      komodo_timestamp(),
    );
  }
}

fn set_operation_cancelled(
  status: &mut FileManagerOperationStatus,
  message: String,
  now: i64,
) {
  status.state = FileManagerOperationState::Cancelled;
  status.error = Some(message);
  status.cancellable = false;
  status.pending_conflict = None;
  status.updated_at = now;
}

pub fn update_operation_status(
  operation_id: &str,
  status: FileManagerOperationStatus,
) {
  if let Some(record) =
    operation_statuses().write().unwrap().get_mut(operation_id)
  {
    record.status = status;
  }
}

pub fn list_core_active_operations(
  actor: &str,
  target: &FileManagerTarget,
) -> FileManagerActiveOperations {
  let now = komodo_timestamp();
  prune_transfer_sessions(now);
  let mut statuses = operation_statuses().write().unwrap();
  statuses.retain(|_, status| status.expires_at > now);
  let mut operations = statuses
    .values()
    .filter(|record| {
      record.actor == actor && &record.target == target
    })
    .map(|record| record.status.clone())
    .filter(|status| {
      !matches!(
        status.state,
        FileManagerOperationState::Complete
          | FileManagerOperationState::Failed
          | FileManagerOperationState::Cancelled
      )
    })
    .collect::<Vec<_>>();
  operations.sort_by_key(|status| status.started_at);
  FileManagerActiveOperations { operations }
}

pub fn get_core_operation_status(
  operation_id: &str,
  actor: &str,
  target: &FileManagerTarget,
) -> Option<FileManagerOperationStatus> {
  let now = komodo_timestamp();
  prune_transfer_sessions(now);
  let mut statuses = operation_statuses().write().unwrap();
  statuses.retain(|_, status| status.expires_at > now);
  statuses.get(operation_id).and_then(|record| {
    (record.actor == actor && &record.target == target)
      .then(|| record.status.clone())
  })
}

#[derive(Clone)]
pub enum TransferSessionKind {
  Upload {
    destination: String,
    file_name: String,
    total_bytes: u64,
    overwrite: bool,
    expected_revision: Option<FileManagerRevision>,
  },
  Download {
    paths: Vec<String>,
    allow_managed: bool,
  },
}

#[derive(Clone)]
pub struct TransferSession {
  pub operation_id: String,
  pub actor: String,
  pub target: FileManagerTarget,
  pub expires_at: i64,
  pub kind: TransferSessionKind,
}

fn transfer_sessions()
-> &'static RwLock<HashMap<String, TransferSession>> {
  static SESSIONS: OnceLock<
    RwLock<HashMap<String, TransferSession>>,
  > = OnceLock::new();
  SESSIONS.get_or_init(Default::default)
}

fn cancel_pending_transfer_status(
  statuses: &mut HashMap<String, CoreOperationRecord>,
  session: &TransferSession,
  message: &str,
  now: i64,
) -> Option<FileManagerOperationStatus> {
  let record = statuses.get_mut(&session.operation_id)?;
  if record.actor != session.actor
    || record.target != session.target
    || record.status.state != FileManagerOperationState::Pending
  {
    return None;
  }
  set_operation_cancelled(
    &mut record.status,
    message.to_string(),
    now,
  );
  Some(record.status.clone())
}

fn prune_transfer_sessions(now: i64) {
  let mut sessions = transfer_sessions().write().unwrap();
  let mut statuses = operation_statuses().write().unwrap();
  sessions.retain(|_, session| {
    if session.expires_at > now {
      return true;
    }
    cancel_pending_transfer_status(
      &mut statuses,
      session,
      "Transfer token expired before streaming began",
      now,
    );
    false
  });
}

pub fn cancel_pending_transfer_session(
  operation_id: &str,
  actor: &str,
  target: &FileManagerTarget,
) -> Option<FileManagerOperationStatus> {
  let now = komodo_timestamp();
  let status = {
    let mut sessions = transfer_sessions().write().unwrap();
    let token = sessions.iter().find_map(|(token, session)| {
      (session.operation_id == operation_id
        && session.actor == actor
        && &session.target == target)
        .then(|| token.clone())
    })?;
    let mut statuses = operation_statuses().write().unwrap();
    let status = cancel_pending_transfer_status(
      &mut statuses,
      sessions.get(&token)?,
      "Transfer was cancelled before streaming began",
      now,
    )?;
    sessions.remove(&token);
    status
  };
  prune_transfer_sessions(now);
  Some(status)
}

pub fn create_transfer_session(
  actor: String,
  target: FileManagerTarget,
  kind: TransferSessionKind,
) -> (String, TransferSession) {
  let now = komodo_timestamp();
  prune_transfer_sessions(now);
  let token = Uuid::new_v4().to_string();
  let operation_id = Uuid::new_v4().to_string();
  let description = match &kind {
    TransferSessionKind::Upload { .. } => "Upload file",
    TransferSessionKind::Download { .. } => "Download files",
  };
  insert_operation_status(
    operation_id.clone(),
    actor.clone(),
    target.clone(),
    description,
  );
  let session = TransferSession {
    operation_id,
    actor,
    target,
    expires_at: now + TRANSFER_TTL_MS,
    kind,
  };
  let mut sessions = transfer_sessions().write().unwrap();
  sessions.insert(token.clone(), session.clone());
  (token, session)
}

pub fn consume_transfer_session(
  token: &str,
  actor: &str,
) -> anyhow::Result<TransferSession> {
  let now = komodo_timestamp();
  prune_transfer_sessions(now);
  let session = {
    let mut sessions = transfer_sessions().write().unwrap();
    let session = sessions.get(token).context(
      "Transfer token is missing, expired, or already used",
    )?;
    if session.actor != actor {
      return Err(anyhow!("Transfer token belongs to another user"));
    }
    sessions.remove(token).context(
      "Transfer token is missing, expired, or already used",
    )?
  };
  if session.expires_at <= now {
    return Err(anyhow!("Transfer token has expired"));
  }
  Ok(session)
}

pub struct ResolvedFileManagerTarget {
  pub server: Server,
  pub periphery: PeripheryFileManagerTarget,
  pub resource: ResourceTarget,
  pub stack: Option<Stack>,
  pub managed_file: Option<String>,
}

pub fn require_managed_file(
  resolved: &ResolvedFileManagerTarget,
) -> anyhow::Result<String> {
  resolved.managed_file.clone().context(
    "Rendered managed compose is available only for UI-managed stacks",
  )
}

pub async fn resolve_target(
  target: &FileManagerTarget,
  user: &User,
  permission: PermissionLevel,
) -> anyhow::Result<ResolvedFileManagerTarget> {
  match target {
    FileManagerTarget::Stack { stack } => {
      let stack = get_check_permissions::<Stack>(
        stack,
        user,
        permission.file_manager(),
      )
      .await?;
      if !stack.config.swarm_id.is_empty() {
        return Err(anyhow!(
          "File Manager is unavailable for Swarm stacks"
        ));
      }
      if stack.config.server_id.is_empty() {
        return Err(anyhow!("Stack does not have a server assigned"));
      }
      let server =
        resource::get::<Server>(&stack.config.server_id).await?;
      let repo = if stack.config.linked_repo.is_empty() {
        None
      } else {
        Some(resource::get::<Repo>(&stack.config.linked_repo).await?)
      };
      let managed_file = is_ui_managed(&stack).then(|| {
        stack
          .compose_file_paths()
          .first()
          .and_then(|path| std::path::Path::new(path).file_name())
          .and_then(|name| name.to_str())
          .unwrap_or("compose.yaml")
          .to_string()
      });
      Ok(ResolvedFileManagerTarget {
        server,
        periphery: PeripheryFileManagerTarget::Stack {
          stack: Box::new(stack.clone()),
          repo: repo.map(Box::new),
        },
        resource: ResourceTarget::Stack(stack.id.clone()),
        stack: Some(stack),
        managed_file,
      })
    }
    FileManagerTarget::Volume { server, volume } => {
      if volume.trim().is_empty() {
        return Err(anyhow!("Volume name cannot be empty"));
      }
      let server = get_check_permissions::<Server>(
        server,
        user,
        permission.file_manager(),
      )
      .await?;
      Ok(ResolvedFileManagerTarget {
        periphery: PeripheryFileManagerTarget::Volume {
          volume: volume.clone(),
        },
        resource: ResourceTarget::Server(server.id.clone()),
        server,
        stack: None,
        managed_file: None,
      })
    }
  }
}

pub async fn can_write(
  target: &FileManagerTarget,
  user: &User,
) -> bool {
  resolve_target(target, user, PermissionLevel::Write)
    .await
    .is_ok()
}

pub fn is_ui_managed(stack: &Stack) -> bool {
  !stack.config.files_on_host
    && stack.config.repo.is_empty()
    && stack.config.linked_repo.is_empty()
}

pub fn managed_text(
  stack: &Stack,
  path: &str,
) -> FileManagerTextFile {
  FileManagerTextFile {
    path: path.to_string(),
    contents: stack.config.file_contents.clone(),
    revision: managed_revision(&stack.config.file_contents),
  }
}

pub fn managed_entry(stack: &Stack, name: &str) -> FileManagerEntry {
  FileManagerEntry {
    path: name.to_string(),
    name: name.to_string(),
    kind: FileManagerEntryKind::File,
    size: stack.config.file_contents.len() as u64,
    modified_at: 0,
    revision: managed_revision(&stack.config.file_contents),
    managed: true,
  }
}

pub fn managed_revision(contents: &str) -> FileManagerRevision {
  let mut hash = Sha256::new();
  hash.update(contents.as_bytes());
  FileManagerRevision {
    id: hex::encode(hash.finalize()),
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn target() -> FileManagerTarget {
    FileManagerTarget::Volume {
      server: "server-1".into(),
      volume: "volume-1".into(),
    }
  }

  #[test]
  fn core_operation_status_is_scoped_and_reaches_terminal_state() {
    let target = target();
    let operation_id = create_operation_status(
      "actor-1".into(),
      target.clone(),
      "Copy files",
    );

    assert!(
      get_core_operation_status(&operation_id, "actor-2", &target)
        .is_none()
    );
    let queued =
      get_core_operation_status(&operation_id, "actor-1", &target)
        .unwrap();
    assert_eq!(queued.state, FileManagerOperationState::Pending);
    assert_eq!(queued.phase, FileManagerOperationPhase::Queued);
    assert_eq!(queued.description, "Copy files");

    set_operation_finalizing(&operation_id);
    assert_eq!(
      get_core_operation_status(&operation_id, "actor-1", &target)
        .unwrap()
        .phase,
      FileManagerOperationPhase::Finalizing
    );
    complete_operation(&operation_id);
    assert_eq!(
      get_core_operation_status(&operation_id, "actor-1", &target)
        .unwrap()
        .state,
      FileManagerOperationState::Complete
    );
  }

  #[test]
  fn transfer_session_registers_status_before_streaming_begins() {
    let target = target();
    let (token, session) = create_transfer_session(
      "actor-1".into(),
      target.clone(),
      TransferSessionKind::Upload {
        destination: String::new(),
        file_name: "notes.txt".into(),
        total_bytes: 12,
        overwrite: false,
        expected_revision: None,
      },
    );

    let status = get_core_operation_status(
      &session.operation_id,
      "actor-1",
      &target,
    )
    .unwrap();
    assert_eq!(status.state, FileManagerOperationState::Pending);
    assert_eq!(status.description, "Upload file");
    assert!(consume_transfer_session(&token, "actor-1").is_ok());
  }

  #[test]
  fn wrong_actor_cannot_consume_another_users_transfer_session() {
    let target = FileManagerTarget::Volume {
      server: "server-owned".into(),
      volume: "volume-owned".into(),
    };
    let (token, _) = create_transfer_session(
      "actor-owner".into(),
      target,
      TransferSessionKind::Download {
        paths: vec!["archive.tar".into()],
        allow_managed: false,
      },
    );

    let error = consume_transfer_session(&token, "actor-other")
      .err()
      .unwrap();
    assert_eq!(
      error.to_string(),
      "Transfer token belongs to another user"
    );
    assert!(consume_transfer_session(&token, "actor-owner").is_ok());
  }

  #[test]
  fn expired_transfer_session_is_pruned_with_its_pending_status() {
    let target = FileManagerTarget::Volume {
      server: "server-expired".into(),
      volume: "volume-expired".into(),
    };
    let (token, session) = create_transfer_session(
      "actor-expired".into(),
      target.clone(),
      TransferSessionKind::Download {
        paths: vec!["archive.tar".into()],
        allow_managed: false,
      },
    );
    transfer_sessions()
      .write()
      .unwrap()
      .get_mut(&token)
      .unwrap()
      .expires_at = komodo_timestamp() - 1;

    let active =
      list_core_active_operations("actor-expired", &target);
    assert!(active.operations.is_empty());
    let status = get_core_operation_status(
      &session.operation_id,
      "actor-expired",
      &target,
    )
    .unwrap();
    assert_eq!(status.state, FileManagerOperationState::Cancelled);
    assert!(!status.cancellable);
    assert_eq!(
      status.error.as_deref(),
      Some("Transfer token expired before streaming began")
    );
    assert!(
      consume_transfer_session(&token, "actor-expired").is_err()
    );
  }

  #[test]
  fn unconsumed_transfer_session_can_be_cancelled_locally() {
    let target = FileManagerTarget::Volume {
      server: "server-cancel".into(),
      volume: "volume-cancel".into(),
    };
    let (token, session) = create_transfer_session(
      "actor-cancel".into(),
      target.clone(),
      TransferSessionKind::Download {
        paths: vec!["archive.tar".into()],
        allow_managed: false,
      },
    );

    assert!(
      cancel_pending_transfer_session(
        &session.operation_id,
        "another-actor",
        &target,
      )
      .is_none()
    );
    let status = cancel_pending_transfer_session(
      &session.operation_id,
      "actor-cancel",
      &target,
    )
    .unwrap();
    assert_eq!(status.state, FileManagerOperationState::Cancelled);
    assert!(!status.cancellable);
    assert_eq!(
      status.error.as_deref(),
      Some("Transfer was cancelled before streaming began")
    );
    assert!(
      consume_transfer_session(&token, "actor-cancel").is_err()
    );
    assert!(
      list_core_active_operations("actor-cancel", &target)
        .operations
        .is_empty()
    );
  }

  #[test]
  fn consumed_transfer_session_is_not_cancelled_locally() {
    let target = FileManagerTarget::Volume {
      server: "server-consumed".into(),
      volume: "volume-consumed".into(),
    };
    let (token, session) = create_transfer_session(
      "actor-consumed".into(),
      target.clone(),
      TransferSessionKind::Download {
        paths: vec!["archive.tar".into()],
        allow_managed: false,
      },
    );
    consume_transfer_session(&token, "actor-consumed").unwrap();
    set_operation_finalizing(&session.operation_id);

    assert!(
      cancel_pending_transfer_session(
        &session.operation_id,
        "actor-consumed",
        &target,
      )
      .is_none()
    );
    let status = get_core_operation_status(
      &session.operation_id,
      "actor-consumed",
      &target,
    )
    .unwrap();
    assert_eq!(status.state, FileManagerOperationState::Running);
    assert_eq!(status.phase, FileManagerOperationPhase::Finalizing);
  }

  #[test]
  fn managed_download_authorization_is_scoped_to_its_session() {
    let target = FileManagerTarget::Stack {
      stack: "stack-managed".into(),
    };
    let (token, _) = create_transfer_session(
      "actor-managed".into(),
      target,
      TransferSessionKind::Download {
        paths: vec!["compose.generated.yaml".into()],
        allow_managed: true,
      },
    );

    let session =
      consume_transfer_session(&token, "actor-managed").unwrap();
    let TransferSessionKind::Download {
      paths,
      allow_managed,
    } = session.kind
    else {
      panic!("Expected a download transfer session");
    };
    assert_eq!(paths, ["compose.generated.yaml"]);
    assert!(allow_managed);
  }
}
