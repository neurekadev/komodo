use std::{
  collections::HashMap,
  sync::{OnceLock, RwLock},
};

use anyhow::{Context as _, anyhow};
use komodo_client::entities::{
  ResourceTarget,
  file_manager::{
    FileManagerEntry, FileManagerEntryKind, FileManagerRevision,
    FileManagerTarget, FileManagerTextFile,
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

pub fn create_transfer_session(
  actor: String,
  target: FileManagerTarget,
  kind: TransferSessionKind,
) -> (String, TransferSession) {
  let now = komodo_timestamp();
  let token = Uuid::new_v4().to_string();
  let session = TransferSession {
    operation_id: Uuid::new_v4().to_string(),
    actor,
    target,
    expires_at: now + TRANSFER_TTL_MS,
    kind,
  };
  let mut sessions = transfer_sessions().write().unwrap();
  sessions.retain(|_, session| session.expires_at > now);
  sessions.insert(token.clone(), session.clone());
  (token, session)
}

pub fn consume_transfer_session(
  token: &str,
  actor: &str,
) -> anyhow::Result<TransferSession> {
  let session =
    transfer_sessions().write().unwrap().remove(token).context(
      "Transfer token is missing, expired, or already used",
    )?;
  if session.actor != actor {
    return Err(anyhow!("Transfer token belongs to another user"));
  }
  if session.expires_at < komodo_timestamp() {
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
        permission.into(),
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
        permission.into(),
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
