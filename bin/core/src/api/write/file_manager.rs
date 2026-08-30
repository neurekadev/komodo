use std::{
  collections::HashMap,
  sync::{OnceLock, RwLock},
};

use anyhow::{Context as _, anyhow};
use database::bson::doc;
use formatting::format_serror;
use interpolate::Interpolator;
use komodo_client::{
  api::write::*,
  entities::{
    Operation, ResourceTarget,
    file_manager::{
      FileManagerOperation, FileManagerOperationTicket,
      FileManagerPreflight, FileManagerRevision, FileManagerTarget,
      FileManagerTransferTicket,
    },
    komodo_timestamp,
    permission::PermissionLevel,
    stack::Stack,
    update::Update,
  },
};
use mogh_resolver::Resolve;
use periphery_client::api::file_manager as periphery;
use uuid::Uuid;

use crate::{
  config::core_config,
  file_manager::{
    ResolvedFileManagerTarget, TransferSessionKind,
    complete_operation, create_operation_status,
    create_transfer_session, fail_operation, managed_revision,
    resolve_target, set_operation_finalizing,
  },
  helpers::{
    periphery_client,
    query::{VariablesAndSecrets, get_variables_and_secrets},
    update::{add_update, make_update},
  },
  state::db_client,
};

use super::WriteArgs;

#[derive(Clone)]
struct ManagedPlan {
  actor: String,
  stack_name: String,
  before: String,
  after: String,
  expires_at: i64,
}

#[derive(Clone)]
struct HistoryEntry {
  managed: Option<ManagedHistory>,
}

#[derive(Clone)]
struct ManagedHistory {
  stack_name: String,
  before: String,
  after: String,
}

#[derive(Default)]
struct History {
  undo: Vec<HistoryEntry>,
  redo: Vec<HistoryEntry>,
}

fn managed_plans() -> &'static RwLock<HashMap<String, ManagedPlan>> {
  static PLANS: OnceLock<RwLock<HashMap<String, ManagedPlan>>> =
    OnceLock::new();
  PLANS.get_or_init(Default::default)
}

fn histories() -> &'static RwLock<HashMap<String, History>> {
  static HISTORIES: OnceLock<RwLock<HashMap<String, History>>> =
    OnceLock::new();
  HISTORIES.get_or_init(Default::default)
}

impl Resolve<WriteArgs> for PreflightFileManagerOperation {
  async fn resolve(
    self,
    WriteArgs { user }: &WriteArgs,
  ) -> mogh_error::Result<FileManagerPreflight> {
    ensure_writes_enabled()?;
    let resolved =
      resolve_target(&self.target, user, PermissionLevel::Write)
        .await?;
    let (operation, managed) =
      prepare_managed_operation(&resolved, &self.operation).await?;
    let response = periphery_client(&resolved.server)
      .await?
      .request(periphery::PreflightFileManagerOperation {
        target: resolved.periphery,
        actor: user.id.clone(),
        operation,
      })
      .await?;
    if let Some((stack, before, after)) = managed {
      let mut plans = managed_plans().write().unwrap();
      plans.retain(|_, plan| plan.expires_at > komodo_timestamp());
      plans.insert(
        response.plan_id.clone(),
        ManagedPlan {
          actor: user.id.clone(),
          stack_name: stack.name,
          before,
          after,
          expires_at: response.expires_at,
        },
      );
    }
    Ok(response)
  }
}

impl Resolve<WriteArgs> for CommitFileManagerOperation {
  async fn resolve(
    self,
    WriteArgs { user }: &WriteArgs,
  ) -> mogh_error::Result<FileManagerOperationTicket> {
    ensure_writes_enabled()?;
    let resolved =
      resolve_target(&self.target, user, PermissionLevel::Write)
        .await?;
    let managed =
      managed_plans().write().unwrap().remove(&self.plan_id);
    if let Some(plan) = &managed {
      if plan.actor != user.id {
        return Err(
          anyhow!("Preflight plan belongs to another user").into(),
        );
      }
      if plan.expires_at < komodo_timestamp() {
        return Err(anyhow!("Preflight plan has expired").into());
      }
    }
    let operation_id = create_operation_status(
      user.id.clone(),
      self.target.clone(),
      "File operation",
    );
    let ticket = FileManagerOperationTicket {
      operation_id: operation_id.clone(),
    };
    let actor = user.clone();
    let target = self.target;
    tokio::spawn(async move {
      let result = async {
      let response = periphery_client(&resolved.server)
        .await?
        .request(periphery::CommitFileManagerOperation {
          target: resolved.periphery.clone(),
          actor: actor.id.clone(),
          operation_id: operation_id.clone(),
          plan_id: self.plan_id,
          decisions: self.decisions,
          confirmed: self.confirmed,
        })
        .await?;
      set_operation_finalizing(&operation_id);
      if let Some(plan) = &managed
        && let Err(error) = update_managed_source(
          &plan.stack_name,
          &plan.before,
          &plan.after,
        )
        .await
      {
        let _ = periphery_client(&resolved.server)
          .await?
          .request(periphery::UndoFileManagerOperation {
            target: resolved.periphery.clone(),
            actor: actor.id.clone(),
            operation_id: Uuid::new_v4().to_string(),
            confirmed: true,
          })
          .await;
        return Err(error.context(
          "Host write was rolled back after the managed source changed",
        ));
      }
      push_history(
        &target,
        &actor.id,
        HistoryEntry {
          managed: managed.map(|plan| ManagedHistory {
            stack_name: plan.stack_name,
            before: plan.before,
            after: plan.after,
          }),
        },
      );
      anyhow::Ok(response.affected_paths)
    }
    .await;
      let operation_error =
        result.as_ref().err().map(ToString::to_string);
      let audit = audit_result(
        resolved.resource,
        "File operation",
        result,
        &actor,
      )
      .await;
      if let Some(error) = operation_error {
        fail_operation(&operation_id, error);
      } else if let Err(error) = audit {
        fail_operation(&operation_id, error.error.to_string());
      } else {
        complete_operation(&operation_id);
      }
    });
    Ok(ticket)
  }
}

impl Resolve<WriteArgs> for UndoFileManagerOperation {
  async fn resolve(
    self,
    WriteArgs { user }: &WriteArgs,
  ) -> mogh_error::Result<FileManagerOperationTicket> {
    ensure_writes_enabled()?;
    let resolved =
      resolve_target(&self.target, user, PermissionLevel::Write)
        .await?;
    let entry = pop_undo(&self.target, &user.id);
    let operation_id = create_operation_status(
      user.id.clone(),
      self.target.clone(),
      "Undo file operation",
    );
    let ticket = FileManagerOperationTicket {
      operation_id: operation_id.clone(),
    };
    let actor = user.clone();
    let target = self.target;
    tokio::spawn(async move {
      let result = async {
      let response = periphery_client(&resolved.server)
        .await?
        .request(periphery::UndoFileManagerOperation {
          target: resolved.periphery.clone(),
          actor: actor.id.clone(),
          operation_id: operation_id.clone(),
          confirmed: self.confirmed,
        })
        .await;
      let response = match response {
        Ok(response) => response,
        Err(error) => {
          if let Some(entry) = entry.clone() {
            restore_undo(&target, &actor.id, entry);
          }
          return Err(error);
        }
      };
      set_operation_finalizing(&operation_id);
      if let Some(ManagedHistory {
        stack_name,
        before,
        after,
      }) = entry.as_ref().and_then(|entry| entry.managed.as_ref())
        && let Err(error) =
          update_managed_source(stack_name, after, before).await
      {
        let _ = periphery_client(&resolved.server)
          .await?
          .request(periphery::RedoFileManagerOperation {
            target: resolved.periphery.clone(),
            actor: actor.id.clone(),
            operation_id: Uuid::new_v4().to_string(),
            confirmed: true,
          })
          .await;
        restore_undo(&target, &actor.id, entry.clone().unwrap());
        return Err(error.context(
          "Host undo was rolled back after the managed source changed",
        ));
      }
      if let Some(entry) = entry {
        push_redo(&target, &actor.id, entry);
      }
      anyhow::Ok(response.affected_paths)
    }
    .await;
      let operation_error =
        result.as_ref().err().map(ToString::to_string);
      let audit = audit_result(
        resolved.resource,
        "Undo file operation",
        result,
        &actor,
      )
      .await;
      if let Some(error) = operation_error {
        fail_operation(&operation_id, error);
      } else if let Err(error) = audit {
        fail_operation(&operation_id, error.error.to_string());
      } else {
        complete_operation(&operation_id);
      }
    });
    Ok(ticket)
  }
}

impl Resolve<WriteArgs> for RedoFileManagerOperation {
  async fn resolve(
    self,
    WriteArgs { user }: &WriteArgs,
  ) -> mogh_error::Result<FileManagerOperationTicket> {
    ensure_writes_enabled()?;
    let resolved =
      resolve_target(&self.target, user, PermissionLevel::Write)
        .await?;
    let entry = pop_redo(&self.target, &user.id);
    let operation_id = create_operation_status(
      user.id.clone(),
      self.target.clone(),
      "Redo file operation",
    );
    let ticket = FileManagerOperationTicket {
      operation_id: operation_id.clone(),
    };
    let actor = user.clone();
    let target = self.target;
    tokio::spawn(async move {
      let result = async {
      let response = periphery_client(&resolved.server)
        .await?
        .request(periphery::RedoFileManagerOperation {
          target: resolved.periphery.clone(),
          actor: actor.id.clone(),
          operation_id: operation_id.clone(),
          confirmed: self.confirmed,
        })
        .await;
      let response = match response {
        Ok(response) => response,
        Err(error) => {
          if let Some(entry) = entry.clone() {
            restore_redo(&target, &actor.id, entry);
          }
          return Err(error);
        }
      };
      set_operation_finalizing(&operation_id);
      if let Some(ManagedHistory {
        stack_name,
        before,
        after,
      }) = entry.as_ref().and_then(|entry| entry.managed.as_ref())
        && let Err(error) =
          update_managed_source(stack_name, before, after).await
      {
        let _ = periphery_client(&resolved.server)
          .await?
          .request(periphery::UndoFileManagerOperation {
            target: resolved.periphery.clone(),
            actor: actor.id.clone(),
            operation_id: Uuid::new_v4().to_string(),
            confirmed: true,
          })
          .await;
        restore_redo(&target, &actor.id, entry.clone().unwrap());
        return Err(error.context(
          "Host redo was rolled back after the managed source changed",
        ));
      }
      if let Some(entry) = entry {
        restore_undo(&target, &actor.id, entry);
      }
      anyhow::Ok(response.affected_paths)
    }
    .await;
      let operation_error =
        result.as_ref().err().map(ToString::to_string);
      let audit = audit_result(
        resolved.resource,
        "Redo file operation",
        result,
        &actor,
      )
      .await;
      if let Some(error) = operation_error {
        fail_operation(&operation_id, error);
      } else if let Err(error) = audit {
        fail_operation(&operation_id, error.error.to_string());
      } else {
        complete_operation(&operation_id);
      }
    });
    Ok(ticket)
  }
}

impl Resolve<WriteArgs> for PrepareFileManagerUpload {
  async fn resolve(
    self,
    WriteArgs { user }: &WriteArgs,
  ) -> mogh_error::Result<FileManagerTransferTicket> {
    ensure_writes_enabled()?;
    let _ =
      resolve_target(&self.target, user, PermissionLevel::Write)
        .await?;
    if self.file_names.len() != 1 {
      return Err(anyhow!(
        "Prepare one upload ticket per file; clients may upload multiple files concurrently"
      )
      .into());
    }
    let total_bytes =
      self.total_bytes.context("Upload byte count is required")?;
    if self.overwrite && !self.confirmed {
      return Err(anyhow!(
        "Explicit confirmation is required to overwrite an uploaded file"
      )
      .into());
    }
    if self.overwrite && self.expected_revision.is_none() {
      return Err(
        anyhow!(
          "The current destination revision is required for overwrite"
        )
        .into(),
      );
    }
    let (token, session) = create_transfer_session(
      user.id.clone(),
      self.target,
      TransferSessionKind::Upload {
        destination: self.destination,
        file_name: self.file_names.into_iter().next().unwrap(),
        total_bytes,
        overwrite: self.overwrite,
        expected_revision: self.expected_revision,
      },
    );
    Ok(FileManagerTransferTicket {
      operation_id: session.operation_id,
      url: format!("/file-manager/upload/{token}"),
      token,
      expires_at: session.expires_at,
    })
  }
}

impl Resolve<WriteArgs> for PrepareFileManagerDownload {
  async fn resolve(
    self,
    WriteArgs { user }: &WriteArgs,
  ) -> mogh_error::Result<FileManagerTransferTicket> {
    let resolved =
      resolve_target(&self.target, user, PermissionLevel::Read)
        .await?;
    if self.paths.is_empty() {
      return Err(
        anyhow!("Select at least one entry to download").into(),
      );
    }
    if resolved.managed_file.as_ref().is_some_and(|managed| {
      self.paths.iter().any(|path| path == managed)
    }) {
      return Err(anyhow!(
        "The managed compose file is available only through the editor"
      )
      .into());
    }
    let (token, session) = create_transfer_session(
      user.id.clone(),
      self.target,
      TransferSessionKind::Download { paths: self.paths },
    );
    Ok(FileManagerTransferTicket {
      operation_id: session.operation_id,
      url: format!("/file-manager/download/{token}"),
      token,
      expires_at: session.expires_at,
    })
  }
}

async fn prepare_managed_operation(
  resolved: &ResolvedFileManagerTarget,
  operation: &FileManagerOperation,
) -> anyhow::Result<(
  FileManagerOperation,
  Option<(Stack, String, String)>,
)> {
  let FileManagerOperation::WriteText {
    path,
    contents,
    expected_revision,
  } = operation
  else {
    return Ok((operation.clone(), None));
  };
  if resolved.managed_file.as_deref() != Some(path.as_str()) {
    return Ok((operation.clone(), None));
  }
  let stack = resolved
    .stack
    .as_ref()
    .context("Managed File Manager stack is missing")?;
  if managed_revision(&stack.config.file_contents)
    != *expected_revision
  {
    return Err(anyhow!(
      "Managed compose source changed since it was opened; reload before saving"
    ));
  }
  let expected_revision = periphery_client(&resolved.server)
    .await?
    .request(periphery::ReadFileManagerText {
      target: resolved.periphery.clone(),
      path: path.clone(),
    })
    .await
    .map(|file| file.revision)
    .unwrap_or_else(|_| FileManagerRevision::default());
  let expanded =
    expand_managed_source(stack.clone(), contents.clone()).await?;
  Ok((
    FileManagerOperation::WriteText {
      path: path.clone(),
      contents: expanded,
      expected_revision,
    },
    Some((
      stack.clone(),
      stack.config.file_contents.clone(),
      contents.clone(),
    )),
  ))
}

async fn expand_managed_source(
  mut stack: Stack,
  contents: String,
) -> anyhow::Result<String> {
  stack.config.file_contents = contents;
  if !stack.config.skip_secret_interp {
    let VariablesAndSecrets { variables, secrets } =
      get_variables_and_secrets().await?;
    Interpolator::new(Some(&variables), &secrets)
      .interpolate_stack(&mut stack)?;
  }
  Ok(stack.config.file_contents)
}

async fn update_managed_source(
  stack_name: &str,
  expected: &str,
  contents: &str,
) -> anyhow::Result<()> {
  let result = db_client()
    .stacks
    .update_one(
      doc! {
        "name": stack_name,
        "config.file_contents": expected,
      },
      doc! { "$set": { "config.file_contents": contents } },
    )
    .await
    .context("Failed to update managed compose source")?;
  if result.matched_count != 1 {
    return Err(anyhow!(
      "Managed compose source changed concurrently; reload and retry"
    ));
  }
  Ok(())
}

async fn audit_result(
  target: ResourceTarget,
  description: &str,
  result: anyhow::Result<Vec<String>>,
  user: &komodo_client::entities::user::User,
) -> mogh_error::Result<Update> {
  let mut update = make_update(target, Operation::FileManager, user);
  match result {
    Ok(paths) => update.push_simple_log(
      description,
      if paths.is_empty() {
        "Operation completed".to_string()
      } else {
        format!("Affected paths: {}", paths.join(", "))
      },
    ),
    Err(error) => {
      update.push_error_log(description, format_serror(&error.into()))
    }
  }
  update.finalize();
  update.id = add_update(update.clone()).await?;
  Ok(update)
}

fn ensure_writes_enabled() -> anyhow::Result<()> {
  if core_config().ui_write_disabled {
    Err(anyhow!("UI writes are disabled by Core configuration"))
  } else {
    Ok(())
  }
}

fn history_key(target: &FileManagerTarget, actor: &str) -> String {
  format!(
    "{}:{actor}",
    serde_json::to_string(target).unwrap_or_default()
  )
}

fn push_history(
  target: &FileManagerTarget,
  actor: &str,
  entry: HistoryEntry,
) {
  let mut histories = histories().write().unwrap();
  let history =
    histories.entry(history_key(target, actor)).or_default();
  history.undo.push(entry);
  history.redo.clear();
}

fn pop_undo(
  target: &FileManagerTarget,
  actor: &str,
) -> Option<HistoryEntry> {
  histories()
    .write()
    .unwrap()
    .entry(history_key(target, actor))
    .or_default()
    .undo
    .pop()
}

fn restore_undo(
  target: &FileManagerTarget,
  actor: &str,
  entry: HistoryEntry,
) {
  histories()
    .write()
    .unwrap()
    .entry(history_key(target, actor))
    .or_default()
    .undo
    .push(entry);
}

fn push_redo(
  target: &FileManagerTarget,
  actor: &str,
  entry: HistoryEntry,
) {
  histories()
    .write()
    .unwrap()
    .entry(history_key(target, actor))
    .or_default()
    .redo
    .push(entry);
}

fn pop_redo(
  target: &FileManagerTarget,
  actor: &str,
) -> Option<HistoryEntry> {
  histories()
    .write()
    .unwrap()
    .entry(history_key(target, actor))
    .or_default()
    .redo
    .pop()
}

fn restore_redo(
  target: &FileManagerTarget,
  actor: &str,
  entry: HistoryEntry,
) {
  histories()
    .write()
    .unwrap()
    .entry(history_key(target, actor))
    .or_default()
    .redo
    .push(entry);
}
