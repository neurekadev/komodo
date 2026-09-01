use std::{
  collections::{HashMap, HashSet},
  str::FromStr,
  sync::{OnceLock, RwLock},
  time::Duration,
};

use anyhow::{Context as _, anyhow};
use database::{
  bson::doc,
  mungos::{
    find::find_collect,
    mongodb::{Collection, bson::oid::ObjectId},
  },
};
use formatting::format_serror;
use interpolate::Interpolator;
use komodo_client::{
  api::write::*,
  entities::{
    Operation, ResourceTarget,
    file_manager::{
      FileManagerOperation, FileManagerOperationState,
      FileManagerOperationStatus, FileManagerOperationTicket,
      FileManagerPreflight, FileManagerRevision, FileManagerTarget,
      FileManagerTransferTicket,
    },
    komodo_timestamp,
    permission::PermissionLevel,
    server::Server,
    stack::Stack,
    update::Update,
  },
};
use mogh_resolver::Resolve;
use periphery_client::api::file_manager as periphery;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
  config::core_config,
  file_manager::{
    ResolvedFileManagerTarget, TransferSessionKind,
    cancel_pending_transfer_session, complete_operation,
    create_operation_status, create_transfer_session, fail_operation,
    get_core_operation_status, managed_revision,
    require_managed_file, resolve_target, set_operation_finalizing,
    update_operation_status,
  },
  helpers::{
    periphery_client,
    query::{VariablesAndSecrets, get_variables_and_secrets},
    update::{add_update, make_update},
  },
  resource,
  state::db_client,
};

use super::WriteArgs;

#[derive(Clone)]
struct ManagedPlan {
  stack_id: String,
  stack_name: String,
  compose_file_path: String,
  before: String,
  after: String,
  durable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManagedFileManagerTransaction {
  #[serde(rename = "_id")]
  stack_id: String,
  stack_name: String,
  operation_id: String,
  actor: String,
  target: FileManagerTarget,
  server_id: String,
  compose_file_path: String,
  source_before: String,
  source_after: String,
  begun: bool,
  created_at: i64,
  lease_owner: String,
  lease_expires_at: i64,
  #[serde(default)]
  last_error: Option<String>,
}

const MANAGED_TRANSACTION_COLLECTION: &str =
  "FileManagerManagedTransaction";
const MANAGED_TRANSACTION_LEASE_MS: i64 = 30_000;
const MANAGED_TRANSACTION_RECONCILE_INTERVAL: Duration =
  Duration::from_secs(10);
const MANAGED_TRANSACTION_LEASE_RENEW_INTERVAL: Duration =
  Duration::from_secs(10);

fn managed_transaction_collection()
-> Collection<ManagedFileManagerTransaction> {
  db_client()
    .db
    .collection(MANAGED_TRANSACTION_COLLECTION)
}

fn managed_transaction_owner() -> &'static str {
  static OWNER: OnceLock<String> = OnceLock::new();
  OWNER.get_or_init(|| Uuid::new_v4().to_string())
}

fn live_managed_transactions() -> &'static RwLock<HashSet<String>> {
  static LIVE: OnceLock<RwLock<HashSet<String>>> = OnceLock::new();
  LIVE.get_or_init(Default::default)
}

struct LiveManagedTransaction {
  operation_id: String,
  lease_cancel: CancellationToken,
}

impl Drop for LiveManagedTransaction {
  fn drop(&mut self) {
    live_managed_transactions()
      .write()
      .unwrap()
      .remove(&self.operation_id);
    self.lease_cancel.cancel();
  }
}

async fn insert_managed_transaction(
  transaction: &ManagedFileManagerTransaction,
) -> anyhow::Result<()> {
  let collection = managed_transaction_collection();
  if collection
    .find_one(doc! { "_id": &transaction.stack_id })
    .await
    .context("Failed to check pending managed saves")?
    .is_some()
  {
    return Err(anyhow!(
      "Another managed save is still being reconciled for this stack"
    ));
  }
  collection
    .insert_one(transaction)
    .await
    .context(
      "Failed to durably record the managed save before changing the host",
    )?;
  Ok(())
}

async fn mark_managed_transaction_begun(
  stack_id: &str,
  operation_id: &str,
) -> anyhow::Result<()> {
  let result = managed_transaction_collection()
    .update_one(
      doc! { "_id": stack_id, "operation_id": operation_id },
      doc! { "$set": {
        "begun": true,
        "lease_owner": managed_transaction_owner(),
        "lease_expires_at": komodo_timestamp()
          + MANAGED_TRANSACTION_LEASE_MS,
      } },
    )
    .await
    .context("Failed to persist managed save handshake")?;
  if result.matched_count != 1 {
    return Err(anyhow!(
      "Managed save intent disappeared during its handshake"
    ));
  }
  Ok(())
}

async fn delete_managed_transaction(
  stack_id: &str,
  operation_id: &str,
) -> anyhow::Result<()> {
  managed_transaction_collection()
    .delete_one(
      doc! { "_id": stack_id, "operation_id": operation_id },
    )
    .await
    .context("Failed to retire reconciled managed save intent")?;
  Ok(())
}

fn start_live_managed_transaction(
  transaction: &ManagedFileManagerTransaction,
) -> LiveManagedTransaction {
  live_managed_transactions()
    .write()
    .unwrap()
    .insert(transaction.operation_id.clone());
  let cancel = CancellationToken::new();
  let renew_cancel = cancel.clone();
  let stack_id = transaction.stack_id.clone();
  let operation_id = transaction.operation_id.clone();
  tokio::spawn(async move {
    let mut interval = tokio::time::interval(
      MANAGED_TRANSACTION_LEASE_RENEW_INTERVAL,
    );
    interval.tick().await;
    loop {
      tokio::select! {
        _ = renew_cancel.cancelled() => break,
        _ = interval.tick() => {}
      }
      let result = managed_transaction_collection()
        .update_one(
          doc! {
            "_id": &stack_id,
            "operation_id": &operation_id,
            "lease_owner": managed_transaction_owner(),
          },
          doc! { "$set": {
            "lease_expires_at": komodo_timestamp()
              + MANAGED_TRANSACTION_LEASE_MS,
          } },
        )
        .await;
      if let Err(error) = result {
        warn!(
          "Failed to renew managed save lease {operation_id}: {error:#}"
        );
      }
    }
  });
  LiveManagedTransaction {
    operation_id: transaction.operation_id.clone(),
    lease_cancel: cancel,
  }
}

fn transaction_periphery_target(
  transaction: &ManagedFileManagerTransaction,
) -> periphery::PeripheryFileManagerTarget {
  let mut stack = Stack {
    id: transaction.stack_id.clone(),
    name: transaction.stack_name.clone(),
    ..Default::default()
  };
  stack.config.server_id = transaction.server_id.clone();
  stack.config.file_paths =
    vec![transaction.compose_file_path.clone()];
  periphery::PeripheryFileManagerTarget::Stack {
    stack: Box::new(stack),
    repo: None,
  }
}

async fn claim_managed_transaction(
  transaction: &ManagedFileManagerTransaction,
) -> anyhow::Result<bool> {
  let now = komodo_timestamp();
  let result = managed_transaction_collection()
    .update_one(
      doc! {
        "_id": &transaction.stack_id,
        "operation_id": &transaction.operation_id,
        "$or": [
          { "lease_owner": managed_transaction_owner() },
          { "lease_expires_at": { "$lte": now } },
          { "lease_expires_at": { "$exists": false } },
        ],
      },
      doc! { "$set": {
        "lease_owner": managed_transaction_owner(),
        "lease_expires_at": now + MANAGED_TRANSACTION_LEASE_MS,
      } },
    )
    .await
    .context("Failed to claim managed save reconciliation lease")?;
  Ok(result.matched_count == 1)
}

async fn managed_stack_source(
  stack_id: &str,
) -> anyhow::Result<Option<String>> {
  let id = ObjectId::from_str(stack_id)
    .context("Managed stack id is invalid")?;
  Ok(
    db_client()
      .stacks
      .find_one(doc! { "_id": id })
      .await
      .context("Failed to read managed compose source")?
      .map(|stack| stack.config.file_contents),
  )
}

async fn record_managed_transaction_error(
  transaction: &ManagedFileManagerTransaction,
  error: &anyhow::Error,
) {
  let _ = managed_transaction_collection()
    .update_one(
      doc! {
        "_id": &transaction.stack_id,
        "operation_id": &transaction.operation_id,
      },
      doc! { "$set": {
        "last_error": format!("{error:#}"),
        "lease_expires_at": komodo_timestamp()
          + MANAGED_TRANSACTION_RECONCILE_INTERVAL.as_millis() as i64,
      } },
    )
    .await;
}

pub fn spawn_managed_transaction_reconciliation_loop() {
  tokio::spawn(async {
    let mut interval = tokio::time::interval(
      MANAGED_TRANSACTION_RECONCILE_INTERVAL,
    );
    loop {
      interval.tick().await;
      let transactions = match find_collect(
        &managed_transaction_collection(),
        None,
        None,
      )
      .await
      {
        Ok(transactions) => transactions,
        Err(error) => {
          warn!(
            "Failed to list pending managed save transactions: {error:#}"
          );
          continue;
        }
      };
      for transaction in transactions {
        if live_managed_transactions()
          .read()
          .unwrap()
          .contains(&transaction.operation_id)
        {
          continue;
        }
        match claim_managed_transaction(&transaction).await {
          Ok(true) => {}
          Ok(false) => continue,
          Err(error) => {
            warn!(
              "Failed to claim managed save {}: {error:#}",
              transaction.operation_id
            );
            continue;
          }
        }
        if let Err(error) =
          reconcile_managed_transaction(&transaction).await
        {
          warn!(
            "Managed save {} still needs reconciliation: {error:#}",
            transaction.operation_id
          );
          record_managed_transaction_error(&transaction, &error)
            .await;
        }
      }
    }
  });
}

async fn finalize_reconciled_managed_transaction(
  transaction: &ManagedFileManagerTransaction,
  server: &Server,
  target: &periphery::PeripheryFileManagerTarget,
  action: periphery::FileManagerManagedTransactionFinalizeAction,
) -> anyhow::Result<()> {
  let expected = match action {
    periphery::FileManagerManagedTransactionFinalizeAction::Commit => {
      periphery::FileManagerManagedTransactionState::Committed
    }
    periphery::FileManagerManagedTransactionFinalizeAction::Rollback => {
      periphery::FileManagerManagedTransactionState::RolledBack
    }
  };
  let status = periphery_client(server)
    .await?
    .request(periphery::FinalizeManagedFileManagerTransaction {
      target: target.clone(),
      actor: transaction.actor.clone(),
      operation_id: transaction.operation_id.clone(),
      action,
    })
    .await?;
  if status.state != expected {
    return Err(anyhow!(
      "Managed reconciliation returned invalid state {:?}",
      status.state
    ));
  }
  delete_managed_transaction(
    &transaction.stack_id,
    &transaction.operation_id,
  )
  .await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManagedReconciliationAction {
  DeleteIntent,
  RollSourceForward,
  CommitHost,
  RollbackHost,
  Indeterminate,
}

fn managed_reconciliation_action(
  begun: bool,
  state: Option<periphery::FileManagerManagedTransactionState>,
  source: Option<&str>,
  before: &str,
  after: &str,
) -> ManagedReconciliationAction {
  use periphery::FileManagerManagedTransactionState as State;
  match state {
    None if begun => ManagedReconciliationAction::Indeterminate,
    None => ManagedReconciliationAction::DeleteIntent,
    Some(State::Applied) if source == Some(after) => {
      ManagedReconciliationAction::CommitHost
    }
    Some(State::Applied) if source == Some(before) => {
      ManagedReconciliationAction::RollSourceForward
    }
    Some(State::Applied) => ManagedReconciliationAction::RollbackHost,
    Some(
      State::Prepared | State::Applying | State::RollbackRequested,
    ) if source == Some(after) => {
      ManagedReconciliationAction::Indeterminate
    }
    Some(
      State::Prepared | State::Applying | State::RollbackRequested,
    ) => ManagedReconciliationAction::RollbackHost,
    Some(State::RolledBack) if source == Some(after) => {
      ManagedReconciliationAction::Indeterminate
    }
    Some(State::RolledBack) => {
      ManagedReconciliationAction::DeleteIntent
    }
    Some(State::CommitRequested | State::Committed)
      if source == Some(after) =>
    {
      ManagedReconciliationAction::CommitHost
    }
    Some(State::CommitRequested | State::Committed) => {
      ManagedReconciliationAction::Indeterminate
    }
  }
}

async fn reconcile_managed_transaction(
  transaction: &ManagedFileManagerTransaction,
) -> anyhow::Result<()> {
  let server = resource::get::<Server>(&transaction.server_id)
    .await
    .context("Managed save server is unavailable")?;
  let target = transaction_periphery_target(transaction);
  let source = managed_stack_source(&transaction.stack_id).await?;
  let status = periphery_client(&server)
    .await?
    .request(periphery::GetManagedFileManagerTransaction {
      target: target.clone(),
      actor: transaction.actor.clone(),
      operation_id: transaction.operation_id.clone(),
    })
    .await?;
  match managed_reconciliation_action(
    transaction.begun,
    status.map(|status| status.state),
    source.as_deref(),
    &transaction.source_before,
    &transaction.source_after,
  ) {
    ManagedReconciliationAction::DeleteIntent => {
      delete_managed_transaction(
        &transaction.stack_id,
        &transaction.operation_id,
      )
      .await
    }
    ManagedReconciliationAction::RollSourceForward => {
      update_managed_source(
        &transaction.stack_id,
        &transaction.source_before,
        &transaction.source_after,
      )
      .await?;
      finalize_reconciled_managed_transaction(
        transaction,
        &server,
        &target,
        periphery::FileManagerManagedTransactionFinalizeAction::Commit,
      )
      .await
    }
    ManagedReconciliationAction::CommitHost => {
      finalize_reconciled_managed_transaction(
        transaction,
        &server,
        &target,
        periphery::FileManagerManagedTransactionFinalizeAction::Commit,
      )
      .await
    }
    ManagedReconciliationAction::RollbackHost => {
      finalize_reconciled_managed_transaction(
        transaction,
        &server,
        &target,
        periphery::FileManagerManagedTransactionFinalizeAction::Rollback,
      )
      .await
    }
    ManagedReconciliationAction::Indeterminate => Err(anyhow!(
      "Managed save has contradictory or incomplete durable state; retaining it for safe retry"
    )),
  }
}

#[derive(Clone)]
struct CoreFileManagerPlan {
  actor: String,
  target: FileManagerTarget,
  expires_at: i64,
  managed: Option<ManagedPlan>,
}

fn file_manager_plans()
-> &'static RwLock<HashMap<String, CoreFileManagerPlan>> {
  static PLANS: OnceLock<
    RwLock<HashMap<String, CoreFileManagerPlan>>,
  > = OnceLock::new();
  PLANS.get_or_init(Default::default)
}

fn take_file_manager_plan(
  plan_id: &str,
  actor: &str,
  target: &FileManagerTarget,
  now: i64,
) -> anyhow::Result<CoreFileManagerPlan> {
  let mut plans = file_manager_plans().write().unwrap();
  let Some(plan) = plans.get(plan_id) else {
    return Err(anyhow!(
      "Preflight plan is missing, expired, or already consumed; retry preflight"
    ));
  };
  if plan.actor != actor {
    return Err(anyhow!("Preflight plan belongs to another user"));
  }
  if &plan.target != target {
    return Err(anyhow!(
      "Preflight plan belongs to another File Manager target"
    ));
  }
  if plan.expires_at < now {
    plans.remove(plan_id);
    return Err(anyhow!(
      "Preflight plan has expired; retry preflight"
    ));
  }
  plans
    .remove(plan_id)
    .context("Preflight plan disappeared while consuming it")
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
    let managed = managed.map(|(stack, before, after)| ManagedPlan {
      stack_id: stack.id,
      stack_name: stack.name,
      compose_file_path: stack
        .config
        .file_paths
        .first()
        .cloned()
        .unwrap_or_else(|| "compose.yaml".into()),
      before,
      after,
      durable: response.supports_durable_managed_transactions,
    });
    let mut plans = file_manager_plans().write().unwrap();
    plans.retain(|_, plan| plan.expires_at > komodo_timestamp());
    plans.insert(
      response.plan_id.clone(),
      CoreFileManagerPlan {
        actor: user.id.clone(),
        target: self.target.clone(),
        expires_at: response.expires_at,
        managed,
      },
    );
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
    let managed = take_file_manager_plan(
      &self.plan_id,
      &user.id,
      &self.target,
      komodo_timestamp(),
    )?
    .managed;
    let operation_id = create_operation_status(
      user.id.clone(),
      self.target.clone(),
      "File operation",
    );
    let ticket = FileManagerOperationTicket {
      operation_id: operation_id.clone(),
    };
    let durable_transaction = managed
      .as_ref()
      .filter(|plan| plan.durable)
      .map(|plan| ManagedFileManagerTransaction {
        stack_id: plan.stack_id.clone(),
        stack_name: plan.stack_name.clone(),
        operation_id: operation_id.clone(),
        actor: user.id.clone(),
        target: self.target.clone(),
        server_id: resolved.server.id.clone(),
        compose_file_path: plan.compose_file_path.clone(),
        source_before: plan.before.clone(),
        source_after: plan.after.clone(),
        begun: false,
        created_at: komodo_timestamp(),
        lease_owner: managed_transaction_owner().to_string(),
        lease_expires_at: komodo_timestamp()
          + MANAGED_TRANSACTION_LEASE_MS,
        last_error: None,
      });
    let live_managed = if let Some(transaction) = &durable_transaction
    {
      if let Err(error) =
        insert_managed_transaction(transaction).await
      {
        fail_operation(&operation_id, format!("{error:#}"));
        return Err(error.into());
      }
      Some(start_live_managed_transaction(transaction))
    } else {
      None
    };
    let durable_stack_id = durable_transaction
      .as_ref()
      .map(|transaction| transaction.stack_id.clone());
    let actor = user.clone();
    let target = self.target;
    tokio::spawn(async move {
      let _live_managed = live_managed;
      let result = async {
      let client = periphery_client(&resolved.server).await?;
      if let Some(stack_id) = durable_stack_id.as_deref() {
        let begin = client
          .request(periphery::BeginManagedFileManagerTransaction {
            target: resolved.periphery.clone(),
            actor: actor.id.clone(),
            operation_id: operation_id.clone(),
            plan_id: self.plan_id.clone(),
          })
          .await;
        if let Err(error) = begin {
          let rollback = rollback_managed_host_operation(
            &resolved,
            &actor.id,
            &operation_id,
            Some(stack_id),
          )
          .await;
          return Err(managed_failure_after_rollback(
            error,
            rollback,
            "Managed save handshake was cancelled before the host write",
            false,
          ));
        }
        if let Err(error) = mark_managed_transaction_begun(
          stack_id,
          &operation_id,
        )
        .await
        {
          let rollback = rollback_managed_host_operation(
            &resolved,
            &actor.id,
            &operation_id,
            Some(stack_id),
          )
          .await;
          return Err(managed_failure_after_rollback(
            error,
            rollback,
            "Managed save handshake was rolled back after Core could not persist it",
            false,
          ));
        }
      }
      let response = client
        .request(periphery::CommitFileManagerOperation {
          target: resolved.periphery.clone(),
          actor: actor.id.clone(),
          operation_id: operation_id.clone(),
          plan_id: self.plan_id,
          decisions: self.decisions,
          confirmed: self.confirmed,
          durable_managed: durable_stack_id.is_some(),
        })
        .await;
      let response = match response {
        Ok(response) => response,
        Err(error) if managed.is_some() => {
          let original_status = monitor_periphery_operation(
            &resolved,
            &actor.id,
            &operation_id,
          )
          .await;
          match original_status {
            Ok(status) => {
              let decision = match managed_request_recovery_decision(
                &status,
              ) {
                Ok(decision) => decision,
                Err(status_error) => {
                  return Err(anyhow!(
                    "Managed commit request failed: {error:#}; Periphery returned an invalid recovery status: {status_error:#}"
                  ));
                }
              };
              let (error, rollback_context, unavailable_is_safe) =
                match decision {
                  ManagedRequestRecoveryDecision::RequireRollback => (
                    error,
                    "Managed host write was rolled back after the commit response was lost",
                    false,
                  ),
                  ManagedRequestRecoveryDecision::RollbackIfAvailable => {
                    let terminal =
                      file_operation_terminal_error(status);
                    (
                      anyhow!(
                        "Managed commit request failed: {error:#}; Periphery terminalized the operation without completing: {terminal:#}"
                      ),
                      "Retained managed host changes were rolled back after the operation did not complete",
                      true,
                    )
                  }
                };
              let rollback = rollback_managed_host_operation(
                &resolved,
                &actor.id,
                &operation_id,
                durable_stack_id.as_deref(),
              )
              .await;
              return Err(managed_failure_after_rollback(
                error,
                rollback,
                rollback_context,
                unavailable_is_safe,
              ));
            }
            Err(status_error) => {
              let rollback = rollback_managed_host_operation(
                &resolved,
                &actor.id,
                &operation_id,
                durable_stack_id.as_deref(),
              )
              .await;
              let error = anyhow!(
                "Managed commit request failed: {error:#}; original operation status remained unavailable: {status_error:#}"
              );
              return Err(managed_failure_after_rollback(
                error,
                rollback,
                "Managed host write was rolled back after the commit response and operation status were lost",
                false,
              ));
            }
          }
        }
        Err(error) => return Err(error),
      };
      let status = match monitor_periphery_operation(
        &resolved,
        &actor.id,
        &operation_id,
      )
      .await
      {
        Ok(status) => status,
        Err(error) if managed.is_some() => {
          let rollback = rollback_managed_host_operation(
            &resolved,
            &actor.id,
            &operation_id,
            durable_stack_id.as_deref(),
          )
          .await;
          return Err(managed_failure_after_rollback(
            error,
            rollback,
            "Managed host write was rolled back after Periphery operation status was lost",
            false,
          ));
        }
        Err(error) => return Err(error),
      };
      if status.state != FileManagerOperationState::Complete {
        let error = file_operation_terminal_error(status);
        if managed.is_some() {
          let rollback = rollback_managed_host_operation(
            &resolved,
            &actor.id,
            &operation_id,
            durable_stack_id.as_deref(),
          )
          .await;
          return Err(managed_failure_after_rollback(
            error,
            rollback,
            "Managed host changes were rolled back after the file operation did not complete",
            true,
          ));
        }
        return Err(error);
      }
      set_operation_finalizing(&operation_id);
      if let Some(plan) = &managed {
        if let Err(error) = update_managed_source(
          &plan.stack_id,
          &plan.before,
          &plan.after,
        )
        .await
        {
          let rollback = rollback_managed_host_operation(
            &resolved,
            &actor.id,
            &operation_id,
            durable_stack_id.as_deref(),
          )
          .await;
          return Err(managed_failure_after_rollback(
            error,
            rollback,
            "Host write was rolled back after the managed source update failed",
            false,
          ));
        }
        if plan.durable {
          commit_durable_managed_transaction(
            &resolved,
            &actor.id,
            &operation_id,
            &plan.stack_id,
          )
          .await?;
        }
      }
      anyhow::Ok(response.affected_paths)
    }
    .await;
      let operation_error =
        result.as_ref().err().map(|error| format!("{error:#}"));
      let audit = audit_result(
        resolved.resource,
        "File operation",
        result,
        &actor,
      )
      .await;
      let cancelled =
        get_core_operation_status(&operation_id, &actor.id, &target)
          .is_some_and(|status| {
            status.state == FileManagerOperationState::Cancelled
          });
      if cancelled {
        // The authoritative Periphery terminal state is already cached.
      } else if let Some(error) = operation_error {
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

impl Resolve<WriteArgs> for ResolveFileManagerOperationConflict {
  async fn resolve(
    self,
    WriteArgs { user }: &WriteArgs,
  ) -> mogh_error::Result<FileManagerOperationStatus> {
    ensure_writes_enabled()?;
    let resolved =
      resolve_target(&self.target, user, PermissionLevel::Write)
        .await?;
    let status = periphery_client(&resolved.server)
      .await?
      .request(periphery::ResolveFileManagerOperationConflict {
        target: resolved.periphery,
        actor: user.id.clone(),
        operation_id: self.operation_id.clone(),
        decision_id: self.decision_id,
        action: self.action,
        apply_to_all: self.apply_to_all,
      })
      .await?;
    update_operation_status(&self.operation_id, status.clone());
    Ok(status)
  }
}

impl Resolve<WriteArgs> for CancelFileManagerOperation {
  async fn resolve(
    self,
    WriteArgs { user }: &WriteArgs,
  ) -> mogh_error::Result<FileManagerOperationStatus> {
    let resolved =
      resolve_target(&self.target, user, PermissionLevel::Read)
        .await?;
    if let Some(status) = cancel_pending_transfer_session(
      &self.operation_id,
      &user.id,
      &self.target,
    ) {
      return Ok(status);
    }
    let status = periphery_client(&resolved.server)
      .await?
      .request(periphery::CancelFileManagerOperation {
        target: resolved.periphery,
        actor: user.id.clone(),
        operation_id: self.operation_id.clone(),
      })
      .await?;
    update_operation_status(&self.operation_id, status.clone());
    Ok(status)
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
    let operation_id = create_operation_status(
      user.id.clone(),
      self.target.clone(),
      "Undo file operation",
    );
    let ticket = FileManagerOperationTicket {
      operation_id: operation_id.clone(),
    };
    let actor = user.clone();
    tokio::spawn(async move {
      let result = async {
        let response = periphery_client(&resolved.server)
          .await?
          .request(periphery::UndoFileManagerOperation {
            target: resolved.periphery.clone(),
            actor: actor.id.clone(),
            operation_id: operation_id.clone(),
            confirmed: self.confirmed,
            rollback_operation_id: None,
          })
          .await?;
        let terminal = monitor_periphery_operation(
          &resolved,
          &actor.id,
          &operation_id,
        )
        .await;
        match terminal {
          Ok(status)
            if status.state
              == FileManagerOperationState::Complete => {}
          Ok(status) => {
            return Err(anyhow!(status.error.unwrap_or_else(|| {
              "Undo operation did not complete".into()
            })));
          }
          Err(error) => return Err(error),
        }
        set_operation_finalizing(&operation_id);
        anyhow::Ok(response.affected_paths)
      }
      .await;
      let operation_error =
        result.as_ref().err().map(|error| format!("{error:#}"));
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
    let operation_id = create_operation_status(
      user.id.clone(),
      self.target.clone(),
      "Redo file operation",
    );
    let ticket = FileManagerOperationTicket {
      operation_id: operation_id.clone(),
    };
    let actor = user.clone();
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
          .await?;
        let terminal = monitor_periphery_operation(
          &resolved,
          &actor.id,
          &operation_id,
        )
        .await;
        match terminal {
          Ok(status)
            if status.state
              == FileManagerOperationState::Complete => {}
          Ok(status) => {
            return Err(anyhow!(status.error.unwrap_or_else(|| {
              "Redo operation did not complete".into()
            })));
          }
          Err(error) => return Err(error),
        }
        set_operation_finalizing(&operation_id);
        anyhow::Ok(response.affected_paths)
      }
      .await;
      let operation_error =
        result.as_ref().err().map(|error| format!("{error:#}"));
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
      TransferSessionKind::Download {
        paths: self.paths,
        allow_managed: false,
      },
    );
    Ok(FileManagerTransferTicket {
      operation_id: session.operation_id,
      url: format!("/file-manager/download/{token}"),
      token,
      expires_at: session.expires_at,
    })
  }
}

impl Resolve<WriteArgs>
  for PrepareManagedFileManagerRenderedDownload
{
  async fn resolve(
    self,
    WriteArgs { user }: &WriteArgs,
  ) -> mogh_error::Result<FileManagerTransferTicket> {
    let resolved =
      resolve_target(&self.target, user, PermissionLevel::Read)
        .await?;
    let path = require_managed_file(&resolved)?;
    let (token, session) = create_transfer_session(
      user.id.clone(),
      self.target,
      TransferSessionKind::Download {
        paths: vec![path],
        allow_managed: true,
      },
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
  stack_id: &str,
  expected: &str,
  contents: &str,
) -> anyhow::Result<()> {
  let stack_id = ObjectId::from_str(stack_id)
    .context("Managed stack id is invalid")?;
  let result = db_client()
    .stacks
    .update_one(
      doc! {
        "_id": stack_id,
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

const MANAGED_ROLLBACK_UNAVAILABLE: &str =
  "Requested File Manager rollback is unavailable";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManagedRollbackOutcome {
  Complete,
  Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManagedRequestRecoveryDecision {
  RequireRollback,
  RollbackIfAvailable,
}

fn managed_request_recovery_decision(
  status: &FileManagerOperationStatus,
) -> anyhow::Result<ManagedRequestRecoveryDecision> {
  match status.state {
    FileManagerOperationState::Complete => {
      Ok(ManagedRequestRecoveryDecision::RequireRollback)
    }
    FileManagerOperationState::Failed
    | FileManagerOperationState::Cancelled => {
      Ok(ManagedRequestRecoveryDecision::RollbackIfAvailable)
    }
    _ => Err(anyhow!(
      "Managed commit recovery received a non-terminal operation state"
    )),
  }
}

fn file_operation_terminal_error(
  status: FileManagerOperationStatus,
) -> anyhow::Error {
  match status.state {
    FileManagerOperationState::Cancelled => anyhow!(
      status
        .error
        .unwrap_or_else(|| "File operation was cancelled".into())
    ),
    FileManagerOperationState::Failed => anyhow!(
      status
        .error
        .unwrap_or_else(|| "File operation failed".into())
    ),
    _ => anyhow!("File operation ended in an invalid state"),
  }
}

fn managed_rollback_status_outcome(
  status: FileManagerOperationStatus,
) -> anyhow::Result<ManagedRollbackOutcome> {
  match status.state {
    FileManagerOperationState::Complete => {
      Ok(ManagedRollbackOutcome::Complete)
    }
    FileManagerOperationState::Cancelled => Err(anyhow!(
      "Exact managed-file rollback was cancelled: {}",
      status
        .error
        .unwrap_or_else(|| "unknown cancellation reason".into())
    )),
    FileManagerOperationState::Failed
      if status.error.as_deref().is_some_and(|error| {
        error.contains(MANAGED_ROLLBACK_UNAVAILABLE)
      }) =>
    {
      Ok(ManagedRollbackOutcome::Unavailable)
    }
    FileManagerOperationState::Failed => Err(anyhow!(
      "Exact managed-file rollback failed: {}",
      status
        .error
        .unwrap_or_else(|| "unknown rollback failure".into())
    )),
    _ => Err(anyhow!(
      "Exact managed-file rollback ended in an invalid state"
    )),
  }
}

async fn rollback_managed_host_operation(
  resolved: &ResolvedFileManagerTarget,
  actor: &str,
  source_operation_id: &str,
  durable_stack_id: Option<&str>,
) -> anyhow::Result<ManagedRollbackOutcome> {
  if let Some(stack_id) = durable_stack_id {
    let status = periphery_client(&resolved.server)
      .await?
      .request(periphery::FinalizeManagedFileManagerTransaction {
        target: resolved.periphery.clone(),
        actor: actor.to_string(),
        operation_id: source_operation_id.to_string(),
        action: periphery::FileManagerManagedTransactionFinalizeAction::Rollback,
      })
      .await
      .context("Crash-durable managed rollback failed")?;
    if status.state
      != periphery::FileManagerManagedTransactionState::RolledBack
    {
      return Err(anyhow!(
        "Crash-durable managed rollback returned invalid state {:?}",
        status.state
      ));
    }
    delete_managed_transaction(stack_id, source_operation_id)
      .await?;
    return Ok(ManagedRollbackOutcome::Complete);
  }
  let rollback_id = Uuid::new_v4().to_string();
  let request = periphery::UndoFileManagerOperation {
    target: resolved.periphery.clone(),
    actor: actor.to_string(),
    operation_id: rollback_id.clone(),
    confirmed: true,
    rollback_operation_id: Some(source_operation_id.to_string()),
  };
  let accepted = async {
    periphery_client(&resolved.server)
      .await?
      .request(request)
      .await
      .map(|_| ())
  }
  .await;
  if let Err(accept_error) = accepted {
    if format!("{accept_error:#}")
      .contains(MANAGED_ROLLBACK_UNAVAILABLE)
    {
      return Ok(ManagedRollbackOutcome::Unavailable);
    }
    return match monitor_periphery_operation(
      resolved,
      actor,
      &rollback_id,
    )
    .await
    {
      Ok(status) => managed_rollback_status_outcome(status).map_err(
        |status_error| {
          anyhow!(
            "Exact managed-file rollback request failed: {accept_error:#}; rollback operation also failed: {status_error:#}"
          )
        },
      ),
      Err(status_error) => Err(anyhow!(
        "Exact managed-file rollback request failed: {accept_error:#}; rollback status was also unavailable: {status_error:#}"
      )),
    };
  }
  let rollback =
    monitor_periphery_operation(resolved, actor, &rollback_id)
      .await
      .context("Exact managed-file rollback status was lost")?;
  managed_rollback_status_outcome(rollback)
}

async fn commit_durable_managed_transaction(
  resolved: &ResolvedFileManagerTarget,
  actor: &str,
  operation_id: &str,
  stack_id: &str,
) -> anyhow::Result<()> {
  let status = periphery_client(&resolved.server)
    .await?
    .request(periphery::FinalizeManagedFileManagerTransaction {
      target: resolved.periphery.clone(),
      actor: actor.to_string(),
      operation_id: operation_id.to_string(),
      action: periphery::FileManagerManagedTransactionFinalizeAction::Commit,
    })
    .await
    .context("Crash-durable managed commit acknowledgement failed")?;
  if status.state
    != periphery::FileManagerManagedTransactionState::Committed
  {
    return Err(anyhow!(
      "Crash-durable managed commit returned invalid state {:?}",
      status.state
    ));
  }
  delete_managed_transaction(stack_id, operation_id).await
}

fn managed_failure_after_rollback(
  primary: anyhow::Error,
  rollback: anyhow::Result<ManagedRollbackOutcome>,
  rollback_success_context: &'static str,
  unavailable_is_safe: bool,
) -> anyhow::Error {
  match rollback {
    Ok(ManagedRollbackOutcome::Complete) => {
      primary.context(rollback_success_context)
    }
    Ok(ManagedRollbackOutcome::Unavailable)
      if unavailable_is_safe =>
    {
      primary
    }
    Ok(ManagedRollbackOutcome::Unavailable) => anyhow!(
      "Managed operation failed: {primary:#}; exact host rollback was unavailable"
    ),
    Err(rollback_error) => anyhow!(
      "Managed operation failed: {primary:#}; exact host rollback also failed: {rollback_error:#}"
    ),
  }
}

async fn monitor_periphery_operation(
  resolved: &ResolvedFileManagerTarget,
  actor: &str,
  operation_id: &str,
) -> anyhow::Result<FileManagerOperationStatus> {
  let mut failures = 0_u32;
  loop {
    match periphery_client(&resolved.server).await {
      Ok(client) => match client
        .request(periphery::GetFileManagerOperationStatus {
          target: resolved.periphery.clone(),
          actor: actor.to_string(),
          operation_id: operation_id.to_string(),
        })
        .await
      {
        Ok(status) => {
          failures = 0;
          if status.state == FileManagerOperationState::Complete {
            let mut finalizing = status.clone();
            finalizing.state = FileManagerOperationState::Running;
            finalizing.phase =
              komodo_client::entities::file_manager::FileManagerOperationPhase::Finalizing;
            finalizing.cancellable = false;
            update_operation_status(operation_id, finalizing);
            return Ok(status);
          }
          update_operation_status(operation_id, status.clone());
          if matches!(
            status.state,
            FileManagerOperationState::Failed
              | FileManagerOperationState::Cancelled
          ) {
            return Ok(status);
          }
        }
        Err(error) => {
          failures = failures.saturating_add(1);
          if failures >= 12 {
            return Err(anyhow!(
              "Periphery operation status became unavailable; Periphery may have restarted: {error:#}"
            ));
          }
        }
      },
      Err(error) => {
        failures = failures.saturating_add(1);
        if failures >= 12 {
          return Err(anyhow!(
            "Periphery operation status became unavailable; Periphery may have restarted: {error:#}"
          ));
        }
      }
    }
    let delay = 1_u64 << failures.min(3);
    tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
  }
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

#[cfg(test)]
mod tests {
  use super::*;

  fn test_target(stack: &str) -> FileManagerTarget {
    FileManagerTarget::Stack {
      stack: stack.into(),
    }
  }

  fn test_core_plan(
    actor: &str,
    managed: bool,
  ) -> CoreFileManagerPlan {
    CoreFileManagerPlan {
      actor: actor.into(),
      target: test_target("stack"),
      expires_at: i64::MAX,
      managed: managed.then(|| ManagedPlan {
        stack_id: "stack-id".into(),
        stack_name: "stack".into(),
        compose_file_path: "compose.yaml".into(),
        before: "before".into(),
        after: "after".into(),
        durable: true,
      }),
    }
  }

  #[test]
  fn missing_preflight_plan_is_rejected() {
    let error = take_file_manager_plan(
      &Uuid::new_v4().to_string(),
      "owner",
      &test_target("stack"),
      0,
    )
    .err()
    .unwrap();

    assert!(error.to_string().contains("missing"));
  }

  #[test]
  fn ordinary_preflight_plan_is_consumed() {
    let plan_id = Uuid::new_v4().to_string();
    file_manager_plans()
      .write()
      .unwrap()
      .insert(plan_id.clone(), test_core_plan("owner", false));

    let plan = take_file_manager_plan(
      &plan_id,
      "owner",
      &test_target("stack"),
      0,
    )
    .unwrap();
    assert!(plan.managed.is_none());
    assert!(
      !file_manager_plans().read().unwrap().contains_key(&plan_id)
    );
  }

  #[test]
  fn wrong_actor_cannot_consume_a_managed_preflight_plan() {
    let plan_id = Uuid::new_v4().to_string();
    file_manager_plans()
      .write()
      .unwrap()
      .insert(plan_id.clone(), test_core_plan("owner", true));

    assert!(
      take_file_manager_plan(
        &plan_id,
        "other",
        &test_target("stack"),
        0,
      )
      .err()
      .unwrap()
      .to_string()
      .contains("another user")
    );
    let plan = take_file_manager_plan(
      &plan_id,
      "owner",
      &test_target("stack"),
      0,
    )
    .unwrap();
    assert_eq!(plan.actor, "owner");
    assert!(plan.managed.is_some());
  }

  #[test]
  fn wrong_target_cannot_consume_a_preflight_plan() {
    let plan_id = Uuid::new_v4().to_string();
    file_manager_plans()
      .write()
      .unwrap()
      .insert(plan_id.clone(), test_core_plan("owner", true));

    let error = take_file_manager_plan(
      &plan_id,
      "owner",
      &test_target("other-stack"),
      0,
    )
    .err()
    .unwrap();
    assert!(
      error.to_string().contains("another File Manager target")
    );

    let plan = take_file_manager_plan(
      &plan_id,
      "owner",
      &test_target("stack"),
      0,
    )
    .unwrap();
    assert!(plan.managed.is_some());
  }

  #[test]
  fn managed_recovery_reports_primary_after_successful_rollback() {
    let error = managed_failure_after_rollback(
      anyhow!("operation status was lost"),
      Ok(ManagedRollbackOutcome::Complete),
      "host write was rolled back",
      false,
    );

    let message = format!("{error:#}");
    assert!(message.contains("host write was rolled back"));
    assert!(message.contains("operation status was lost"));
  }

  #[test]
  fn managed_recovery_reports_primary_and_rollback_failures() {
    let error = managed_failure_after_rollback(
      anyhow!("operation status was lost"),
      Err(anyhow!("requested rollback is unavailable")),
      "unused success context",
      false,
    );

    let message = format!("{error:#}");
    assert!(message.contains("operation status was lost"));
    assert!(message.contains("requested rollback is unavailable"));
  }

  #[test]
  fn known_terminal_failure_tolerates_an_absent_exact_rollback() {
    let error = managed_failure_after_rollback(
      anyhow!("file operation failed"),
      Ok(ManagedRollbackOutcome::Unavailable),
      "unused success context",
      true,
    );

    assert_eq!(error.to_string(), "file operation failed");
  }

  #[test]
  fn monitor_uncertainty_reports_an_absent_exact_rollback() {
    let error = managed_failure_after_rollback(
      anyhow!("operation status was lost"),
      Ok(ManagedRollbackOutcome::Unavailable),
      "unused success context",
      false,
    );

    let message = format!("{error:#}");
    assert!(message.contains("operation status was lost"));
    assert!(message.contains("exact host rollback was unavailable"));
  }

  #[test]
  fn request_uncertainty_retains_primary_after_compensation() {
    let error = managed_failure_after_rollback(
      anyhow!("commit response was lost"),
      Ok(ManagedRollbackOutcome::Complete),
      "host write was rolled back after request uncertainty",
      false,
    );

    let message = format!("{error:#}");
    assert!(message.contains("commit response was lost"));
    assert!(message.contains(
      "host write was rolled back after request uncertainty"
    ));
  }

  #[test]
  fn request_recovery_waits_for_a_terminal_decision() {
    let status = |state| FileManagerOperationStatus {
      state,
      ..Default::default()
    };

    assert_eq!(
      managed_request_recovery_decision(&status(
        FileManagerOperationState::Complete
      ))
      .unwrap(),
      ManagedRequestRecoveryDecision::RequireRollback
    );
    assert_eq!(
      managed_request_recovery_decision(&status(
        FileManagerOperationState::Failed
      ))
      .unwrap(),
      ManagedRequestRecoveryDecision::RollbackIfAvailable
    );
    assert_eq!(
      managed_request_recovery_decision(&status(
        FileManagerOperationState::Cancelled
      ))
      .unwrap(),
      ManagedRequestRecoveryDecision::RollbackIfAvailable
    );
    assert!(
      managed_request_recovery_decision(&status(
        FileManagerOperationState::Running
      ))
      .is_err()
    );
  }

  #[test]
  fn exact_rollback_unavailable_terminal_is_classified_separately() {
    let outcome =
      managed_rollback_status_outcome(FileManagerOperationStatus {
        state: FileManagerOperationState::Failed,
        error: Some(format!(
          "Undo failed: {MANAGED_ROLLBACK_UNAVAILABLE}"
        )),
        ..Default::default()
      })
      .unwrap();

    assert_eq!(outcome, ManagedRollbackOutcome::Unavailable);
  }
}
