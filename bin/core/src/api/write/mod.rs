use std::sync::Arc;

use anyhow::Context;
use axum::{
  Extension, Router, extract::Path, middleware, routing::post,
};
use komodo_client::{api::write::*, entities::user::User};
use mogh_auth_server::middleware::authenticate_request;
use mogh_error::Json;
use mogh_error::Response;
use mogh_resolver::Resolve;
use serde::{Deserialize, Serialize};
use serde_json::json;
use strum::Display;
use strum::EnumDiscriminants;
use typeshare::typeshare;
use uuid::Uuid;

use crate::auth::KomodoAuthImpl;

use super::Variant;

mod action;
mod alert;
mod alerter;
mod backup;
mod build;
mod builder;
mod deployment;
mod file_manager;
mod onboarding;
mod permissions;
mod procedure;
mod provider;
mod repo;
mod resource;
mod server;
mod service_user;
mod stack;
mod swarm;
mod sync;
mod tag;
mod terminal;
mod user;
mod user_group;
mod variable;

pub use {
  deployment::check_deployment_for_update_inner,
  file_manager::spawn_managed_transaction_reconciliation_loop,
  stack::check_stack_for_update_inner,
};

pub struct WriteArgs {
  pub user: User,
}

tokio::task_local! {
  /// Set while the write router owns the backup mutation barrier's read side.
  /// Synchronous nested executions reuse that ownership instead of attempting
  /// a recursive Tokio RwLock read after a writer has queued.
  static WRITE_MUTATION_GUARD_HELD: WriteMutationGuard;
}

type WriteMutationGuard = Arc<tokio::sync::OwnedRwLockReadGuard<()>>;

pub(super) fn mutation_guard_held_by_write_request() -> bool {
  WRITE_MUTATION_GUARD_HELD.try_with(|_| ()).is_ok()
}

/// Share the current lease instead of recursively reading behind a queued
/// writer. Direct resolver callers acquire their own lease before mutation.
pub(super) async fn owned_write_mutation_guard() -> WriteMutationGuard
{
  if let Ok(guard) = WRITE_MUTATION_GUARD_HELD.try_with(Arc::clone) {
    return guard;
  }
  Arc::new(
    crate::backup::mutation_barrier().clone().read_owned().await,
  )
}

pub(super) fn spawn_guarded_write_job(
  guard: WriteMutationGuard,
  job: impl std::future::Future<Output = ()> + Send + 'static,
) -> tokio::task::JoinHandle<()> {
  tokio::spawn(WRITE_MUTATION_GUARD_HELD.scope(guard, job))
}

#[typeshare]
#[derive(
  Serialize, Deserialize, Debug, Clone, Resolve, EnumDiscriminants,
)]
#[strum_discriminants(name(WriteRequestMethod), derive(Display))]
#[args(WriteArgs)]
#[response(Response)]
#[error(mogh_error::Error)]
#[serde(tag = "type", content = "params")]
pub enum WriteRequest {
  // ==== BACKUPS ====
  UpdateBackupSettings(UpdateBackupSettings),
  InitializeBackupRepositories(InitializeBackupRepositories),
  RunBackup(RunBackup),
  PlanBackupRestore(PlanBackupRestore),
  ExecuteBackupRestore(ExecuteBackupRestore),
  VerifyBackupRepository(VerifyBackupRepository),
  PromoteBackupMirror(PromoteBackupMirror),
  CancelBackupRun(CancelBackupRun),
  PlanCoreRecovery(PlanCoreRecovery),
  ExecuteCoreRecovery(ExecuteCoreRecovery),

  // ==== RESOURCE ====
  UpdateResourceMeta(UpdateResourceMeta),

  // ==== SWARM ====
  CreateSwarm(CreateSwarm),
  CopySwarm(CopySwarm),
  DeleteSwarm(DeleteSwarm),
  UpdateSwarm(UpdateSwarm),
  RenameSwarm(RenameSwarm),

  // ==== SERVER ====
  CreateServer(CreateServer),
  CopyServer(CopyServer),
  DeleteServer(DeleteServer),
  UpdateServer(UpdateServer),
  RenameServer(RenameServer),
  CreateNetwork(CreateNetwork),
  UpdateServerPublicKey(UpdateServerPublicKey),
  RotateServerKeys(RotateServerKeys),

  // ==== TERMINAL ====
  CreateTerminal(CreateTerminal),
  DeleteTerminal(DeleteTerminal),
  DeleteAllTerminals(DeleteAllTerminals),
  BatchDeleteAllTerminals(BatchDeleteAllTerminals),

  // ==== STACK ====
  CreateStack(CreateStack),
  CopyStack(CopyStack),
  DeleteStack(DeleteStack),
  UpdateStack(UpdateStack),
  RenameStack(RenameStack),
  WriteStackFileContents(WriteStackFileContents),
  RefreshStackCache(RefreshStackCache),
  CheckStackForUpdate(CheckStackForUpdate),
  BatchCheckStackForUpdate(BatchCheckStackForUpdate),

  // ==== FILE MANAGER ====
  PreflightFileManagerOperation(PreflightFileManagerOperation),
  CommitFileManagerOperation(CommitFileManagerOperation),
  ResolveFileManagerOperationConflict(
    ResolveFileManagerOperationConflict,
  ),
  CancelFileManagerOperation(CancelFileManagerOperation),
  PrepareFileManagerUpload(PrepareFileManagerUpload),
  PrepareFileManagerDownload(PrepareFileManagerDownload),
  PrepareManagedFileManagerRenderedDownload(
    PrepareManagedFileManagerRenderedDownload,
  ),
  UndoFileManagerOperation(UndoFileManagerOperation),
  RedoFileManagerOperation(RedoFileManagerOperation),

  // ==== DEPLOYMENT ====
  CreateDeployment(CreateDeployment),
  CopyDeployment(CopyDeployment),
  CreateDeploymentFromContainer(CreateDeploymentFromContainer),
  DeleteDeployment(DeleteDeployment),
  UpdateDeployment(UpdateDeployment),
  RenameDeployment(RenameDeployment),
  CheckDeploymentForUpdate(CheckDeploymentForUpdate),
  BatchCheckDeploymentForUpdate(BatchCheckDeploymentForUpdate),

  // ==== BUILD ====
  CreateBuild(CreateBuild),
  CopyBuild(CopyBuild),
  DeleteBuild(DeleteBuild),
  UpdateBuild(UpdateBuild),
  RenameBuild(RenameBuild),
  WriteBuildFileContents(WriteBuildFileContents),
  RefreshBuildCache(RefreshBuildCache),

  // ==== REPO ====
  CreateRepo(CreateRepo),
  CopyRepo(CopyRepo),
  DeleteRepo(DeleteRepo),
  UpdateRepo(UpdateRepo),
  RenameRepo(RenameRepo),
  RefreshRepoCache(RefreshRepoCache),

  // ==== PROCEDURE ====
  CreateProcedure(CreateProcedure),
  CopyProcedure(CopyProcedure),
  DeleteProcedure(DeleteProcedure),
  UpdateProcedure(UpdateProcedure),
  RenameProcedure(RenameProcedure),

  // ==== ACTION ====
  CreateAction(CreateAction),
  CopyAction(CopyAction),
  DeleteAction(DeleteAction),
  UpdateAction(UpdateAction),
  RenameAction(RenameAction),

  // ==== SYNC ====
  CreateResourceSync(CreateResourceSync),
  CopyResourceSync(CopyResourceSync),
  DeleteResourceSync(DeleteResourceSync),
  UpdateResourceSync(UpdateResourceSync),
  RenameResourceSync(RenameResourceSync),
  WriteSyncFileContents(WriteSyncFileContents),
  CommitSync(CommitSync),
  RefreshResourceSyncPending(RefreshResourceSyncPending),

  // ==== BUILDER ====
  CreateBuilder(CreateBuilder),
  CopyBuilder(CopyBuilder),
  DeleteBuilder(DeleteBuilder),
  UpdateBuilder(UpdateBuilder),
  RenameBuilder(RenameBuilder),

  // ==== ALERTER ====
  CreateAlerter(CreateAlerter),
  CopyAlerter(CopyAlerter),
  DeleteAlerter(DeleteAlerter),
  UpdateAlerter(UpdateAlerter),
  RenameAlerter(RenameAlerter),

  // ==== ONBOARDING KEY ====
  CreateOnboardingKey(CreateOnboardingKey),
  UpdateOnboardingKey(UpdateOnboardingKey),
  DeleteOnboardingKey(DeleteOnboardingKey),

  // ==== USER ====
  PushRecentlyViewed(PushRecentlyViewed),
  SetLastSeenUpdate(SetLastSeenUpdate),
  SetFileManagerSafeMode(SetFileManagerSafeMode),
  CreateLocalUser(CreateLocalUser),
  DeleteUser(DeleteUser),

  // ==== SERVICE USER ====
  CreateServiceUser(CreateServiceUser),
  UpdateServiceUserDescription(UpdateServiceUserDescription),
  CreateApiKeyForServiceUser(CreateApiKeyForServiceUser),
  DeleteApiKeyForServiceUser(DeleteApiKeyForServiceUser),

  // ==== USER GROUP ====
  CreateUserGroup(CreateUserGroup),
  RenameUserGroup(RenameUserGroup),
  DeleteUserGroup(DeleteUserGroup),
  AddUserToUserGroup(AddUserToUserGroup),
  RemoveUserFromUserGroup(RemoveUserFromUserGroup),
  SetUsersInUserGroup(SetUsersInUserGroup),
  SetEveryoneUserGroup(SetEveryoneUserGroup),

  // ==== PERMISSIONS ====
  UpdateUserAdmin(UpdateUserAdmin),
  UpdateUserBasePermissions(UpdateUserBasePermissions),
  UpdatePermissionOnResourceType(UpdatePermissionOnResourceType),
  UpdatePermissionOnTarget(UpdatePermissionOnTarget),

  // ==== TAG ====
  CreateTag(CreateTag),
  DeleteTag(DeleteTag),
  RenameTag(RenameTag),
  UpdateTagColor(UpdateTagColor),

  // ==== VARIABLE ====
  CreateVariable(CreateVariable),
  UpdateVariableValue(UpdateVariableValue),
  UpdateVariableDescription(UpdateVariableDescription),
  UpdateVariableIsSecret(UpdateVariableIsSecret),
  DeleteVariable(DeleteVariable),

  // ==== PROVIDER ====
  CreateGitProviderAccount(CreateGitProviderAccount),
  UpdateGitProviderAccount(UpdateGitProviderAccount),
  DeleteGitProviderAccount(DeleteGitProviderAccount),
  #[serde(alias = "CreateDockerRegistryAccount")]
  CreateImageRegistryAccount(CreateImageRegistryAccount),
  #[serde(alias = "UpdateDockerRegistryAccount")]
  UpdateImageRegistryAccount(UpdateImageRegistryAccount),
  #[serde(alias = "DeleteDockerRegistryAccount")]
  DeleteImageRegistryAccount(DeleteImageRegistryAccount),

  // ==== ALERT ====
  CloseAlert(CloseAlert),
}

pub fn router() -> Router {
  Router::new()
    .route("/", post(handler))
    .route("/{variant}", post(variant_handler))
    .layer(middleware::from_fn(
      authenticate_request::<KomodoAuthImpl, true>,
    ))
}

async fn variant_handler(
  user: Extension<User>,
  Path(Variant { variant }): Path<Variant>,
  Json(params): Json<serde_json::Value>,
) -> mogh_error::Result<axum::response::Response> {
  let req: WriteRequest = serde_json::from_value(json!({
    "type": variant,
    "params": params,
  }))?;
  handler(user, Json(req)).await
}

async fn handler(
  Extension(user): Extension<User>,
  Json(request): Json<WriteRequest>,
) -> mogh_error::Result<axum::response::Response> {
  let res = tokio::spawn(task(request, user))
    .await
    .context("failure in spawned task");

  res?
}

async fn task(
  request: WriteRequest,
  user: User,
) -> mogh_error::Result<axum::response::Response> {
  let mutation_guard = if matches!(
    &request,
    WriteRequest::UpdateBackupSettings(_)
      | WriteRequest::InitializeBackupRepositories(_)
      | WriteRequest::RunBackup(_)
      | WriteRequest::PlanBackupRestore(_)
      | WriteRequest::ExecuteBackupRestore(_)
      | WriteRequest::VerifyBackupRepository(_)
      | WriteRequest::PromoteBackupMirror(_)
      | WriteRequest::CancelBackupRun(_)
      | WriteRequest::PlanCoreRecovery(_)
      | WriteRequest::ExecuteCoreRecovery(_)
  ) {
    None
  } else {
    Some(crate::backup::mutation_barrier().clone().read_owned().await)
  };
  let task_id = Uuid::new_v4();
  let method: WriteRequestMethod = (&request).into();

  let user_id = user.id.clone();
  let username = user.username.clone();

  if !matches!(
    request,
    WriteRequest::SetLastSeenUpdate(_)
      | WriteRequest::PushRecentlyViewed(_)
      | WriteRequest::SetFileManagerSafeMode(_)
  ) {
    info!(
      task_id = task_id.to_string(),
      method = method.to_string(),
      user_id,
      username,
      "WRITE REQUEST",
    );
  }

  let args = WriteArgs { user };
  let resolve = request.resolve(&args);
  let res = if let Some(_mutation_guard) = mutation_guard {
    WRITE_MUTATION_GUARD_HELD
      .scope(Arc::new(_mutation_guard), resolve)
      .await
  } else {
    resolve.await
  };

  if let Err(e) = &res {
    warn!(
      task_id = task_id.to_string(),
      method = method.to_string(),
      user_id,
      username,
      "WRITE REQUEST | ERROR: {:#}",
      e.error
    );
  }

  res.map(|res| res.0)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn synchronous_nested_work_reuses_only_the_current_task_guard()
   {
    assert!(!mutation_guard_held_by_write_request());
    let barrier = Arc::new(tokio::sync::RwLock::new(()));
    let guard = Arc::new(barrier.read_owned().await);
    WRITE_MUTATION_GUARD_HELD
      .scope(guard, async {
        assert!(mutation_guard_held_by_write_request());
        assert!(
          !tokio::spawn(async {
            mutation_guard_held_by_write_request()
          })
          .await
          .unwrap()
        );
      })
      .await;
  }

  #[tokio::test]
  async fn detached_job_keeps_the_request_lease_until_completion() {
    let barrier = Arc::new(tokio::sync::RwLock::new(()));
    let guard = Arc::new(barrier.clone().read_owned().await);
    let (finish, wait) = tokio::sync::oneshot::channel();
    let (job,) = WRITE_MUTATION_GUARD_HELD
      .scope(guard, async {
        // Return the detached handle without awaiting its completion here.
        (spawn_guarded_write_job(
          owned_write_mutation_guard().await,
          async move {
            assert!(mutation_guard_held_by_write_request());
            let _ = wait.await;
          },
        ),)
      })
      .await;
    assert!(barrier.try_write().is_err());
    finish.send(()).unwrap();
    job.await.unwrap();
    assert!(barrier.try_write().is_ok());
  }

  #[tokio::test]
  async fn detached_job_reuses_lease_even_after_a_writer_queues() {
    use futures_util::FutureExt;
    let barrier = Arc::new(tokio::sync::RwLock::new(()));
    let guard = Arc::new(barrier.clone().read_owned().await);
    let mut writer = Box::pin(barrier.clone().write_owned());
    assert!(futures_util::poll!(writer.as_mut()).is_pending());
    WRITE_MUTATION_GUARD_HELD.scope(guard, async {
      let shared = owned_write_mutation_guard().now_or_never()
        .expect("Sharing a live read lease must never queue behind a writer");
      assert!(Arc::strong_count(&shared) >= 2);
    }).await;
    drop(writer.await);
    assert!(barrier.try_write().is_ok());
  }
}
