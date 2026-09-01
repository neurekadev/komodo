use anyhow::{Context, anyhow};
use komodo_client::{
  api::write::*,
  entities::{
    backup::{
      BackupRestorePlan, BackupRun, BackupSettings, CoreRecoveryPlan,
    },
    permission::PermissionLevel,
    stack::Stack,
  },
};
use mogh_error::AddStatusCodeError;
use mogh_resolver::Resolve;
use reqwest::StatusCode;

use crate::resource::KomodoResource;

use super::WriteArgs;

fn require_admin(
  user: &komodo_client::entities::user::User,
) -> mogh_error::Result<()> {
  if user.admin {
    Ok(())
  } else {
    Err(
      anyhow!("This backup operation is admin only")
        .status_code(StatusCode::FORBIDDEN),
    )
  }
}

impl Resolve<WriteArgs> for UpdateBackupSettings {
  async fn resolve(
    self,
    WriteArgs { user }: &WriteArgs,
  ) -> mogh_error::Result<BackupSettings> {
    require_admin(user)?;
    Ok(crate::backup::save_settings(self.settings).await?)
  }
}

impl Resolve<WriteArgs> for InitializeBackupRepositories {
  async fn resolve(
    self,
    WriteArgs { user }: &WriteArgs,
  ) -> mogh_error::Result<BackupRun> {
    require_admin(user)?;
    Ok(crate::backup::initialize_repositories().await?)
  }
}

impl Resolve<WriteArgs> for RunBackup {
  async fn resolve(
    self,
    WriteArgs { user }: &WriteArgs,
  ) -> mogh_error::Result<BackupRun> {
    if let Some(target) = &self.target {
      crate::backup::authorize_target(
        target,
        user,
        PermissionLevel::Execute,
      )
      .await?;
    } else {
      require_admin(user)?;
    }
    Ok(crate::backup::run_backup(self.target).await?)
  }
}

impl Resolve<WriteArgs> for PlanBackupRestore {
  async fn resolve(
    self,
    WriteArgs { user }: &WriteArgs,
  ) -> mogh_error::Result<BackupRestorePlan> {
    let snapshot = crate::backup::authorize_snapshot(
      &self.snapshot,
      user,
      PermissionLevel::Execute,
    )
    .await?;
    if let Some(destination) = &self.destination_server_id {
      let source_server = match &snapshot.target {
        komodo_client::entities::backup::BackupTarget::Volume {
          server_id,
          ..
        } => Some(server_id.clone()),
        komodo_client::entities::backup::BackupTarget::Stack {
          stack_id,
        } => Some(
          crate::resource::get::<Stack>(stack_id)
            .await?
            .config
            .server_id,
        ),
        _ => None,
      };
      if source_server.as_deref() != Some(destination.as_str()) {
        crate::backup::authorize_target(
          &komodo_client::entities::backup::BackupTarget::Volume {
            server_id: destination.clone(),
            volume_name: String::new(),
          },
          user,
          PermissionLevel::Execute,
        )
        .await?;
        if matches!(
          snapshot.target,
          komodo_client::entities::backup::BackupTarget::Stack { .. }
        ) && !<Stack as KomodoResource>::user_can_create(user)
        {
          return Err(anyhow!(
            "Cross-node stack restore requires Stack-create permission"
          )
          .status_code(StatusCode::FORBIDDEN));
        }
      }
    }
    Ok(crate::backup::plan_restore(snapshot, user, self).await?)
  }
}

impl Resolve<WriteArgs> for ExecuteBackupRestore {
  async fn resolve(
    self,
    WriteArgs { user }: &WriteArgs,
  ) -> mogh_error::Result<BackupRun> {
    // Re-authorize against the repository-backed snapshot at execution time.
    let plan = crate::backup::restore_plan(&self.plan_id)
      .await
      .context("Failed to load restore plan")?;
    crate::backup::authorize_snapshot(
      &plan.snapshot,
      user,
      PermissionLevel::Execute,
    )
    .await?;
    Ok(crate::backup::execute_restore(&self.plan_id, user).await?)
  }
}

impl Resolve<WriteArgs> for VerifyBackupRepository {
  async fn resolve(
    self,
    WriteArgs { user }: &WriteArgs,
  ) -> mogh_error::Result<BackupRun> {
    require_admin(user)?;
    Ok(crate::backup::verify(self.mirror, self.full).await?)
  }
}

impl Resolve<WriteArgs> for PromoteBackupMirror {
  async fn resolve(
    self,
    WriteArgs { user }: &WriteArgs,
  ) -> mogh_error::Result<BackupSettings> {
    require_admin(user)?;
    Ok(crate::backup::promote_mirror().await?)
  }
}

impl Resolve<WriteArgs> for CancelBackupRun {
  async fn resolve(
    self,
    WriteArgs { user }: &WriteArgs,
  ) -> mogh_error::Result<BackupRun> {
    require_admin(user)?;
    Ok(crate::backup::cancel_run(&self.run_id).await?)
  }
}

impl Resolve<WriteArgs> for PlanCoreRecovery {
  async fn resolve(
    self,
    WriteArgs { user }: &WriteArgs,
  ) -> mogh_error::Result<CoreRecoveryPlan> {
    require_admin(user)?;
    Ok(
      crate::backup::plan_core_recovery(
        &self.snapshot,
        user.id.clone(),
      )
      .await?,
    )
  }
}

impl Resolve<WriteArgs> for ExecuteCoreRecovery {
  async fn resolve(
    self,
    WriteArgs { user }: &WriteArgs,
  ) -> mogh_error::Result<BackupRun> {
    require_admin(user)?;
    Ok(
      crate::backup::execute_core_recovery(&self.plan_id, &user.id)
        .await?,
    )
  }
}
