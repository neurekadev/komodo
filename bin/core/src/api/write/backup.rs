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

fn stack_recovery_requested(
  stack_snapshot: bool,
  explicit_recovery: bool,
  snapshot_server: Option<&str>,
  current_server: Option<&str>,
  destination_server: Option<&str>,
) -> bool {
  stack_snapshot
    && (explicit_recovery
      || current_server != snapshot_server
      || destination_server.is_some_and(|destination| {
        Some(destination) != snapshot_server
      }))
}

fn destination_backup_permission_required(
  source_server: Option<&str>,
  destination_server: &str,
  recovering_stack: bool,
) -> bool {
  recovering_stack || source_server != Some(destination_server)
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
    Ok(crate::backup::run_backup(self.target, user).await?)
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
    let current_stack_server = match &snapshot.target {
      komodo_client::entities::backup::BackupTarget::Stack {
        stack_id,
      } => crate::resource::get::<Stack>(stack_id)
        .await
        .ok()
        .map(|stack| stack.config.server_id),
      _ => None,
    };
    let source_server = match &snapshot.target {
      komodo_client::entities::backup::BackupTarget::Volume {
        server_id,
        ..
      } => Some(server_id.clone()),
      komodo_client::entities::backup::BackupTarget::Stack {
        ..
      } => crate::backup::snapshot_server_id(&snapshot)
        .map(str::to_string),
      _ => None,
    };
    let destination_server =
      self.destination_server_id.clone().or_else(|| {
        current_stack_server
          .clone()
          .or_else(|| source_server.clone())
      });
    let recovering_stack = stack_recovery_requested(
      matches!(
        &snapshot.target,
        komodo_client::entities::backup::BackupTarget::Stack { .. }
      ),
      self
        .recovered_stack_name
        .as_deref()
        .is_some_and(|name| !name.trim().is_empty()),
      source_server.as_deref(),
      current_stack_server.as_deref(),
      destination_server.as_deref(),
    );
    if let Some(destination) = destination_server
      && destination_backup_permission_required(
        source_server.as_deref(),
        &destination,
        recovering_stack,
      )
    {
      crate::backup::authorize_target(
        &komodo_client::entities::backup::BackupTarget::Volume {
          server_id: destination,
          volume_name: String::new(),
        },
        user,
        PermissionLevel::Execute,
      )
      .await?;
    }
    if recovering_stack
      && !<Stack as KomodoResource>::user_can_create(user)
    {
      return Err(
        anyhow!(
          "Recovered Stack creation requires Stack-create permission"
        )
        .status_code(StatusCode::FORBIDDEN),
      );
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
    Ok(
      crate::backup::promote_mirror(self.allow_primary_unavailable)
        .await?,
    )
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
        self.repository,
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

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn moved_stack_recovery_requires_destination_authorization() {
    let recovering = stack_recovery_requested(
      true,
      false,
      Some("snapshot-server"),
      Some("current-server"),
      Some("snapshot-server"),
    );
    assert!(recovering);
    assert!(destination_backup_permission_required(
      Some("snapshot-server"),
      "snapshot-server",
      recovering,
    ));
  }

  #[test]
  fn in_place_stack_restore_does_not_add_server_authorization() {
    let recovering = stack_recovery_requested(
      true,
      false,
      Some("server"),
      Some("server"),
      Some("server"),
    );
    assert!(!recovering);
    assert!(!destination_backup_permission_required(
      Some("server"),
      "server",
      recovering,
    ));
  }
}
