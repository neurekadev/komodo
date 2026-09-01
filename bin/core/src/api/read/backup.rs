use anyhow::anyhow;
use komodo_client::{
  api::read::{
    BackupSnapshotDirectory, BackupSnapshotList, GetBackupSettings,
    GetBackupStatus, ListBackupSnapshotDirectory,
    ListBackupSnapshots,
  },
  entities::{
    backup::{BackupSettings, BackupStatus},
    permission::PermissionLevel,
  },
};
use mogh_error::AddStatusCodeError;
use mogh_resolver::Resolve;
use reqwest::StatusCode;

use super::ReadArgs;

impl Resolve<ReadArgs> for GetBackupSettings {
  async fn resolve(
    self,
    ReadArgs { user }: &ReadArgs,
  ) -> mogh_error::Result<BackupSettings> {
    if !user.admin {
      return Err(
        anyhow!("Backup settings are admin only")
          .status_code(StatusCode::FORBIDDEN),
      );
    }
    Ok(crate::backup::get_redacted_settings().await?)
  }
}

impl Resolve<ReadArgs> for GetBackupStatus {
  async fn resolve(
    self,
    ReadArgs { user }: &ReadArgs,
  ) -> mogh_error::Result<BackupStatus> {
    let mut status = crate::backup::status().await?;
    if !user.admin {
      let mut authorized = Vec::new();
      for run in status.recent_runs {
        let Some(target) = &run.target else {
          continue;
        };
        if crate::backup::authorize_target(
          target,
          user,
          PermissionLevel::Read,
        )
        .await
        .is_ok()
        {
          authorized.push(run);
        }
      }
      status.active_run = status.active_run.filter(|run| {
        authorized.iter().any(|candidate| candidate.id == run.id)
      });
      status.recent_runs = authorized;
      status.critical_alert = None;
    }
    Ok(status)
  }
}

impl Resolve<ReadArgs> for ListBackupSnapshots {
  async fn resolve(
    self,
    ReadArgs { user }: &ReadArgs,
  ) -> mogh_error::Result<BackupSnapshotList> {
    if let Some(target) = &self.target {
      crate::backup::authorize_target(
        target,
        user,
        PermissionLevel::Read,
      )
      .await?;
    } else if !user.admin {
      return Err(
        anyhow!("A backup target is required")
          .status_code(StatusCode::FORBIDDEN),
      );
    }
    let (mut snapshots, hidden) =
      crate::backup::list_snapshots().await?;
    if let Some(target) = self.target {
      snapshots.retain(|snapshot| snapshot.target == target);
    }
    snapshots
      .sort_by_key(|snapshot| std::cmp::Reverse(snapshot.created_at));
    let total = snapshots.len() as u64;
    let limit = self.limit.clamp(1, 500);
    let snapshots = snapshots
      .into_iter()
      .skip(self.page.saturating_mul(limit) as usize)
      .take(limit as usize)
      .collect();
    Ok(BackupSnapshotList {
      snapshots,
      total,
      hidden,
    })
  }
}

impl Resolve<ReadArgs> for ListBackupSnapshotDirectory {
  async fn resolve(
    self,
    ReadArgs { user }: &ReadArgs,
  ) -> mogh_error::Result<BackupSnapshotDirectory> {
    crate::backup::authorize_snapshot(
      &self.snapshot,
      user,
      PermissionLevel::Read,
    )
    .await?;
    let page = crate::backup::list_directory(
      self.snapshot,
      self.parent,
      self.search,
      self.page,
      self.limit,
    )
    .await?;
    Ok(BackupSnapshotDirectory {
      entries: page.entries,
      total: page.total,
      page: page.page,
      has_more: page.has_more,
    })
  }
}
