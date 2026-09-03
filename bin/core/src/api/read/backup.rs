use anyhow::anyhow;
use komodo_client::{
  api::read::{
    BackupSnapshotDirectory, BackupSnapshotList, GetBackupSettings,
    GetBackupStatus, ListBackupSnapshotDirectory,
    ListBackupSnapshots,
  },
  entities::{
    backup::{
      BackupSettings, BackupSnapshot, BackupStatus, BackupTarget,
    },
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
      let mut authorized_active = Vec::new();
      for run in status.active_runs {
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
          authorized_active.push(run);
        }
      }
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
      status.active_runs = authorized_active;
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
    let (snapshots, hidden, inventory) =
      crate::backup::list_snapshots().await?;
    let mut page =
      snapshot_inventory_page(snapshots, hidden, inventory, &self);
    if let Some(BackupTarget::Stack { stack_id }) = &self.target
      && !page.snapshots.is_empty()
    {
      // The full inventory has been consumed and its permit released. A slow
      // Periphery must not prevent unrelated snapshot requests from proceeding.
      let current =
        crate::backup::current_stack_backup_source(stack_id)
          .await
          .ok();
      for snapshot in &mut page.snapshots {
        snapshot.source_paths_match_current =
          stack_source_matches(snapshot, current.as_ref());
      }
    }
    Ok(page)
  }
}

fn snapshot_inventory_page(
  mut snapshots: Vec<BackupSnapshot>,
  hidden: u64,
  inventory: tokio::sync::OwnedSemaphorePermit,
  request: &ListBackupSnapshots,
) -> BackupSnapshotList {
  if let Some(target) = &request.target {
    snapshots.retain(|snapshot| &snapshot.target == target);
  }
  snapshots
    .sort_by_key(|snapshot| std::cmp::Reverse(snapshot.created_at));
  let total = snapshots.len() as u64;
  let limit = request.limit.clamp(1, 500);
  // Allocate the page separately: collecting an IntoIter can reuse the full
  // inventory's backing allocation even after most entries have been dropped.
  let mut page = Vec::with_capacity(limit as usize);
  page.extend(
    snapshots
      .into_iter()
      .skip(request.page.saturating_mul(limit) as usize)
      .take(limit as usize),
  );
  // Only the bounded response page survives this point, not the full inventory.
  drop(inventory);
  BackupSnapshotList {
    snapshots: page,
    total,
    hidden,
  }
}

fn stack_source_matches(
  snapshot: &BackupSnapshot,
  current: Option<&(String, Vec<String>)>,
) -> Option<bool> {
  current.map(|(server_id, paths)| {
    crate::backup::snapshot_server_id(snapshot)
      == Some(server_id.as_str())
      && crate::backup::backup_source_paths_match(
        &snapshot.restorable_source_paths,
        paths,
      )
  })
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

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::Arc;

  #[test]
  fn paged_inventory_releases_admission_before_source_discovery() {
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let inventory = slots.clone().try_acquire_owned().unwrap();
    let target = BackupTarget::Stack {
      stack_id: "stack".into(),
    };
    let snapshots = (0..600)
      .map(|created_at| BackupSnapshot {
        name: created_at.to_string(),
        target: target.clone(),
        created_at,
        ..Default::default()
      })
      .chain(std::iter::once(BackupSnapshot::default()))
      .collect();
    let page = snapshot_inventory_page(
      snapshots,
      2,
      inventory,
      &ListBackupSnapshots {
        target: Some(target),
        page: 1,
        limit: 10,
      },
    );
    assert_eq!(page.total, 600);
    assert_eq!(page.hidden, 2);
    assert_eq!(page.snapshots.len(), 10);
    assert_eq!(page.snapshots.capacity(), 10);
    assert_eq!(page.snapshots[0].created_at, 589);
    // A caller can still retain its page while another full inventory starts.
    assert!(slots.try_acquire_owned().is_ok());
  }

  #[test]
  fn source_comparison_distinguishes_unavailable_matching_and_changed()
   {
    let snapshot = BackupSnapshot {
      hostname: "komodo-periphery-server".into(),
      restorable_source_paths: vec!["/data/stack".into()],
      ..Default::default()
    };
    assert_eq!(stack_source_matches(&snapshot, None), None);
    assert_eq!(
      stack_source_matches(
        &snapshot,
        Some(&("server".into(), vec!["/data/stack".into()]))
      ),
      Some(true)
    );
    assert_eq!(
      stack_source_matches(
        &snapshot,
        Some(&("other".into(), vec!["/data/stack".into()]))
      ),
      Some(false)
    );
    assert_eq!(
      stack_source_matches(
        &snapshot,
        Some(&("server".into(), vec!["/data/changed".into()]))
      ),
      Some(false)
    );
  }
}
