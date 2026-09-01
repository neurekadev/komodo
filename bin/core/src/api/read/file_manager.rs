use anyhow::Context as _;
use komodo_client::{
  api::read::*,
  entities::{
    file_manager::{
      FileManagerActiveOperations, FileManagerCapabilities,
      FileManagerDirectory, FileManagerEntry, FileManagerEntryKind,
      FileManagerJournalStatus, FileManagerOperationPhase,
      FileManagerOperationState, FileManagerOperationStatus,
      FileManagerTextFile,
    },
    permission::PermissionLevel,
  },
};
use mogh_resolver::Resolve;
use periphery_client::api::file_manager as periphery;

use crate::{
  config::core_config,
  file_manager::{
    can_write, get_core_operation_status,
    list_core_active_operations, managed_entry, managed_text,
    require_managed_file, resolve_target,
  },
  helpers::periphery_client,
};

use super::ReadArgs;

impl Resolve<ReadArgs> for GetFileManagerCapabilities {
  async fn resolve(
    self,
    ReadArgs { user }: &ReadArgs,
  ) -> mogh_error::Result<FileManagerCapabilities> {
    let resolved =
      resolve_target(&self.target, user, PermissionLevel::Read)
        .await?;
    let mut capabilities = periphery_client(&resolved.server)
      .await?
      .request(periphery::GetFileManagerCapabilities {
        target: resolved.periphery,
      })
      .await?;
    capabilities.managed_file = resolved.managed_file.clone();
    capabilities.managed_files = resolved.managed_files.clone();
    if core_config().ui_write_disabled {
      capabilities.read_only = true;
      capabilities.reason =
        Some("UI writes are disabled by Core configuration".into());
    } else if !can_write(&self.target, user).await {
      capabilities.read_only = true;
      capabilities.reason = Some(
        "You do not have write permission for this target".into(),
      );
    }
    Ok(capabilities)
  }
}

impl Resolve<ReadArgs> for ListFileManagerDirectory {
  async fn resolve(
    self,
    ReadArgs { user }: &ReadArgs,
  ) -> mogh_error::Result<FileManagerDirectory> {
    let resolved =
      resolve_target(&self.target, user, PermissionLevel::Read)
        .await?;
    let virtual_directory = self.path.is_empty()
      || resolved.managed_files.iter().any(|managed| {
        managed
          .path
          .strip_prefix(&self.path)
          .is_some_and(|suffix| suffix.starts_with('/'))
      });
    let directory_result = periphery_client(&resolved.server)
      .await?
      .request(periphery::ListFileManagerDirectory {
        target: resolved.periphery,
        path: self.path.clone(),
      })
      .await;
    let mut directory = match directory_result {
      Ok(directory) => directory,
      Err(_) if virtual_directory => FileManagerDirectory {
        path: self.path.clone(),
        entries: Vec::new(),
      },
      Err(error) => return Err(error.into()),
    };
    if let Some(stack) = resolved.stack.as_ref() {
      for managed in &resolved.managed_files {
        let remainder = if self.path.is_empty() {
          Some(managed.path.as_str())
        } else {
          managed
            .path
            .strip_prefix(&self.path)
            .and_then(|suffix| suffix.strip_prefix('/'))
        };
        let Some(remainder) = remainder else {
          continue;
        };
        let Some((name, nested)) = remainder
          .split_once('/')
          .map(|(name, _)| (name, true))
          .or_else(|| {
            (!remainder.is_empty()).then_some((remainder, false))
          })
        else {
          continue;
        };
        let entry_path = if self.path.is_empty() {
          name.to_string()
        } else {
          format!("{}/{name}", self.path)
        };
        let replacement = if nested {
          FileManagerEntry {
            path: entry_path.clone(),
            name: name.to_string(),
            kind: FileManagerEntryKind::Directory,
            size: 0,
            modified_at: 0,
            revision: Default::default(),
            managed: true,
          }
        } else {
          managed_entry(stack, managed)
        };
        if let Some(entry) = directory
          .entries
          .iter_mut()
          .find(|entry| entry.path == entry_path)
        {
          if !nested {
            *entry = replacement;
          }
        } else {
          directory.entries.push(replacement);
        }
      }
      directory.entries.sort_by(|a, b| {
        (
          a.kind != FileManagerEntryKind::Directory,
          a.name.to_lowercase(),
        )
          .cmp(&(
            b.kind != FileManagerEntryKind::Directory,
            b.name.to_lowercase(),
          ))
      });
    }
    Ok(directory)
  }
}

impl Resolve<ReadArgs> for ReadFileManagerText {
  async fn resolve(
    self,
    ReadArgs { user }: &ReadArgs,
  ) -> mogh_error::Result<FileManagerTextFile> {
    let resolved =
      resolve_target(&self.target, user, PermissionLevel::Read)
        .await?;
    if let Some(managed) = resolved
      .managed_files
      .iter()
      .find(|managed| managed.path == self.path)
    {
      let stack = resolved
        .stack
        .as_ref()
        .context("Managed File Manager stack is missing")?;
      return Ok(managed_text(stack, managed));
    }
    Ok(
      periphery_client(&resolved.server)
        .await?
        .request(periphery::ReadFileManagerText {
          target: resolved.periphery,
          path: self.path,
        })
        .await?,
    )
  }
}

impl Resolve<ReadArgs> for ReadManagedFileManagerRenderedText {
  async fn resolve(
    self,
    ReadArgs { user }: &ReadArgs,
  ) -> mogh_error::Result<FileManagerTextFile> {
    let resolved =
      resolve_target(&self.target, user, PermissionLevel::Read)
        .await?;
    let managed =
      require_managed_file(&resolved, self.path.as_deref())?;
    Ok(
      periphery_client(&resolved.server)
        .await?
        .request(periphery::ReadFileManagerText {
          target: resolved.periphery,
          path: managed.path,
        })
        .await?,
    )
  }
}

impl Resolve<ReadArgs> for GetFileManagerOperationStatus {
  async fn resolve(
    self,
    ReadArgs { user }: &ReadArgs,
  ) -> mogh_error::Result<FileManagerOperationStatus> {
    let resolved =
      resolve_target(&self.target, user, PermissionLevel::Read)
        .await?;
    let core_status = get_core_operation_status(
      &self.operation_id,
      &user.id,
      &self.target,
    );
    if core_status.as_ref().is_some_and(|status| {
      matches!(
        status.state,
        FileManagerOperationState::Complete
          | FileManagerOperationState::Failed
          | FileManagerOperationState::Cancelled
      ) || status.phase == FileManagerOperationPhase::Finalizing
    }) {
      return Ok(core_status.unwrap());
    }
    let periphery_status = periphery_client(&resolved.server)
      .await?
      .request(periphery::GetFileManagerOperationStatus {
        target: resolved.periphery,
        actor: user.id.clone(),
        operation_id: self.operation_id,
      })
      .await;
    match periphery_status {
      Ok(mut status)
        if core_status.is_some()
          && status.state == FileManagerOperationState::Complete =>
      {
        status.state = FileManagerOperationState::Running;
        status.phase = FileManagerOperationPhase::Finalizing;
        Ok(status)
      }
      Ok(status) => Ok(status),
      Err(_) if core_status.is_some() => Ok(core_status.unwrap()),
      Err(error) => Err(error.into()),
    }
  }
}

impl Resolve<ReadArgs> for ListActiveFileManagerOperations {
  async fn resolve(
    self,
    ReadArgs { user }: &ReadArgs,
  ) -> mogh_error::Result<FileManagerActiveOperations> {
    let resolved =
      resolve_target(&self.target, user, PermissionLevel::Read)
        .await?;
    let mut active =
      list_core_active_operations(&user.id, &self.target);
    if let Ok(periphery_active) = periphery_client(&resolved.server)
      .await?
      .request(periphery::ListActiveFileManagerOperations {
        target: resolved.periphery,
        actor: user.id.clone(),
      })
      .await
    {
      for status in periphery_active.operations {
        if let Some(existing) =
          active.operations.iter_mut().find(|existing| {
            existing.operation_id == status.operation_id
          })
        {
          *existing = status;
        } else {
          active.operations.push(status);
        }
      }
      active.operations.sort_by_key(|status| status.started_at);
    }
    Ok(active)
  }
}

impl Resolve<ReadArgs> for GetFileManagerJournalStatus {
  async fn resolve(
    self,
    ReadArgs { user }: &ReadArgs,
  ) -> mogh_error::Result<FileManagerJournalStatus> {
    let resolved =
      resolve_target(&self.target, user, PermissionLevel::Read)
        .await?;
    Ok(
      periphery_client(&resolved.server)
        .await?
        .request(periphery::GetFileManagerJournalStatus {
          target: resolved.periphery,
          actor: user.id.clone(),
        })
        .await?,
    )
  }
}
