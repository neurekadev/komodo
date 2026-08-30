use anyhow::Context as _;
use komodo_client::{
  api::read::*,
  entities::{
    file_manager::{
      FileManagerCapabilities, FileManagerDirectory,
      FileManagerEntryKind, FileManagerJournalStatus,
      FileManagerOperationStatus, FileManagerTextFile,
    },
    permission::PermissionLevel,
  },
};
use mogh_resolver::Resolve;
use periphery_client::api::file_manager as periphery;

use crate::{
  config::core_config,
  file_manager::{
    can_write, managed_entry, managed_text, resolve_target,
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
    let mut directory = periphery_client(&resolved.server)
      .await?
      .request(periphery::ListFileManagerDirectory {
        target: resolved.periphery,
        path: self.path.clone(),
      })
      .await?;
    if self.path.is_empty()
      && let (Some(stack), Some(managed)) =
        (resolved.stack.as_ref(), resolved.managed_file.as_deref())
    {
      if let Some(entry) = directory
        .entries
        .iter_mut()
        .find(|entry| entry.name == managed)
      {
        *entry = managed_entry(stack, managed);
      } else {
        directory.entries.push(managed_entry(stack, managed));
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
    if resolved.managed_file.as_deref() == Some(self.path.as_str()) {
      let stack = resolved
        .stack
        .as_ref()
        .context("Managed File Manager stack is missing")?;
      return Ok(managed_text(stack, &self.path));
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

impl Resolve<ReadArgs> for GetFileManagerOperationStatus {
  async fn resolve(
    self,
    ReadArgs { user }: &ReadArgs,
  ) -> mogh_error::Result<FileManagerOperationStatus> {
    let resolved =
      resolve_target(&self.target, user, PermissionLevel::Read)
        .await?;
    Ok(
      periphery_client(&resolved.server)
        .await?
        .request(periphery::GetFileManagerOperationStatus {
          target: resolved.periphery,
          actor: user.id.clone(),
          operation_id: self.operation_id,
        })
        .await?,
    )
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
