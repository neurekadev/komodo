use komodo_client::entities::file_manager::{
  FileManagerActiveOperations, FileManagerCapabilities,
  FileManagerDirectory, FileManagerJournalStatus,
  FileManagerOperationStatus, FileManagerPreflight,
  FileManagerTextFile,
};
use mogh_resolver::Resolve;
use periphery_client::api::file_manager::*;

use crate::{api::Args, file_manager};

impl Resolve<Args> for GetFileManagerCapabilities {
  async fn resolve(
    self,
    _: &Args,
  ) -> anyhow::Result<FileManagerCapabilities> {
    Ok(file_manager::capabilities(&self.target).await)
  }
}

impl Resolve<Args> for ListFileManagerDirectory {
  async fn resolve(
    self,
    _: &Args,
  ) -> anyhow::Result<FileManagerDirectory> {
    file_manager::list_directory(&self.target, &self.path).await
  }
}

impl Resolve<Args> for ReadFileManagerText {
  async fn resolve(
    self,
    _: &Args,
  ) -> anyhow::Result<FileManagerTextFile> {
    file_manager::read_text(&self.target, &self.path).await
  }
}

impl Resolve<Args> for PreflightFileManagerOperation {
  async fn resolve(
    self,
    _: &Args,
  ) -> anyhow::Result<FileManagerPreflight> {
    file_manager::preflight(&self.target, self.actor, self.operation)
      .await
  }
}

impl Resolve<Args> for CommitFileManagerOperation {
  async fn resolve(
    self,
    _: &Args,
  ) -> anyhow::Result<FileManagerCommitResponse> {
    file_manager::commit(
      &self.target,
      &self.actor,
      &self.operation_id,
      &self.plan_id,
      &self.decisions,
      self.confirmed,
    )
    .await
  }
}

impl Resolve<Args> for GetFileManagerOperationStatus {
  async fn resolve(
    self,
    _: &Args,
  ) -> anyhow::Result<FileManagerOperationStatus> {
    file_manager::operation_status(
      &self.target,
      &self.actor,
      &self.operation_id,
    )
    .await
  }
}

impl Resolve<Args> for ListActiveFileManagerOperations {
  async fn resolve(
    self,
    _: &Args,
  ) -> anyhow::Result<FileManagerActiveOperations> {
    file_manager::list_active_operations(&self.target, &self.actor)
      .await
  }
}

impl Resolve<Args> for ResolveFileManagerOperationConflict {
  async fn resolve(
    self,
    _: &Args,
  ) -> anyhow::Result<FileManagerOperationStatus> {
    file_manager::resolve_operation_conflict(
      &self.target,
      &self.actor,
      &self.operation_id,
      self.decision_id,
      self.action,
      self.apply_to_all,
    )
    .await
  }
}

impl Resolve<Args> for CancelFileManagerOperation {
  async fn resolve(
    self,
    _: &Args,
  ) -> anyhow::Result<FileManagerOperationStatus> {
    file_manager::cancel_file_manager_operation(
      &self.target,
      &self.actor,
      &self.operation_id,
    )
    .await
  }
}

impl Resolve<Args> for GetFileManagerJournalStatus {
  async fn resolve(
    self,
    _: &Args,
  ) -> anyhow::Result<FileManagerJournalStatus> {
    file_manager::journal_status(&self.target, &self.actor).await
  }
}

impl Resolve<Args> for UndoFileManagerOperation {
  async fn resolve(
    self,
    _: &Args,
  ) -> anyhow::Result<FileManagerCommitResponse> {
    file_manager::undo(
      &self.target,
      &self.actor,
      &self.operation_id,
      self.confirmed,
      self.rollback_operation_id.as_deref(),
    )
    .await
  }
}

impl Resolve<Args> for RedoFileManagerOperation {
  async fn resolve(
    self,
    _: &Args,
  ) -> anyhow::Result<FileManagerCommitResponse> {
    file_manager::redo(
      &self.target,
      &self.actor,
      &self.operation_id,
      self.confirmed,
    )
    .await
  }
}

impl Resolve<Args> for StartFileManagerUpload {
  async fn resolve(self, args: &Args) -> anyhow::Result<uuid::Uuid> {
    file_manager::start_upload(&args.core, self).await
  }
}

impl Resolve<Args> for StartFileManagerDownload {
  async fn resolve(
    self,
    args: &Args,
  ) -> anyhow::Result<StartFileManagerDownloadResponse> {
    file_manager::start_download(
      &args.core,
      self.target,
      self.actor,
      self.operation_id,
      self.paths,
    )
    .await
  }
}
