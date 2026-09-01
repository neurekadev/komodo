use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use super::{I64, U64};

/// A filesystem root exposed by the File Manager.
///
/// Core resolves this logical target to a trusted host root. Host paths are
/// never accepted from, or returned to, API clients.
#[typeshare]
#[derive(
  Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(tag = "type", content = "params")]
pub enum FileManagerTarget {
  Stack { stack: String },
  Volume { server: String, volume: String },
}

#[typeshare]
#[derive(
  Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum FileManagerEntryKind {
  #[default]
  File,
  Directory,
  Symlink,
  Special,
}

/// An opaque optimistic-concurrency revision.
#[typeshare]
#[derive(
  Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct FileManagerRevision {
  pub id: String,
}

#[typeshare]
#[derive(
  Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct FileManagerEntry {
  /// Normalized root-relative path using `/` separators.
  pub path: String,
  pub name: String,
  pub kind: FileManagerEntryKind,
  pub size: U64,
  /// Unix timestamp in milliseconds.
  pub modified_at: I64,
  pub revision: FileManagerRevision,
  /// The entry is the database-managed compose source.
  #[serde(default)]
  pub managed: bool,
}

#[typeshare]
#[derive(
  Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct FileManagerLimits {
  pub max_text_bytes: U64,
  pub max_entries: U64,
  pub max_depth: U64,
  pub max_archive_expanded_bytes: U64,
  pub max_archive_expansion_ratio: U64,
  pub minimum_free_bytes: U64,
}

#[typeshare]
#[derive(
  Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct FileManagerCapabilities {
  pub available: bool,
  pub read_only: bool,
  pub reason: Option<String>,
  pub managed_file: Option<String>,
  pub limits: FileManagerLimits,
}

#[typeshare]
#[derive(
  Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct FileManagerDirectory {
  pub path: String,
  pub entries: Vec<FileManagerEntry>,
}

#[typeshare]
#[derive(
  Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct FileManagerTextFile {
  pub path: String,
  pub contents: String,
  pub revision: FileManagerRevision,
}

#[typeshare]
#[derive(
  Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum FileManagerArchiveFormat {
  #[default]
  Zip,
  Tar,
  TarGz,
  SevenZip,
}

#[typeshare]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(tag = "type", content = "params")]
pub enum FileManagerOperation {
  CreateFile {
    path: String,
  },
  CreateDirectory {
    path: String,
  },
  Rename {
    path: String,
    new_name: String,
  },
  Move {
    paths: Vec<String>,
    destination: String,
  },
  Copy {
    paths: Vec<String>,
    destination: String,
  },
  Delete {
    paths: Vec<String>,
  },
  WriteText {
    path: String,
    contents: String,
    expected_revision: FileManagerRevision,
  },
  CreateArchive {
    paths: Vec<String>,
    destination: String,
    format: FileManagerArchiveFormat,
  },
  ExtractArchive {
    path: String,
    destination: String,
  },
}

impl FileManagerOperation {
  pub fn affected_paths(&self) -> Vec<&str> {
    match self {
      FileManagerOperation::CreateFile { path }
      | FileManagerOperation::CreateDirectory { path }
      | FileManagerOperation::Rename { path, .. }
      | FileManagerOperation::WriteText { path, .. } => vec![path],
      FileManagerOperation::Move { paths, destination }
      | FileManagerOperation::Copy { paths, destination } => paths
        .iter()
        .map(String::as_str)
        .chain(std::iter::once(destination.as_str()))
        .collect(),
      FileManagerOperation::Delete { paths } => {
        paths.iter().map(String::as_str).collect()
      }
      FileManagerOperation::CreateArchive {
        paths,
        destination,
        ..
      } => paths
        .iter()
        .map(String::as_str)
        .chain(std::iter::once(destination.as_str()))
        .collect(),
      FileManagerOperation::ExtractArchive { path, destination } => {
        vec![path, destination]
      }
    }
  }

  pub fn requires_confirmation(&self) -> bool {
    matches!(self, FileManagerOperation::Delete { .. })
  }

  /// Whether this operation is retained in user-visible undo history.
  pub fn is_undoable(&self) -> bool {
    matches!(
      self,
      FileManagerOperation::CreateFile { .. }
        | FileManagerOperation::CreateDirectory { .. }
        | FileManagerOperation::Rename { .. }
        | FileManagerOperation::Move { .. }
        | FileManagerOperation::Copy { .. }
        | FileManagerOperation::Delete { .. }
    )
  }
}

#[typeshare]
#[derive(
  Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum FileManagerConflictAction {
  #[default]
  Skip,
  Overwrite,
}

#[typeshare]
#[derive(
  Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct FileManagerConflict {
  pub path: String,
  pub existing_kind: FileManagerEntryKind,
  pub incoming_kind: FileManagerEntryKind,
}

#[typeshare]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct FileManagerConflictDecision {
  pub path: String,
  pub action: FileManagerConflictAction,
}

#[typeshare]
#[derive(
  Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct FileManagerPendingConflict {
  /// Opaque, single-use identifier for the currently pending decision.
  pub decision_id: String,
  pub conflict: FileManagerConflict,
}

#[typeshare]
#[derive(
  Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct FileManagerPreflight {
  pub plan_id: String,
  pub expires_at: I64,
  pub conflicts: Vec<FileManagerConflict>,
  pub confirmation_required: bool,
  /// Whether this Periphery supports crash-durable coordination for writes to
  /// database-managed files. Older Periphery versions omit this field.
  #[serde(default)]
  pub supports_durable_managed_transactions: bool,
}

#[typeshare]
#[derive(
  Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum FileManagerOperationState {
  #[default]
  Pending,
  Running,
  WaitingForInput,
  Complete,
  Failed,
  Cancelled,
}

#[typeshare]
#[derive(
  Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum FileManagerOperationPhase {
  #[default]
  Queued,
  Preparing,
  Snapshotting,
  Applying,
  Verifying,
  Transferring,
  Finalizing,
  RollingBack,
}

#[typeshare]
#[derive(
  Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct FileManagerOperationTicket {
  pub operation_id: String,
}

#[typeshare]
#[derive(
  Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct FileManagerOperationStatus {
  pub operation_id: String,
  pub state: FileManagerOperationState,
  #[serde(default)]
  pub phase: FileManagerOperationPhase,
  #[serde(default)]
  pub description: String,
  /// Server timestamp in milliseconds when the operation was accepted.
  #[serde(default)]
  pub started_at: I64,
  /// Server timestamp in milliseconds when this status last changed.
  #[serde(default)]
  pub updated_at: I64,
  /// Server timestamp in milliseconds when the current phase began.
  #[serde(default)]
  pub phase_started_at: I64,
  /// Whether the server can still accept a cancellation request.
  #[serde(default)]
  pub cancellable: bool,
  /// Bytes currently retained in staging or internal rollback storage.
  #[serde(default)]
  pub temporary_storage_bytes: U64,
  /// A conflict awaiting an explicit overwrite or skip decision.
  #[serde(default)]
  pub pending_conflict: Option<FileManagerPendingConflict>,
  /// Counters are scoped to `phase` and reset on each phase transition.
  pub completed_entries: U64,
  pub total_entries: U64,
  pub completed_bytes: U64,
  pub total_bytes: U64,
  pub error: Option<String>,
}

#[typeshare]
#[derive(
  Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct FileManagerActiveOperations {
  pub operations: Vec<FileManagerOperationStatus>,
}

#[typeshare]
#[derive(
  Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct FileManagerJournalStatus {
  pub can_undo: bool,
  pub can_redo: bool,
  pub undo_description: Option<String>,
  pub redo_description: Option<String>,
  pub expires_at: Option<I64>,
  /// Bytes retained for the current undo/redo history.
  #[serde(default)]
  pub retained_storage_bytes: U64,
  /// Server-safe explanation of where File Manager data is stored.
  #[serde(default)]
  pub storage_description: String,
}

#[typeshare]
#[derive(
  Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct FileManagerTransferTicket {
  pub operation_id: String,
  pub token: String,
  pub url: String,
  pub expires_at: I64,
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn destructive_operations_require_confirmation() {
    assert!(
      FileManagerOperation::Delete {
        paths: vec!["file".into()]
      }
      .requires_confirmation()
    );
    assert!(
      !FileManagerOperation::CreateFile {
        path: "file".into()
      }
      .requires_confirmation()
    );
    assert!(
      !FileManagerOperation::ExtractArchive {
        path: "archive.zip".into(),
        destination: "archive".into(),
      }
      .requires_confirmation()
    );
  }

  #[test]
  fn only_reversible_file_operations_enter_undo_history() {
    assert!(
      FileManagerOperation::Delete {
        paths: vec!["file".into()]
      }
      .is_undoable()
    );
    assert!(
      !FileManagerOperation::WriteText {
        path: "file".into(),
        contents: "changed".into(),
        expected_revision: Default::default(),
      }
      .is_undoable()
    );
    assert!(
      !FileManagerOperation::ExtractArchive {
        path: "archive.zip".into(),
        destination: "archive".into(),
      }
      .is_undoable()
    );
  }

  #[test]
  fn write_text_does_not_expose_contents_as_affected_path() {
    let op = FileManagerOperation::WriteText {
      path: "compose.yaml".into(),
      contents: "secret".into(),
      expected_revision: Default::default(),
    };
    assert_eq!(op.affected_paths(), ["compose.yaml"]);
  }
}
