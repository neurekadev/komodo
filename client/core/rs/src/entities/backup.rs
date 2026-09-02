use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use super::{I64, U64};

/// A secret submitted to Core. APIs never return the plaintext value.
#[typeshare]
#[derive(Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct BackupSecret {
  /// A new plaintext value. Empty preserves the currently sealed value.
  #[serde(default, skip_serializing_if = "String::is_empty")]
  pub value: String,
  /// True when Core has a sealed value. Ignored on writes.
  #[serde(default)]
  pub configured: bool,
}

impl std::fmt::Debug for BackupSecret {
  fn fmt(
    &self,
    formatter: &mut std::fmt::Formatter<'_>,
  ) -> std::fmt::Result {
    formatter
      .debug_struct("BackupSecret")
      .field("value", &"[REDACTED]")
      .field(
        "configured",
        &(self.configured || !self.value.is_empty()),
      )
      .finish()
  }
}

impl BackupSecret {
  /// Remove secret material before returning a repository through an API.
  pub fn redact(&mut self) {
    self.configured |= !self.value.is_empty();
    self.value.clear();
  }
}

/// A repository supported by Vykar v0.19.1.
#[typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "params")]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub enum BackupRepositoryBackend {
  /// Storage local to Core. Core exposes it to Periphery through the
  /// authenticated embedded Vykar REST service.
  CoreLocal { path: String },
  /// S3 or an S3-compatible object store such as Backblaze B2.
  S3 {
    url: String,
    region: String,
    /// Authoritative credentials used only by Core for maintenance.
    access_key_id: BackupSecret,
    secret_access_key: BackupSecret,
    /// Distinct worker-scoped credentials. Their storage policy must deny
    /// deletion, compaction, and other maintenance operations.
    #[serde(default)]
    worker_access_key_id: BackupSecret,
    #[serde(default)]
    worker_secret_access_key: BackupSecret,
    #[serde(default)]
    soft_delete: bool,
  },
  /// SFTP storage. The key and known-hosts files must exist on the worker.
  Sftp {
    url: String,
    /// Authoritative key used only by Core for maintenance.
    private_key: BackupSecret,
    /// Distinct worker-scoped key whose account cannot delete or maintain the
    /// authoritative repository.
    #[serde(default)]
    worker_private_key: BackupSecret,
    known_hosts: String,
    #[serde(default = "default_sftp_timeout_seconds")]
    timeout_seconds: U64,
  },
  /// A Vykar REST repository.
  Rest {
    url: String,
    /// Authoritative token used only by Core for maintenance.
    access_token: BackupSecret,
    /// Distinct append-only or otherwise maintenance-denied worker token.
    #[serde(default)]
    worker_access_token: BackupSecret,
    #[serde(default)]
    allow_insecure_http: bool,
  },
}

fn default_sftp_timeout_seconds() -> U64 {
  30
}

impl Default for BackupRepositoryBackend {
  fn default() -> Self {
    Self::CoreLocal {
      path: "/data/backups/vykar".into(),
    }
  }
}

impl BackupRepositoryBackend {
  pub fn redact(&mut self) {
    match self {
      Self::CoreLocal { .. } => {}
      Self::S3 {
        access_key_id,
        secret_access_key,
        worker_access_key_id,
        worker_secret_access_key,
        ..
      } => {
        access_key_id.redact();
        secret_access_key.redact();
        worker_access_key_id.redact();
        worker_secret_access_key.redact();
      }
      Self::Sftp {
        private_key,
        worker_private_key,
        ..
      } => {
        private_key.redact();
        worker_private_key.redact();
      }
      Self::Rest {
        access_token,
        worker_access_token,
        ..
      } => {
        access_token.redact();
        worker_access_token.redact();
      }
    }
  }
}

/// An encrypted Vykar repository definition.
#[typeshare]
#[derive(
  Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct BackupRepository {
  pub name: String,
  pub backend: BackupRepositoryBackend,
  /// Vykar repository encryption passphrase. Encryption is mandatory.
  #[serde(default)]
  pub passphrase: BackupSecret,
}

impl BackupRepository {
  pub fn redact(&mut self) {
    self.backend.redact();
    self.passphrase.redact();
  }
}

/// Whether all, only selected, or all except selected resources are backed up.
#[typeshare]
#[derive(
  Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub enum BackupSelectionMode {
  #[default]
  All,
  Include,
  Exclude,
}

#[typeshare]
#[derive(
  Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Hash,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct BackupVolumeTarget {
  pub server_id: String,
  pub volume_name: String,
}

#[typeshare]
#[derive(
  Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct BackupStackSelection {
  #[serde(default)]
  pub mode: BackupSelectionMode,
  #[serde(default)]
  pub stack_ids: Vec<String>,
}

#[typeshare]
#[derive(
  Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct BackupVolumeSelection {
  #[serde(default)]
  pub mode: BackupSelectionMode,
  #[serde(default)]
  pub volumes: Vec<BackupVolumeTarget>,
}

/// Controls bandwidth-aware maintenance and verification.
#[typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct BackupAdvancedSettings {
  /// Maximum simultaneous node operations.
  pub node_concurrency: U64,
  /// Optional per-node upload limit in bytes per second. Zero is unlimited.
  pub upload_bytes_per_second: U64,
  /// Maximum client-side data repacked during one S3/SFTP maintenance cycle.
  pub client_repack_limit_bytes: U64,
  /// Vykar dead-data ratio that triggers compaction.
  pub compact_threshold_percent: U64,
  /// Full repository verification interval.
  pub full_verify_every_days: U64,
  /// Percentage of packs sampled after each cycle.
  pub verify_sample_percent: U64,
}

impl Default for BackupAdvancedSettings {
  fn default() -> Self {
    Self {
      node_concurrency: 2,
      upload_bytes_per_second: 0,
      client_repack_limit_bytes: 5 * 1024 * 1024 * 1024,
      compact_threshold_percent: 20,
      full_verify_every_days: 60,
      verify_sample_percent: 5,
    }
  }
}

/// Singleton backup configuration. Core is always the scheduler.
#[typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct BackupSettings {
  /// Whether the shared backup schedule is active.
  pub enabled: bool,
  /// English schedule or five-field cron expression.
  pub schedule: String,
  /// IANA timezone used to evaluate the schedule.
  pub timezone: String,
  pub core_enabled: bool,
  pub stacks_enabled: bool,
  pub volumes_enabled: bool,
  pub core_keep_last: U64,
  pub stack_keep_last: U64,
  pub volume_keep_last: U64,
  pub stack_selection: BackupStackSelection,
  pub volume_selection: BackupVolumeSelection,
  /// Quiesce the affected running containers during backup.
  pub stop_containers: bool,
  /// Traverse filesystem boundaries encountered beneath a backup source and
  /// include Stack bind roots stored on a different filesystem.
  #[serde(default)]
  pub include_cross_filesystem_mounts: bool,
  /// Include Docker volumes with daemon-generated anonymous names in Volume
  /// backups. Disabled by default.
  #[serde(default)]
  pub include_anonymous_volumes: bool,
  /// Vykar/gitignore-style patterns selecting eligible absolute Stack bind
  /// source paths. An empty list includes every otherwise eligible bind.
  #[serde(default)]
  pub bind_mount_include_patterns: Vec<String>,
  /// Vykar/gitignore-style patterns excluding absolute Stack bind source
  /// paths after the include rules are evaluated.
  #[serde(default)]
  pub bind_mount_exclude_patterns: Vec<String>,
  pub primary: BackupRepository,
  pub mirror: Option<BackupRepository>,
  pub advanced: BackupAdvancedSettings,
  /// Changes whenever settings are saved.
  #[serde(default)]
  pub updated_at: I64,
}

impl Default for BackupSettings {
  fn default() -> Self {
    Self {
      enabled: false,
      schedule: "0 1 * * *".into(),
      timezone: "UTC".into(),
      core_enabled: true,
      stacks_enabled: true,
      volumes_enabled: true,
      core_keep_last: 14,
      stack_keep_last: 14,
      volume_keep_last: 14,
      stack_selection: Default::default(),
      volume_selection: Default::default(),
      stop_containers: true,
      include_cross_filesystem_mounts: false,
      include_anonymous_volumes: false,
      bind_mount_include_patterns: Vec::new(),
      bind_mount_exclude_patterns: Vec::new(),
      primary: BackupRepository {
        name: "Primary".into(),
        ..Default::default()
      },
      mirror: None,
      advanced: Default::default(),
      updated_at: 0,
    }
  }
}

impl BackupSettings {
  pub fn redact(&mut self) {
    self.primary.redact();
    if let Some(mirror) = &mut self.mirror {
      mirror.redact();
    }
  }
}

#[typeshare]
#[derive(
  Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Hash,
)]
#[serde(tag = "type", content = "params")]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub enum BackupTarget {
  #[default]
  Core,
  Stack {
    stack_id: String,
  },
  Volume {
    server_id: String,
    volume_name: String,
  },
  /// A valid Vykar snapshot without a current Komodo source binding.
  Unbound {
    source_label: String,
  },
}

impl BackupTarget {
  pub fn source_label(&self, core_instance_id: &str) -> String {
    match self {
      Self::Core => format!("komodo/v1/core/{core_instance_id}"),
      Self::Stack { stack_id } => {
        format!("komodo/v1/stack/{stack_id}")
      }
      Self::Volume {
        server_id,
        volume_name,
      } => format!("komodo/v1/volume/{server_id}/{volume_name}"),
      Self::Unbound { source_label } => source_label.clone(),
    }
  }
}

#[typeshare]
#[derive(
  Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq,
)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub enum BackupRunState {
  #[default]
  Queued,
  Running,
  Complete,
  Partial,
  Failed,
  Cancelled,
}

#[typeshare]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct BackupRun {
  pub id: String,
  pub target: Option<BackupTarget>,
  pub state: BackupRunState,
  /// Whether this operation supports cancellation while it is active.
  #[serde(default)]
  pub cancellable: bool,
  pub message: String,
  pub started_at: I64,
  pub finished_at: I64,
  pub retry_count: U64,
}

#[typeshare]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct BackupStatus {
  #[serde(default)]
  pub active_runs: Vec<BackupRun>,
  pub recent_runs: Vec<BackupRun>,
  pub next_run_at: I64,
  pub primary_healthy: bool,
  pub mirror_healthy: Option<bool>,
  pub mirror_lagging_snapshots: U64,
  pub last_full_verification_at: I64,
  pub critical_alert: Option<String>,
}

#[typeshare]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct BackupSnapshot {
  pub name: String,
  pub source_label: String,
  pub hostname: String,
  pub target: BackupTarget,
  /// Absolute source roots recorded by Vykar at backup time.
  pub source_paths: Vec<String>,
  /// Exact user-restorable roots, excluding Komodo's internal manifest.
  #[serde(default)]
  pub restorable_source_paths: Vec<String>,
  /// Whether the current Stack still resolves to the snapshot's source roots.
  /// Populated by Core when snapshots are listed for a Stack.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub source_paths_match_current: Option<bool>,
  pub created_at: I64,
  pub original_size: U64,
  pub stored_size: U64,
  pub file_count: U64,
  pub partial: bool,
  pub run_id: String,
  pub manifest_checksum: String,
}

#[typeshare]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct BackupSnapshotItem {
  pub path: String,
  pub name: String,
  pub directory: bool,
  pub size: U64,
  pub modified_at: I64,
  pub has_children: bool,
}

#[typeshare]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct BackupRestorePlan {
  pub id: String,
  pub snapshot: String,
  pub source: BackupTarget,
  pub destination_server_id: Option<String>,
  pub selected_paths: Vec<String>,
  pub created_paths: Vec<String>,
  pub overwritten_paths: Vec<String>,
  pub deleted_paths: Vec<String>,
  pub containers_to_stop: Vec<String>,
  pub expires_at: I64,
}

/// A validated, short-lived plan for replacing the active Core database.
#[typeshare]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct CoreRecoveryPlan {
  pub id: String,
  pub snapshot: String,
  pub current_database: String,
  pub validation_database: String,
  pub backup_schema: String,
  pub backup_version: String,
  pub expires_at: I64,
}

/// Returns true when a resource is included by a selection.
pub fn selection_includes<T: PartialEq>(
  mode: BackupSelectionMode,
  selected: &[T],
  target: &T,
) -> bool {
  match mode {
    BackupSelectionMode::All => true,
    BackupSelectionMode::Include => selected.contains(target),
    BackupSelectionMode::Exclude => !selected.contains(target),
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn backup_run_cancellation_is_explicit_and_defaults_to_disabled() {
    let run = BackupRun {
      state: BackupRunState::Running,
      cancellable: true,
      ..Default::default()
    };
    let mut value = serde_json::to_value(run).unwrap();
    assert!(
      serde_json::from_value::<BackupRun>(value.clone())
        .unwrap()
        .cancellable
    );
    value.as_object_mut().unwrap().remove("cancellable");
    assert!(
      !serde_json::from_value::<BackupRun>(value)
        .unwrap()
        .cancellable
    );
    assert!(!BackupRun::default().cancellable);
  }

  #[test]
  fn selection_modes_are_unambiguous() {
    let selected = vec!["a", "b"];
    assert!(selection_includes(
      BackupSelectionMode::All,
      &selected,
      &"c"
    ));
    assert!(selection_includes(
      BackupSelectionMode::Include,
      &selected,
      &"a"
    ));
    assert!(!selection_includes(
      BackupSelectionMode::Include,
      &selected,
      &"c"
    ));
    assert!(!selection_includes(
      BackupSelectionMode::Exclude,
      &selected,
      &"a"
    ));
    assert!(selection_includes(
      BackupSelectionMode::Exclude,
      &selected,
      &"c"
    ));
  }

  #[test]
  fn mount_and_anonymous_volume_safety_defaults_are_disabled() {
    let settings = BackupSettings::default();
    assert!(!settings.include_cross_filesystem_mounts);
    assert!(!settings.include_anonymous_volumes);
    assert!(settings.bind_mount_include_patterns.is_empty());
    assert!(settings.bind_mount_exclude_patterns.is_empty());
  }

  #[test]
  fn labels_are_stable_and_scoped() {
    assert_eq!(
      BackupTarget::Core.source_label("core-a"),
      "komodo/v1/core/core-a"
    );
    assert_eq!(
      BackupTarget::Stack {
        stack_id: "s1".into()
      }
      .source_label("ignored"),
      "komodo/v1/stack/s1"
    );
    assert_eq!(
      BackupTarget::Volume {
        server_id: "n1".into(),
        volume_name: "data".into(),
      }
      .source_label("ignored"),
      "komodo/v1/volume/n1/data"
    );
  }

  #[test]
  fn repository_redaction_preserves_configured_state() {
    let mut repo = BackupRepository {
      name: "test".into(),
      backend: BackupRepositoryBackend::Rest {
        url: "https://backup.example".into(),
        access_token: BackupSecret {
          value: "secret".into(),
          configured: false,
        },
        worker_access_token: BackupSecret {
          value: "worker-secret".into(),
          configured: false,
        },
        allow_insecure_http: false,
      },
      passphrase: BackupSecret {
        value: "passphrase".into(),
        configured: false,
      },
    };
    repo.redact();
    assert!(repo.passphrase.value.is_empty());
    assert!(repo.passphrase.configured);
    let BackupRepositoryBackend::Rest {
      access_token,
      worker_access_token,
      ..
    } = repo.backend
    else {
      panic!("wrong backend")
    };
    assert!(access_token.value.is_empty());
    assert!(access_token.configured);
    assert!(worker_access_token.value.is_empty());
    assert!(worker_access_token.configured);
  }
}
