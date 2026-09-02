use std::{
  collections::{BTreeMap, HashMap, HashSet},
  fs::OpenOptions,
  io::{Read, Write},
  os::unix::fs::OpenOptionsExt,
  path::{Path, PathBuf},
  sync::{
    Arc, Mutex, OnceLock, RwLock,
    atomic::{AtomicBool, Ordering},
  },
};

use anyhow::{Context, anyhow};
use database::{
  bson::{Bson, doc, to_bson, to_document},
  mungos::{
    find::find_collect,
    mongodb::{
      Collection,
      options::{FindOptions, UpdateOptions},
    },
  },
};
use futures_util::{StreamExt, stream::FuturesUnordered};
use komodo_backup::{
  SnapshotDirectoryPage, VykarRepository,
  backup_manifest_source_name, normalize_selected_paths,
  parse_source_label, snapshot_name,
};
use komodo_client::{
  api::write::PlanBackupRestore,
  entities::{
    backup::{
      BackupRepository, BackupRepositoryBackend, BackupRestorePlan,
      BackupRun, BackupRunState, BackupSecret, BackupSelectionMode,
      BackupSettings, BackupSnapshot, BackupStatus, BackupTarget,
      BackupVolumeTarget, CoreRecoveryPlan, selection_includes,
    },
    docker::volume::{VolumeListItem, VolumeScopeEnum},
    komodo_timestamp,
    repo::Repo,
    server::Server,
    stack::{Stack, StackConfig},
    user::User,
  },
};
use periphery_client::api::backup::{
  BackupSourceFilters, CancelVykarOperation, DiscoverBackupSource,
  FinalizeVykarRestore, PeripheryBackupTarget, PreflightVykarRestore,
  PreflightVykarRestoreResponse, ProtectedRepositoryPath,
  RunVykarBackup, RunVykarBackupBatch, TransactionalVykarRestore,
  VykarBackupTask, VykarRetainedSnapshot,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
  config::core_config,
  helpers::{periphery_client, query::id_or_name_filter},
  permission::get_check_permissions,
  resource::{self, KomodoResource},
  state::{
    CORE_RECOVERY_ACTIVATION_PATH, db_client,
    read_core_recovery_activation,
  },
};

mod crypto;

const SETTINGS_ID: &str = "singleton";
const SETTINGS_COLLECTION: &str = "BackupSettings";
const RUNS_COLLECTION: &str = "BackupRun";
const PLANS_COLLECTION: &str = "BackupRestorePlan";
const CORE_RECOVERY_COLLECTION: &str = "CoreRecoveryPlan";
const HEALTH_COLLECTION: &str = "BackupRepositoryHealth";
const OPERATIONAL_ALERT_PATH: &str =
  "/data/backup-operational-alert.json";
const CORE_PRIVATE_PATH: &str = "/core-secrets";
const CORE_STAGING_PATH: &str = "/core-secrets/.komodo-core-staging";
const CORE_CACHE_PATH: &str = "/data/backups/.komodo-vykar-cache";
const CORE_RECOVERY_STAGING_PATH: &str =
  "/core-secrets/.komodo-core-recovery";
const STACK_MANIFEST_STAGING_PATH: &str =
  "/core-secrets/.komodo-stack-manifest";
const LEGACY_CORE_STAGING_PATHS: [&str; 3] = [
  "/data/backups/.komodo-core-staging",
  "/data/backups/.komodo-core-recovery",
  "/data/backups/.komodo-stack-manifest",
];
const CORE_INSTANCE_ID_PATH: &str = "/data/keys/backup-instance-id";
const LEGACY_CORE_INSTANCE_ID_PATH: &str =
  "/config/keys/backup-instance-id";
const PERIPHERY_HOSTNAME_PREFIX: &str = "komodo-periphery-";
const CORE_RECOVERY_DATABASE_PREFIX: &str = "komodo_recovery_";
const MAX_FLEET_RETRY_ATTEMPTS: u32 = 8;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SealedBackupSettings {
  #[serde(rename = "_id")]
  id: String,
  sealed: String,
  updated_at: i64,
  /// Set only after the primary repository has initialized successfully.
  #[serde(default)]
  primary_initialized: bool,
  /// Set only after the mirror repository has initialized successfully.
  #[serde(default)]
  mirror_initialized: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredRestorePlan {
  #[serde(rename = "_id")]
  id: String,
  /// Restore plans are capabilities scoped to the user who confirmed them.
  #[serde(default)]
  created_by: String,
  plan: BackupRestorePlan,
  publish: Vec<periphery_client::api::backup::RestorePublishPath>,
  #[serde(default)]
  recovered_stack_name: Option<String>,
  /// Set durably before the first publication request. Legacy plans have
  /// unknown execution state and must still be reconciled conservatively.
  #[serde(default = "legacy_restore_execution_started")]
  recovered_stack_execution_started: bool,
  /// Stack identity written after the atomically marked resource insert.
  #[serde(default)]
  recovered_stack_id: Option<String>,
  /// Core durably recorded Periphery's completed commit receipt.
  #[serde(default)]
  recovered_stack_finalized: bool,
  #[serde(default)]
  recovered_stack_run_directory: Option<String>,
  #[serde(default)]
  destination_volume_name: Option<String>,
  #[serde(default)]
  create_volume_if_missing: bool,
  #[serde(default)]
  destination_exists: bool,
  /// Immutable source metadata used when this plan creates a recovered Stack.
  #[serde(default)]
  recovered_stack_source: Option<Stack>,
  /// Missing source resources are recoverable only by administrators.
  #[serde(default, alias = "source_stack_missing")]
  source_resource_missing: bool,
  /// Authenticated roots from the snapshot manifest. An in-place Stack
  /// restore is valid only while the live Stack still resolves to these
  /// exact roots.
  #[serde(default)]
  snapshot_stack_source_paths: Vec<String>,
  /// Original absolute Compose bind source to its canonical snapshot path.
  #[serde(default)]
  snapshot_stack_path_aliases: HashMap<String, String>,
  /// Source absolute bind path to destination absolute bind path. Retained
  /// with the confirmed plan so execution cannot substitute new mappings.
  #[serde(default)]
  bind_path_mappings: HashMap<String, String>,
}

fn legacy_restore_execution_started() -> bool {
  true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredCoreRecoveryPlan {
  #[serde(rename = "_id")]
  id: String,
  #[serde(default)]
  created_by: String,
  recovered_core_instance_id: String,
  plan: CoreRecoveryPlan,
}

#[derive(Debug, Deserialize)]
struct SnapshotBackupManifest {
  schema: String,
  version: u32,
  run_id: String,
  source_label: String,
  hostname: String,
  paths: Vec<String>,
  #[serde(default)]
  path_aliases: BTreeMap<String, String>,
  target: PeripheryBackupTarget,
  configuration_sha256: String,
  paths_sha256: String,
  #[serde(default)]
  path_aliases_sha256: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct RepositoryHealthRecord {
  #[serde(rename = "_id")]
  id: String,
  healthy: bool,
  checked_at: i64,
  /// Full inventory health is shared across polling clients for five minutes.
  #[serde(default)]
  inventory_checked_at: i64,
  #[serde(default)]
  mirror_lagging_snapshots: u64,
  #[serde(default)]
  last_full_verification_at: i64,
  /// Remains set after an integrity check fails until a full check succeeds.
  #[serde(default)]
  verification_failed: bool,
}

fn settings_collection() -> Collection<SealedBackupSettings> {
  db_client().db.collection(SETTINGS_COLLECTION)
}

fn runs_collection() -> Collection<BackupRun> {
  db_client().db.collection(RUNS_COLLECTION)
}

fn plans_collection() -> Collection<StoredRestorePlan> {
  db_client().db.collection(PLANS_COLLECTION)
}

fn core_recovery_collection() -> Collection<StoredCoreRecoveryPlan> {
  db_client().db.collection(CORE_RECOVERY_COLLECTION)
}

fn health_collection() -> Collection<RepositoryHealthRecord> {
  db_client().db.collection(HEALTH_COLLECTION)
}

fn core_export_includes_collection(name: &str) -> bool {
  !matches!(name, SETTINGS_COLLECTION | RUNS_COLLECTION)
}

pub async fn get_settings() -> anyhow::Result<BackupSettings> {
  let Some(record) = settings_collection()
    .find_one(doc! { "_id": SETTINGS_ID })
    .await
    .context("Failed to load backup settings")?
  else {
    let settings = BackupSettings {
      timezone: if core_config().timezone.is_empty() {
        "UTC".into()
      } else {
        core_config().timezone.clone()
      },
      ..Default::default()
    };
    return Ok(settings);
  };
  let bytes = crypto::open(&record.sealed)?;
  serde_json::from_slice(&bytes)
    .context("Failed to decode sealed backup settings")
}

pub async fn get_redacted_settings() -> anyhow::Result<BackupSettings>
{
  let mut settings = match get_settings().await {
    Ok(settings) => settings,
    Err(error) => {
      record_configuration_alert(&error);
      BackupSettings {
        timezone: if core_config().timezone.is_empty() {
          "UTC".into()
        } else {
          core_config().timezone.clone()
        },
        ..Default::default()
      }
    }
  };
  settings.redact();
  Ok(settings)
}

pub async fn save_settings(
  proposed: BackupSettings,
) -> anyhow::Result<BackupSettings> {
  let _repository_roles =
    repository_role_barrier().clone().write_owned().await;
  save_settings_inner(proposed, false).await
}

async fn save_settings_after_promotion(
  proposed: BackupSettings,
) -> anyhow::Result<BackupSettings> {
  // Health is keyed by role, not repository identity. Invalidate both roles
  // before the swap so any later failure leaves maintenance in the safe
  // full-verification-required state rather than assigning stale health to the
  // opposite repository.
  health_collection()
    .delete_many(doc! { "_id": { "$in": ["primary", "mirror"] } })
    .await?;
  save_settings_inner(proposed, true).await
}

async fn save_settings_inner(
  mut proposed: BackupSettings,
  allow_primary_location_change: bool,
) -> anyhow::Result<BackupSettings> {
  normalize_bind_mount_patterns(
    &mut proposed.bind_mount_include_patterns,
  );
  normalize_bind_mount_patterns(
    &mut proposed.bind_mount_exclude_patterns,
  );
  validate_settings(&proposed)?;
  let (existing, primary_initialized, mirror_initialized) =
    match settings_collection()
      .find_one(doc! { "_id": SETTINGS_ID })
      .await
      .context("Failed to load existing backup settings")?
    {
      Some(record) => {
        let primary_initialized = record.primary_initialized;
        match crypto::open(&record.sealed).and_then(|bytes| {
          serde_json::from_slice::<BackupSettings>(&bytes)
            .context("Failed to decode sealed backup settings")
        }) {
          Ok(settings) => (
            Some(settings),
            primary_initialized,
            record.mirror_initialized,
          ),
          Err(error) => {
            // An administrator can repair an unreadable sealed record by
            // submitting a complete replacement. No unavailable secret or
            // initialization state is inherited from the damaged record.
            warn!(
              "Replacing unreadable sealed backup settings: {error:#}"
            );
            (None, false, false)
          }
        }
      }
      None => (None, false, false),
    };
  let primary_location_unchanged = existing
    .as_ref()
    .map(|existing| {
      repositories_share_location(
        &proposed.primary,
        &existing.primary,
      )
    })
    .transpose()?
    .unwrap_or(true);
  if let Some(existing) = &existing {
    if primary_initialized
      && !allow_primary_location_change
      && !primary_location_unchanged
    {
      return Err(anyhow!(
        "Primary repository location cannot be changed after initialization; configure a mirror and use verified promotion"
      ));
    }
    merge_repository_secrets(
      &mut proposed.primary,
      &existing.primary,
      primary_location_unchanged,
      primary_initialized,
    )?;
    match (&mut proposed.mirror, &existing.mirror) {
      (Some(proposed), Some(existing)) => {
        let location_unchanged =
          repositories_share_location(proposed, existing)?;
        merge_repository_secrets(
          proposed,
          existing,
          location_unchanged,
          mirror_initialized,
        )?
      }
      (Some(proposed), None) => require_repository_secrets(proposed)?,
      _ => {}
    }
  } else {
    require_repository_secrets(&proposed.primary)?;
    if let Some(mirror) = &proposed.mirror {
      require_repository_secrets(mirror)?;
    }
  }
  if !primary_location_unchanged {
    // Health is keyed by role. A corrected pre-initialization location must
    // not inherit verification evidence from the replaced repository.
    health_collection()
      .delete_one(doc! { "_id": "primary" })
      .await?;
  }
  let mirror_changed = match (
    existing
      .as_ref()
      .and_then(|settings| settings.mirror.as_ref()),
    proposed.mirror.as_ref(),
  ) {
    (None, None) => false,
    (Some(existing), Some(proposed)) => {
      !repositories_share_location(existing, proposed)?
    }
    _ => true,
  };
  if mirror_changed {
    // Health is keyed by role. A removed or replaced mirror must never inherit
    // the previous mirror's full-verification timestamp or failure latch.
    // Delete before publishing settings so any persistence failure is safe.
    health_collection()
      .delete_one(doc! { "_id": "mirror" })
      .await?;
  }
  proposed.updated_at = komodo_timestamp();
  let bytes = serde_json::to_vec(&proposed)?;
  let record = SealedBackupSettings {
    id: SETTINGS_ID.into(),
    sealed: crypto::seal(&bytes)?,
    updated_at: proposed.updated_at,
    primary_initialized: primary_initialized
      || allow_primary_location_change,
    mirror_initialized: if allow_primary_location_change {
      proposed.mirror.is_some()
    } else if mirror_changed {
      false
    } else {
      mirror_initialized
    },
  };
  settings_collection()
    .update_one(
      doc! { "_id": SETTINGS_ID },
      doc! { "$set": to_document(&record)? },
    )
    .with_options(UpdateOptions::builder().upsert(true).build())
    .await
    .context("Failed to persist sealed backup settings")?;
  clear_configuration_alert();
  invalidate_fleet_retries();
  notify_scheduler();
  let mut redacted = proposed;
  redacted.redact();
  Ok(redacted)
}

fn normalize_bind_mount_patterns(patterns: &mut Vec<String>) {
  *patterns = patterns
    .drain(..)
    .map(|pattern| pattern.trim_end_matches('\r').to_string())
    .filter(|pattern| !pattern.trim().is_empty())
    .collect();
}

async fn mark_repository_initialized(
  mut settings: BackupSettings,
  mirror: bool,
) -> anyhow::Result<()> {
  let field = if mirror {
    "mirror_initialized"
  } else {
    "primary_initialized"
  };
  let update = if mirror {
    doc! { "$set": { "mirror_initialized": true } }
  } else {
    doc! { "$set": { "primary_initialized": true } }
  };
  let updated = settings_collection()
    .update_one(doc! { "_id": SETTINGS_ID }, update)
    .await
    .with_context(|| {
      format!("Failed to record {field} repository state")
    })?;
  if updated.matched_count > 0 {
    return Ok(());
  }

  // Initialization is also available before the first explicit settings save.
  // Persist the effective defaults so a successfully initialized repository
  // cannot later be silently replaced by editing a new settings record.
  settings.updated_at = komodo_timestamp();
  let bytes = serde_json::to_vec(&settings)?;
  let record = SealedBackupSettings {
    id: SETTINGS_ID.into(),
    sealed: crypto::seal(&bytes)?,
    updated_at: settings.updated_at,
    primary_initialized: !mirror,
    mirror_initialized: mirror,
  };
  settings_collection()
    .update_one(
      doc! { "_id": SETTINGS_ID },
      doc! { "$set": to_document(&record)? },
    )
    .with_options(UpdateOptions::builder().upsert(true).build())
    .await
    .context("Failed to persist initialized backup settings")?;
  Ok(())
}

fn validate_settings(
  settings: &BackupSettings,
) -> anyhow::Result<()> {
  if settings.schedule.trim().is_empty() {
    return Err(anyhow!("Backup schedule cannot be empty"));
  }
  settings
    .timezone
    .parse::<chrono_tz::Tz>()
    .context("Backup timezone must be a valid IANA timezone")?;
  compute_next_run(settings)
    .context("Backup schedule is not valid")?;
  for (name, keep) in [
    ("Core", settings.core_keep_last),
    ("Stack", settings.stack_keep_last),
    ("Volume", settings.volume_keep_last),
  ] {
    if keep == 0 {
      return Err(anyhow!("{name} retention must be at least one"));
    }
  }
  if !(1..=100).contains(&settings.advanced.compact_threshold_percent)
  {
    return Err(anyhow!(
      "Compaction threshold must be between 1 and 100"
    ));
  }
  if !(1..=64).contains(&settings.advanced.node_concurrency) {
    return Err(anyhow!("Node concurrency must be between 1 and 64"));
  }
  if !settings
    .advanced
    .upload_bytes_per_second
    .is_multiple_of(1024 * 1024)
  {
    return Err(anyhow!(
      "Upload limit must be configured in whole MiB/s increments"
    ));
  }
  if settings.advanced.client_repack_limit_bytes == 0 {
    return Err(anyhow!(
      "Client-side repack limit must be greater than zero"
    ));
  }
  if settings.advanced.full_verify_every_days == 0
    || !(1..=100).contains(&settings.advanced.verify_sample_percent)
  {
    return Err(anyhow!(
      "Verification interval must be positive and sample percentage must be between 1 and 100"
    ));
  }
  komodo_backup::VykarPatternMatcher::new(
    &settings.bind_mount_include_patterns,
  )
  .context("Invalid bind-mount include patterns")?;
  komodo_backup::VykarPatternMatcher::new(
    &settings.bind_mount_exclude_patterns,
  )
  .context("Invalid bind-mount exclude patterns")?;
  validate_repository_definition(&settings.primary)?;
  if let Some(mirror) = &settings.mirror {
    validate_repository_definition(mirror)?;
    if repositories_overlap(&settings.primary, mirror)? {
      return Err(anyhow!(
        "Primary and mirror must use different repository locations"
      ));
    }
  }
  Ok(())
}

fn validate_repository_definition(
  repository: &BackupRepository,
) -> anyhow::Result<()> {
  if repository.name.trim().is_empty() {
    return Err(anyhow!("Repository name cannot be empty"));
  }
  match &repository.backend {
    BackupRepositoryBackend::CoreLocal { path } => {
      if !Path::new(path).is_absolute() {
        return Err(anyhow!(
          "Core-local repository path must be absolute"
        ));
      }
      let normalized = normalize_core_local_path(path);
      if normalized.to_string_lossy() != path.as_str() {
        return Err(anyhow!(
          "Core-local repository path must be normalized exactly (no surrounding whitespace, '.', '..', duplicate separators, or trailing separator)"
        ));
      }
      let resolved = resolve_existing_path_ancestor(&normalized)?;
      for reserved in [CORE_PRIVATE_PATH, CORE_CACHE_PATH]
        .into_iter()
        .chain(LEGACY_CORE_STAGING_PATHS)
      {
        let reserved = normalize_core_local_path(reserved);
        let resolved_reserved =
          resolve_existing_path_ancestor(&reserved)?;
        if paths_overlap(&normalized, &reserved)
          || paths_overlap(&resolved, &resolved_reserved)
        {
          return Err(anyhow!(
            "Core-local repository path overlaps internal backup staging or cache data"
          ));
        }
      }
    }
    BackupRepositoryBackend::S3 { url, region, .. } => {
      if url.trim().is_empty() || region.trim().is_empty() {
        return Err(anyhow!(
          "S3 repository URL and region are required"
        ));
      }
    }
    BackupRepositoryBackend::Sftp {
      url, known_hosts, ..
    } => {
      if url.trim().is_empty() || known_hosts.trim().is_empty() {
        return Err(anyhow!(
          "SFTP repository URL and known-hosts entry are required"
        ));
      }
    }
    BackupRepositoryBackend::Rest {
      url,
      allow_insecure_http,
      ..
    } => {
      if url.trim().is_empty() {
        return Err(anyhow!("REST repository URL is required"));
      }
      if url.starts_with("http://") && !allow_insecure_http {
        return Err(anyhow!(
          "Plain HTTP REST repositories require explicit insecure-HTTP approval"
        ));
      }
    }
  }
  Ok(())
}

fn repository_location(repository: &BackupRepository) -> String {
  match &repository.backend {
    BackupRepositoryBackend::CoreLocal { path } => {
      let normalized = normalize_core_local_path(path);
      format!("core-local:{}", normalized.to_string_lossy())
    }
    BackupRepositoryBackend::S3 { url, .. } => {
      format!("s3:{}", normalize_repository_url(url))
    }
    BackupRepositoryBackend::Sftp { url, .. } => {
      format!("sftp:{}", normalize_repository_url(url))
    }
    BackupRepositoryBackend::Rest { url, .. } => {
      format!("rest:{}", normalize_repository_url(url))
    }
  }
}

fn repositories_share_location(
  primary: &BackupRepository,
  mirror: &BackupRepository,
) -> anyhow::Result<bool> {
  match (&primary.backend, &mirror.backend) {
    (
      BackupRepositoryBackend::CoreLocal { path: primary },
      BackupRepositoryBackend::CoreLocal { path: mirror },
    ) => {
      let primary = resolve_existing_path_ancestor(
        &normalize_core_local_path(primary),
      )?;
      let mirror = resolve_existing_path_ancestor(
        &normalize_core_local_path(mirror),
      )?;
      Ok(primary == mirror)
    }
    _ => {
      Ok(repository_location(primary) == repository_location(mirror))
    }
  }
}

fn repositories_overlap(
  primary: &BackupRepository,
  mirror: &BackupRepository,
) -> anyhow::Result<bool> {
  match (&primary.backend, &mirror.backend) {
    (
      BackupRepositoryBackend::CoreLocal { path: primary },
      BackupRepositoryBackend::CoreLocal { path: mirror },
    ) => Ok(paths_overlap(
      &resolve_existing_path_ancestor(&normalize_core_local_path(
        primary,
      ))?,
      &resolve_existing_path_ancestor(&normalize_core_local_path(
        mirror,
      ))?,
    )),
    _ => repositories_share_location(primary, mirror),
  }
}

fn normalize_core_local_path(path: &str) -> PathBuf {
  let mut normalized = PathBuf::new();
  for component in Path::new(path.trim()).components() {
    match component {
      std::path::Component::CurDir => {}
      std::path::Component::ParentDir => {
        normalized.pop();
      }
      component => normalized.push(component.as_os_str()),
    }
  }
  normalized
}

fn resolve_existing_path_ancestor(
  path: &Path,
) -> anyhow::Result<PathBuf> {
  let mut ancestor = path;
  let mut missing = Vec::new();
  loop {
    match ancestor.canonicalize() {
      Ok(mut resolved) => {
        while let Some(component) = missing.pop() {
          resolved.push(component);
        }
        return Ok(resolved);
      }
      Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
        let name = ancestor.file_name().with_context(|| {
          format!(
            "Path has no resolvable ancestor: {}",
            path.display()
          )
        })?;
        missing.push(name.to_os_string());
        ancestor = ancestor.parent().with_context(|| {
          format!(
            "Path has no resolvable ancestor: {}",
            path.display()
          )
        })?;
      }
      Err(error) => {
        return Err(error).with_context(|| {
          format!(
            "Failed to resolve path ancestor: {}",
            path.display()
          )
        });
      }
    }
  }
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
  left == right || left.starts_with(right) || right.starts_with(left)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CoreRecoveryActivation {
  database: String,
  core_instance_id: String,
  /// The database that was active immediately before this activation. Keep
  /// it available as the one-step rollback target until another recovery is
  /// activated.
  #[serde(default)]
  previous_database: Option<String>,
}

fn is_backup_manifest_source(
  snapshot_name: &str,
  path: &str,
) -> bool {
  let expected = backup_manifest_source_name(snapshot_name);
  Path::new(path).file_name().and_then(|name| name.to_str())
    == Some(expected.as_str())
}

fn normalize_repository_url(value: &str) -> String {
  let value = value.trim();
  if let Ok(mut url) = url::Url::parse(value) {
    let normalized_path =
      url.path().trim_end_matches('/').to_string();
    url.set_path(&normalized_path);
    url.to_string().trim_end_matches('/').to_string()
  } else {
    value.trim_end_matches('/').to_string()
  }
}

fn merge_repository_secrets(
  proposed: &mut BackupRepository,
  existing: &BackupRepository,
  location_unchanged: bool,
  initialized: bool,
) -> anyhow::Result<()> {
  if !location_unchanged {
    return require_repository_secrets(proposed);
  }
  if initialized
    && !proposed.passphrase.value.is_empty()
    && proposed.passphrase.value != existing.passphrase.value
  {
    return Err(anyhow!(
      "An initialized repository passphrase cannot be changed in place; configure and verify a new mirror repository instead"
    ));
  }
  preserve_secret(&mut proposed.passphrase, &existing.passphrase);
  match (&mut proposed.backend, &existing.backend) {
    (
      BackupRepositoryBackend::S3 {
        access_key_id,
        secret_access_key,
        worker_access_key_id,
        worker_secret_access_key,
        ..
      },
      BackupRepositoryBackend::S3 {
        access_key_id: old_access,
        secret_access_key: old_secret,
        worker_access_key_id: old_worker_access,
        worker_secret_access_key: old_worker_secret,
        ..
      },
    ) => {
      preserve_secret(access_key_id, old_access);
      preserve_secret(secret_access_key, old_secret);
      preserve_secret(worker_access_key_id, old_worker_access);
      preserve_secret(worker_secret_access_key, old_worker_secret);
    }
    (
      BackupRepositoryBackend::Sftp {
        private_key,
        worker_private_key,
        ..
      },
      BackupRepositoryBackend::Sftp {
        private_key: old_key,
        worker_private_key: old_worker_key,
        ..
      },
    ) => {
      preserve_secret(private_key, old_key);
      preserve_secret(worker_private_key, old_worker_key);
    }
    (
      BackupRepositoryBackend::Rest {
        access_token,
        worker_access_token,
        ..
      },
      BackupRepositoryBackend::Rest {
        access_token: old_token,
        worker_access_token: old_worker_token,
        ..
      },
    ) => {
      preserve_secret(access_token, old_token);
      preserve_secret(worker_access_token, old_worker_token);
    }
    (BackupRepositoryBackend::CoreLocal { .. }, _) => {}
    _ => require_repository_secrets(proposed)?,
  }
  require_repository_secrets(proposed)
}

fn preserve_secret(
  proposed: &mut BackupSecret,
  existing: &BackupSecret,
) {
  if proposed.value.is_empty() {
    proposed.value = existing.value.clone();
  }
  proposed.configured = false;
}

fn require_repository_secrets(
  repository: &BackupRepository,
) -> anyhow::Result<()> {
  if repository.passphrase.value.is_empty() {
    return Err(anyhow!(
      "Repository encryption passphrase is required"
    ));
  }
  let authoritative_valid = match &repository.backend {
    BackupRepositoryBackend::CoreLocal { .. } => true,
    BackupRepositoryBackend::S3 {
      access_key_id,
      secret_access_key,
      ..
    } => {
      !access_key_id.value.is_empty()
        && !secret_access_key.value.is_empty()
    }
    BackupRepositoryBackend::Sftp { private_key, .. } => {
      !private_key.value.is_empty()
    }
    BackupRepositoryBackend::Rest { access_token, .. } => {
      !access_token.value.is_empty()
    }
  };
  if !authoritative_valid {
    return Err(anyhow!(
      "Authoritative repository credentials are required"
    ));
  }
  require_worker_credentials(repository)
}

fn require_worker_credentials(
  repository: &BackupRepository,
) -> anyhow::Result<()> {
  let valid_and_distinct = match &repository.backend {
    BackupRepositoryBackend::CoreLocal { .. } => true,
    BackupRepositoryBackend::S3 {
      access_key_id,
      secret_access_key,
      worker_access_key_id,
      worker_secret_access_key,
      ..
    } => {
      !worker_access_key_id.value.is_empty()
        && !worker_secret_access_key.value.is_empty()
        && (worker_access_key_id.value != access_key_id.value
          || worker_secret_access_key.value
            != secret_access_key.value)
    }
    BackupRepositoryBackend::Sftp {
      private_key,
      worker_private_key,
      ..
    } => {
      !worker_private_key.value.is_empty()
        && worker_private_key.value != private_key.value
    }
    BackupRepositoryBackend::Rest {
      access_token,
      worker_access_token,
      ..
    } => {
      !worker_access_token.value.is_empty()
        && worker_access_token.value != access_token.value
    }
  };
  if valid_and_distinct {
    Ok(())
  } else {
    Err(anyhow!(
      "External repositories require distinct worker-scoped credentials whose policy denies deletion and maintenance"
    ))
  }
}

fn core_instance_id() -> anyhow::Result<&'static str> {
  static ID: OnceLock<Result<String, String>> = OnceLock::new();
  match ID.get_or_init(|| {
    load_or_create_core_instance_id().map_err(|error| {
      format!("Failed to load stable Core backup identity: {error:#}")
    })
  }) {
    Ok(id) => Ok(id),
    Err(error) => Err(anyhow!(error.clone())),
  }
}

fn load_or_create_core_instance_id() -> anyhow::Result<String> {
  match read_core_recovery_activation() {
    Ok(Some(bytes)) => {
      let activation: CoreRecoveryActivation =
        serde_json::from_slice(&bytes)
          .context("Invalid Core recovery activation record")?;
      if activation.core_instance_id.len() != 32
        || !activation
          .core_instance_id
          .chars()
          .all(|character| character.is_ascii_hexdigit())
      {
        return Err(anyhow!(
          "Invalid recovered Core backup identity"
        ));
      }
      return Ok(activation.core_instance_id);
    }
    Ok(None) => {}
    Err(error) => {
      return Err(error)
        .context("Failed to read Core recovery activation record");
    }
  }
  let path = Path::new(CORE_INSTANCE_ID_PATH);
  if let Some(id) = read_core_instance_id(path)? {
    return Ok(id);
  }
  if let Some(id) =
    read_core_instance_id(Path::new(LEGACY_CORE_INSTANCE_ID_PATH))?
  {
    return persist_core_instance_id(path, &id);
  }
  persist_core_instance_id(path, &Uuid::new_v4().simple().to_string())
}

fn read_core_instance_id(
  path: &Path,
) -> anyhow::Result<Option<String>> {
  let id = match std::fs::read_to_string(path) {
    Ok(id) => id,
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
      return Ok(None);
    }
    Err(error) => return Err(error.into()),
  };
  let id = id.trim();
  if id.len() == 32
    && id.chars().all(|character| character.is_ascii_hexdigit())
  {
    Ok(Some(id.to_string()))
  } else {
    Err(anyhow!("Persisted Core backup identity is invalid"))
  }
}

fn persist_core_instance_id(
  path: &Path,
  id: &str,
) -> anyhow::Result<String> {
  let parent = path
    .parent()
    .context("Core backup identity path has no parent")?;
  std::fs::create_dir_all(parent)?;
  match OpenOptions::new()
    .create_new(true)
    .write(true)
    .mode(0o600)
    .open(path)
  {
    Ok(mut file) => {
      file.write_all(id.as_bytes())?;
      file.sync_all()?;
      std::fs::File::open(parent)?.sync_all()?;
      Ok(id.to_string())
    }
    Err(error)
      if error.kind() == std::io::ErrorKind::AlreadyExists =>
    {
      read_core_instance_id(path)?.context(
        "Core backup identity disappeared after concurrent creation",
      )
    }
    Err(error) => Err(error.into()),
  }
}

fn persist_core_recovery_activation(
  database: &str,
  id: &str,
  previous_database: Option<&str>,
) -> anyhow::Result<()> {
  if id.len() != 32
    || !id.chars().all(|character| character.is_ascii_hexdigit())
  {
    return Err(anyhow!("Recovered Core backup identity is invalid"));
  }
  let valid_database_name = |database: &str| {
    !database.is_empty()
      && database.chars().all(|character| {
        character.is_ascii_alphanumeric()
          || matches!(character, '_' | '-')
      })
  };
  if !valid_database_name(database)
    || previous_database
      .is_some_and(|name| !valid_database_name(name))
  {
    return Err(anyhow!("Unsafe active database name"));
  }
  let destination = Path::new(CORE_RECOVERY_ACTIVATION_PATH);
  let parent = destination
    .parent()
    .context("Core backup identity path has no parent")?;
  std::fs::create_dir_all(parent)?;
  let temporary = parent.join(format!(
    ".backup-recovery-activation-{}.tmp",
    Uuid::new_v4().simple()
  ));
  let mut file = OpenOptions::new()
    .create_new(true)
    .write(true)
    .mode(0o600)
    .open(&temporary)?;
  file.write_all(&serde_json::to_vec(&CoreRecoveryActivation {
    database: database.to_string(),
    core_instance_id: id.to_string(),
    previous_database: previous_database.map(str::to_string),
  })?)?;
  file.sync_all()?;
  std::fs::rename(temporary, destination)?;
  std::fs::File::open(parent)?.sync_all()?;
  Ok(())
}

fn core_cache_dir() -> anyhow::Result<PathBuf> {
  let directory = PathBuf::from(CORE_CACHE_PATH);
  std::fs::create_dir_all(&directory)?;
  Ok(directory)
}

fn core_secret_dir() -> anyhow::Result<PathBuf> {
  let directory = PathBuf::from(CORE_PRIVATE_PATH);
  std::fs::create_dir_all(&directory)?;
  Ok(directory)
}

fn core_repository(
  repository: &BackupRepository,
  settings: &BackupSettings,
) -> anyhow::Result<VykarRepository> {
  VykarRepository::new(
    repository,
    &format!("komodo-core-{}", core_instance_id()?),
    &core_cache_dir()?,
    &core_secret_dir()?,
    &settings.advanced,
  )
}

/// Convert Core-local storage to the embedded authenticated REST endpoint, or
/// substitute distinct maintenance-denied credentials for an external
/// backend. Authoritative maintenance credentials never cross the
/// Core/Periphery boundary. Vykar writers do receive the repository
/// passphrase because its client-side encryption and deduplication require
/// read access. Workers sharing a repository must also trust each other's
/// backup writes: worker labels do not authenticate immutable contents or
/// isolate writers, as documented in the administrator guide.
fn repository_for_periphery(
  repository: &BackupRepository,
  mirror: bool,
) -> anyhow::Result<BackupRepository> {
  let BackupRepositoryBackend::CoreLocal { path } =
    &repository.backend
  else {
    require_worker_credentials(repository)?;
    let mut worker = repository.clone();
    match &mut worker.backend {
      BackupRepositoryBackend::S3 {
        access_key_id,
        secret_access_key,
        worker_access_key_id,
        worker_secret_access_key,
        ..
      } => {
        *access_key_id = std::mem::take(worker_access_key_id);
        *secret_access_key = std::mem::take(worker_secret_access_key);
      }
      BackupRepositoryBackend::Sftp {
        private_key,
        worker_private_key,
        ..
      } => {
        *private_key = std::mem::take(worker_private_key);
      }
      BackupRepositoryBackend::Rest {
        access_token,
        worker_access_token,
        ..
      } => {
        *access_token = std::mem::take(worker_access_token);
      }
      BackupRepositoryBackend::CoreLocal { .. } => unreachable!(),
    }
    return Ok(worker);
  };
  let registered = embedded_repository_paths()
    .read()
    .unwrap()
    .get(usize::from(mirror))
    .cloned()
    .flatten()
    .context(
      "Core-local repository was added after startup; restart Core before using it",
    )?;
  if registered.as_path() != Path::new(path) {
    return Err(anyhow!(
      "Core-local repository path changed after startup; restart Core before running backups"
    ));
  }
  Ok(BackupRepository {
    name: repository.name.clone(),
    backend: BackupRepositoryBackend::Rest {
      url: format!(
        "{}/vykar/{}",
        core_config().host.trim_end_matches('/'),
        if mirror { "mirror" } else { "primary" }
      ),
      access_token: BackupSecret {
        value: crypto::embedded_server_token()?,
        configured: false,
      },
      worker_access_token: BackupSecret::default(),
      allow_insecure_http: core_config().host.starts_with("http://"),
    },
    passphrase: repository.passphrase.clone(),
  })
}

fn core_local_repository_paths(
  settings: &BackupSettings,
) -> anyhow::Result<Vec<ProtectedRepositoryPath>> {
  std::iter::once(&settings.primary)
    .chain(settings.mirror.iter())
    .filter_map(|repository| match &repository.backend {
      BackupRepositoryBackend::CoreLocal { path } => {
        Some(komodo_backup::container::current_container_id()
          .context("Cannot identify the Core Docker container for repository protection; retain Docker's hostname mount or default container-ID hostname")
          .map(|core_container_id| ProtectedRepositoryPath {
            path: path.clone(),
            core_container_id,
          }))
      }
      _ => None,
    })
    .collect()
}

fn backup_source_filters(
  settings: &BackupSettings,
) -> BackupSourceFilters {
  BackupSourceFilters {
    include_cross_filesystem_mounts: settings
      .include_cross_filesystem_mounts,
    include_anonymous_volumes: settings.include_anonymous_volumes,
    bind_mount_include_patterns: settings
      .bind_mount_include_patterns
      .clone(),
    bind_mount_exclude_patterns: settings
      .bind_mount_exclude_patterns
      .clone(),
  }
}

pub async fn initialize_repositories() -> anyhow::Result<BackupRun> {
  let _operation = backup_operation_lock().lock().await;
  let _repository_roles =
    repository_role_barrier().clone().read_owned().await;
  let settings = get_settings().await?;
  let run =
    new_non_cancellable_run(None, "Initializing repositories")
      .await?;
  let result = async {
    for (index, repository) in std::iter::once(&settings.primary)
      .chain(settings.mirror.iter())
      .enumerate()
    {
      let repository = repository.clone();
      let repository_settings = settings.clone();
      tokio::task::spawn_blocking(move || {
        core_repository(&repository, &repository_settings)?.init()
      })
      .await
      .context("Vykar initialization worker failed")??;
      mark_repository_initialized(settings.clone(), index == 1)
        .await?;
    }
    anyhow::Ok(())
  }
  .await;
  match result {
    Ok(()) => {
      finish_run(run, BackupRunState::Complete, "Repositories ready")
        .await
    }
    Err(error) => {
      let message = format!("{error:#}");
      let _ = finish_run(run, BackupRunState::Failed, message).await;
      Err(error)
    }
  }
}

pub async fn finalize_interrupted_runs() -> anyhow::Result<u64> {
  let result = runs_collection()
    .update_many(
      doc! {
        "state": {
          "$in": [
            to_bson(&BackupRunState::Queued)?,
            to_bson(&BackupRunState::Running)?,
          ]
        }
      },
      doc! {
        "$set": {
          "state": to_bson(&BackupRunState::Failed)?,
          "message": "Core restarted before the backup operation completed",
          "finished_at": komodo_timestamp(),
          "cancellable": false,
        }
      },
    )
    .await
    .context("Failed to finalize interrupted backup runs")?;
  Ok(result.modified_count)
}

async fn new_run(
  target: Option<BackupTarget>,
  message: &str,
) -> anyhow::Result<BackupRun> {
  create_run(target, message, true).await
}

async fn new_non_cancellable_run(
  target: Option<BackupTarget>,
  message: &str,
) -> anyhow::Result<BackupRun> {
  create_run(target, message, false).await
}

async fn create_run(
  target: Option<BackupTarget>,
  message: &str,
  cancellable: bool,
) -> anyhow::Result<BackupRun> {
  let run = BackupRun {
    id: Uuid::new_v4().to_string(),
    target,
    state: BackupRunState::Running,
    cancellable,
    message: message.into(),
    started_at: komodo_timestamp(),
    ..Default::default()
  };
  if cancellable {
    register_cancellation_token(&run.id);
  } else {
    non_cancellable_runs()
      .lock()
      .unwrap()
      .insert(run.id.clone());
  }
  if let Err(error) = runs_collection().insert_one(&run).await {
    cancellation_tokens().lock().unwrap().remove(&run.id);
    non_cancellable_runs().lock().unwrap().remove(&run.id);
    return Err(error.into());
  }
  Ok(run)
}

async fn finish_run(
  mut run: BackupRun,
  state: BackupRunState,
  message: impl Into<String>,
) -> anyhow::Result<BackupRun> {
  run.state = state;
  run.message = message.into();
  run.finished_at = komodo_timestamp();
  run.cancellable = false;
  let filter = if state == BackupRunState::Cancelled {
    doc! { "id": &run.id }
  } else {
    doc! {
      "id": &run.id,
      "state": { "$ne": to_bson(&BackupRunState::Cancelled)? },
    }
  };
  let updated = runs_collection()
    .update_one(filter, doc! { "$set": to_document(&run)? })
    .await?;
  if updated.matched_count == 0 {
    return runs_collection()
      .find_one(doc! { "id": &run.id })
      .await?
      .context("Backup run disappeared while finishing");
  }
  non_cancellable_runs().lock().unwrap().remove(&run.id);
  Ok(run)
}

pub async fn status() -> anyhow::Result<BackupStatus> {
  let _repository_roles =
    repository_role_barrier().clone().read_owned().await;
  let recent_runs = find_collect(
    &runs_collection(),
    None,
    FindOptions::builder()
      .sort(doc! { "started_at": -1 })
      .limit(20)
      .build(),
  )
  .await
  .unwrap_or_default();
  let active_runs = find_collect(
    &runs_collection(),
    doc! {
      "state": {
        "$in": [
          to_bson(&BackupRunState::Queued)?,
          to_bson(&BackupRunState::Running)?,
        ]
      }
    },
    FindOptions::builder()
      .sort(doc! { "started_at": -1 })
      .build(),
  )
  .await
  .unwrap_or_default();
  // Coalesce concurrent browser polls before checking the shared persisted
  // health cache, so only one caller performs an expired inventory refresh.
  let _health_refresh = repository_health_refresh_lock().lock().await;
  let previous_primary = health_collection()
    .find_one(doc! { "_id": "primary" })
    .await
    .ok()
    .flatten()
    .unwrap_or_default();
  let previous_mirror = health_collection()
    .find_one(doc! { "_id": "mirror" })
    .await
    .ok()
    .flatten()
    .unwrap_or_default();
  let settings = match get_settings().await {
    Ok(settings) => settings,
    Err(error) => {
      record_configuration_alert(&error);
      return Ok(BackupStatus {
        active_runs,
        recent_runs,
        critical_alert: current_critical_alert(),
        ..Default::default()
      });
    }
  };
  if repository_health_cache_is_fresh(
    previous_primary.inventory_checked_at,
    settings.updated_at,
    komodo_timestamp(),
  ) {
    return Ok(BackupStatus {
      active_runs,
      recent_runs,
      next_run_at: next_scheduled_run().unwrap_or_default(),
      primary_healthy: previous_primary.healthy
        && !previous_primary.verification_failed,
      mirror_healthy: settings.mirror.as_ref().map(|_| {
        previous_mirror.healthy
          && !previous_mirror.verification_failed
      }),
      mirror_lagging_snapshots: if settings.mirror.is_some() {
        previous_primary.mirror_lagging_snapshots
      } else {
        0
      },
      last_full_verification_at: previous_primary
        .last_full_verification_at,
      critical_alert: current_critical_alert(),
    });
  }
  let primary_settings = settings.clone();
  let primary_repository = settings.primary.clone();
  let primary = tokio::task::spawn_blocking(move || {
    core_repository(&primary_repository, &primary_settings)?
      .list_snapshots()
      .map(|inventory| {
        (
          inventory
            .snapshots
            .into_iter()
            .map(|snapshot| (snapshot.name, snapshot.partial))
            .collect::<HashMap<_, _>>(),
          inventory.hidden == 0,
        )
      })
  })
  .await
  .context("Primary health worker failed")?;
  let primary_inventory_healthy =
    primary.as_ref().is_ok_and(|(_, healthy)| *healthy);
  let primary_healthy = primary_inventory_healthy
    && !previous_primary.verification_failed;
  let primary_names =
    primary.map(|(names, _)| names).unwrap_or_default();
  let (mirror_healthy, mirror_lagging_snapshots) =
    if let Some(mirror) = settings.mirror.clone() {
      let mirror_settings = settings.clone();
      let mirror = tokio::task::spawn_blocking(move || {
        core_repository(&mirror, &mirror_settings)?
          .list_snapshots()
          .map(|inventory| {
            (
              inventory
                .snapshots
                .into_iter()
                .map(|snapshot| (snapshot.name, snapshot.partial))
                .collect::<HashMap<_, _>>(),
              inventory.hidden == 0,
            )
          })
      })
      .await
      .context("Mirror health worker failed")?;
      match mirror {
        Ok((mirror_snapshots, healthy)) => (
          Some(healthy && !previous_mirror.verification_failed),
          primary_names
            .iter()
            .filter(|(name, primary_partial)| {
              !mirror_copy_is_sufficient(
                **primary_partial,
                mirror_snapshots.get(*name).copied(),
              )
            })
            .count() as u64,
        ),
        Err(_) => (Some(false), primary_names.len() as u64),
      }
    } else {
      (None, 0)
    };
  let checked_at = komodo_timestamp();
  let _ = health_collection()
    .update_one(
      doc! { "_id": "primary" },
      doc! { "$set": {
        "healthy": primary_healthy,
        "checked_at": checked_at,
        "inventory_checked_at": checked_at,
        "mirror_lagging_snapshots": mirror_lagging_snapshots as i64,
      } },
    )
    .with_options(UpdateOptions::builder().upsert(true).build())
    .await;
  if let Some(healthy) = mirror_healthy {
    let _ = health_collection()
      .update_one(
        doc! { "_id": "mirror" },
        doc! { "$set": {
          "healthy": healthy,
          "checked_at": checked_at,
        } },
      )
      .with_options(UpdateOptions::builder().upsert(true).build())
      .await;
  }
  Ok(BackupStatus {
    active_runs,
    recent_runs,
    next_run_at: next_scheduled_run().unwrap_or_default(),
    primary_healthy,
    mirror_healthy,
    mirror_lagging_snapshots,
    last_full_verification_at: previous_primary
      .last_full_verification_at,
    critical_alert: current_critical_alert(),
  })
}

fn repository_health_refresh_lock() -> &'static tokio::sync::Mutex<()>
{
  static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
  LOCK.get_or_init(Default::default)
}

fn repository_health_cache_is_fresh(
  checked_at: i64,
  settings_updated_at: i64,
  now: i64,
) -> bool {
  checked_at > 0
    && checked_at >= settings_updated_at
    && now >= checked_at
    && now.saturating_sub(checked_at) < 5 * 60 * 1000
}

#[derive(Default)]
struct CriticalAlerts {
  configuration: Option<String>,
  operational: Vec<String>,
  maintenance: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct PersistedOperationalAlerts {
  #[serde(default)]
  messages: Vec<String>,
  /// Preserve the previous single-alert format on upgrade.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  message: Option<String>,
}

impl CriticalAlerts {
  fn current(&self) -> Option<String> {
    let messages = self
      .configuration
      .iter()
      .chain(self.operational.iter())
      .chain(self.maintenance.iter())
      .map(String::as_str)
      .collect::<Vec<_>>();
    (!messages.is_empty()).then(|| messages.join("\n"))
  }
}

fn critical_alerts() -> &'static RwLock<CriticalAlerts> {
  static ALERTS: OnceLock<RwLock<CriticalAlerts>> = OnceLock::new();
  ALERTS.get_or_init(|| {
    let operational = match read_operational_alert(Path::new(
      OPERATIONAL_ALERT_PATH,
    )) {
      Ok(message) => message,
      Err(error) => vec![format!(
        "Persisted backup operational alert is unreadable: {error:#}"
      )],
    };
    RwLock::new(CriticalAlerts {
      operational,
      ..Default::default()
    })
  })
}

fn current_critical_alert() -> Option<String> {
  critical_alerts().read().unwrap().current()
}

fn record_operational_alert(message: String) {
  let mut alerts = critical_alerts().write().unwrap();
  append_operational_alert(&mut alerts.operational, message);
  if let Err(error) = persist_operational_alert(
    Path::new(OPERATIONAL_ALERT_PATH),
    &alerts.operational,
  ) {
    error!("Failed to persist backup operational alert: {error:#}");
    append_operational_alert(
      &mut alerts.operational,
      format!(
        "Operational alerts could not be persisted across restart: {error:#}"
      ),
    );
  }
}

fn append_operational_alert(
  messages: &mut Vec<String>,
  message: String,
) {
  if !messages.contains(&message) {
    messages.push(message);
  }
}

fn read_operational_alert(
  path: &Path,
) -> anyhow::Result<Vec<String>> {
  let bytes = match std::fs::read(path) {
    Ok(bytes) => bytes,
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
      return Ok(Vec::new());
    }
    Err(error) => return Err(error.into()),
  };
  let mut alerts: PersistedOperationalAlerts =
    serde_json::from_slice(&bytes)?;
  if let Some(message) = alerts.message {
    append_operational_alert(&mut alerts.messages, message);
  }
  Ok(alerts.messages)
}

fn persist_operational_alert(
  path: &Path,
  messages: &[String],
) -> anyhow::Result<()> {
  let parent =
    path.parent().context("Operational alert has no parent")?;
  std::fs::create_dir_all(parent)?;
  let temporary = parent.join(format!(
    ".backup-operational-alert-{}.tmp",
    Uuid::new_v4().simple()
  ));
  let mut file = OpenOptions::new()
    .create_new(true)
    .write(true)
    .mode(0o600)
    .open(&temporary)?;
  file.write_all(&serde_json::to_vec(
    &PersistedOperationalAlerts {
      messages: messages.to_vec(),
      message: None,
    },
  )?)?;
  file.sync_all()?;
  std::fs::rename(temporary, path)?;
  std::fs::File::open(parent)?.sync_all()?;
  Ok(())
}

const CONFIGURATION_ALERT_PREFIX: &str =
  "Backup configuration unavailable:";

pub fn record_configuration_alert(error: &anyhow::Error) {
  let message = format!("{CONFIGURATION_ALERT_PREFIX} {error:#}");
  error!("{message}");
  critical_alerts().write().unwrap().configuration = Some(message);
}

fn clear_configuration_alert() {
  critical_alerts().write().unwrap().configuration = None;
}

const MAINTENANCE_ALERT_PREFIX: &str = "Backup maintenance blocked:";

fn clear_maintenance_alert() {
  critical_alerts().write().unwrap().maintenance = None;
}

fn record_maintenance_alert(error: &anyhow::Error) {
  critical_alerts().write().unwrap().maintenance =
    Some(format!("{MAINTENANCE_ALERT_PREFIX} {error:#}"));
}

/// Serializes resource mutations with backup discovery/quiescing, restore
/// publication, and the short Core database-export window.
pub fn mutation_barrier() -> &'static Arc<tokio::sync::RwLock<()>> {
  static BARRIER: OnceLock<Arc<tokio::sync::RwLock<()>>> =
    OnceLock::new();
  BARRIER.get_or_init(|| Arc::new(tokio::sync::RwLock::new(())))
}

/// Keeps a primary/mirror role change from racing repository operations while
/// still allowing normal fleet-wide repository concurrency.
fn repository_role_barrier() -> &'static Arc<tokio::sync::RwLock<()>>
{
  static BARRIER: OnceLock<Arc<tokio::sync::RwLock<()>>> =
    OnceLock::new();
  BARRIER.get_or_init(|| Arc::new(tokio::sync::RwLock::new(())))
}

fn backup_operation_lock() -> &'static tokio::sync::Mutex<()> {
  static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
  LOCK.get_or_init(Default::default)
}

fn schedule_core_restart() {
  tokio::spawn(async {
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    std::process::exit(75);
  });
}

fn cancellation_tokens()
-> &'static Mutex<HashMap<String, Arc<AtomicBool>>> {
  static TOKENS: OnceLock<Mutex<HashMap<String, Arc<AtomicBool>>>> =
    OnceLock::new();
  TOKENS.get_or_init(Default::default)
}

fn non_cancellable_runs() -> &'static Mutex<HashSet<String>> {
  static RUNS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
  RUNS.get_or_init(Default::default)
}

fn register_cancellation_token(run_id: &str) -> Arc<AtomicBool> {
  let token = Arc::new(AtomicBool::new(false));
  cancellation_tokens()
    .lock()
    .unwrap()
    .insert(run_id.to_string(), token.clone());
  token
}

fn cancellation_token(run_id: &str) -> Option<Arc<AtomicBool>> {
  cancellation_tokens().lock().unwrap().get(run_id).cloned()
}

fn cancellation_requested(run_id: &str) -> bool {
  cancellation_token(run_id)
    .is_some_and(|token| token.load(Ordering::SeqCst))
}

fn ensure_not_cancelled(run_id: &str) -> anyhow::Result<()> {
  if cancellation_requested(run_id) {
    Err(anyhow!("Backup run was cancelled"))
  } else {
    Ok(())
  }
}

fn core_recovery_operation_lock() -> &'static tokio::sync::Mutex<()> {
  static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
  LOCK.get_or_init(Default::default)
}

pub async fn list_snapshots()
-> anyhow::Result<(Vec<BackupSnapshot>, u64)> {
  let settings = get_settings().await?;
  tokio::task::spawn_blocking(move || {
    let inventory = core_repository(&settings.primary, &settings)?
      .list_snapshots()?;
    let snapshots = inventory
      .snapshots
      .into_iter()
      .map(|mut snapshot| {
        snapshot.target = authenticated_snapshot_target(&snapshot);
        if snapshot.target == BackupTarget::Core
          && let Ok((_, _, created_at)) =
            crypto::authenticate_core_source_label(
              &snapshot.source_label,
              &snapshot.hostname,
              &snapshot.name,
            )
        {
          // A repository replay must not present an old export as a new one.
          snapshot.created_at = created_at;
        }
        snapshot
      })
      .collect();
    Ok((snapshots, inventory.hidden))
  })
  .await
  .context("Vykar inventory worker failed")?
}

fn authorized_source_label(
  target: &BackupTarget,
  hostname: &str,
  snapshot_name: &str,
) -> anyhow::Result<String> {
  let raw = target.source_label(core_instance_id()?);
  crypto::authorize_source_label(&raw, hostname, snapshot_name)
}

fn authenticated_snapshot_target(
  snapshot: &BackupSnapshot,
) -> BackupTarget {
  authenticated_snapshot_source(snapshot)
    .map(|(target, _)| target)
    .unwrap_or_else(|| BackupTarget::Unbound {
      source_label: snapshot.source_label.clone(),
    })
}

fn authenticated_snapshot_source(
  snapshot: &BackupSnapshot,
) -> Option<(BackupTarget, String)> {
  let authenticated = crypto::authenticate_source_label(
    &snapshot.source_label,
    &snapshot.hostname,
    &snapshot.name,
  )
  .ok()
  .map(|raw| (parse_source_label(&raw), raw));
  let valid =
    authenticated.as_ref().is_some_and(
      |(target, raw)| match target {
        BackupTarget::Core => {
          crypto::authenticate_core_source_label(
            &snapshot.source_label,
            &snapshot.hostname,
            &snapshot.name,
          )
          .is_ok()
            && raw
              .strip_prefix("komodo/v1/core/")
              .zip(snapshot.hostname.strip_prefix("komodo-core-"))
              .is_some_and(|(identity, writer)| identity == writer)
        }
        BackupTarget::Stack { .. } => {
          snapshot.hostname.starts_with(PERIPHERY_HOSTNAME_PREFIX)
        }
        BackupTarget::Volume { server_id, .. } => {
          snapshot.hostname.strip_prefix(PERIPHERY_HOSTNAME_PREFIX)
            == Some(server_id.as_str())
        }
        BackupTarget::Unbound { .. } => false,
      },
    );
  valid.then(|| authenticated.unwrap())
}

fn authenticated_retention_deletions(
  snapshots: &[BackupSnapshot],
  settings: &BackupSettings,
) -> Vec<String> {
  let mut by_source: HashMap<
    String,
    (u64, Vec<(&BackupSnapshot, i64)>),
  > = HashMap::new();
  for snapshot in snapshots {
    let Some((target, raw_source)) =
      authenticated_snapshot_source(snapshot)
    else {
      continue;
    };
    let keep = match target {
      BackupTarget::Core => settings.core_keep_last,
      BackupTarget::Stack { .. } => settings.stack_keep_last,
      BackupTarget::Volume { .. } => settings.volume_keep_last,
      BackupTarget::Unbound { .. } => continue,
    };
    let created_at = if target == BackupTarget::Core {
      let Ok((_, _, created_at)) =
        crypto::authenticate_core_source_label(
          &snapshot.source_label,
          &snapshot.hostname,
          &snapshot.name,
        )
      else {
        continue;
      };
      created_at
    } else {
      snapshot.created_at
    };
    let entry =
      by_source.entry(raw_source).or_insert((keep, Vec::new()));
    entry.1.push((snapshot, created_at));
  }
  let mut delete = Vec::new();
  for (_, (keep_last, snapshots)) in by_source {
    delete.extend(retention_deletions_by_creation_time(
      snapshots, keep_last,
    ));
  }
  delete
}

fn retention_deletions_by_creation_time(
  mut snapshots: Vec<(&BackupSnapshot, i64)>,
  keep_last: u64,
) -> Vec<String> {
  snapshots
    .sort_by_key(|(_, created_at)| std::cmp::Reverse(*created_at));
  let mut delete = Vec::new();
  let mut complete_seen = 0_u64;
  let mut partial_seen = 0_u64;
  for (snapshot, _) in snapshots {
    let keep = if snapshot.partial {
      partial_seen += 1;
      partial_seen == 1
    } else {
      complete_seen += 1;
      complete_seen <= keep_last.max(1)
    };
    if !keep {
      delete.push(snapshot.name.clone());
    }
  }
  delete
}

pub async fn list_directory(
  snapshot: String,
  parent: String,
  search: String,
  page: u64,
  limit: u64,
) -> anyhow::Result<SnapshotDirectoryPage> {
  let permit = snapshot_tree_slots().clone().try_acquire_owned()
    .context("Another snapshot tree request is still running; retry after it finishes")?;
  let deadline =
    std::time::Instant::now() + std::time::Duration::from_secs(60);
  let settings = get_settings().await?;
  run_snapshot_tree_worker(permit, deadline, move || {
    core_repository(&settings.primary, &settings)?.list_directory(
      &snapshot, &parent, &search, page, limit, deadline,
    )
  })
  .await
}

async fn run_snapshot_tree_worker<T: Send + 'static>(
  permit: tokio::sync::OwnedSemaphorePermit,
  deadline: std::time::Instant,
  work: impl FnOnce() -> anyhow::Result<T> + Send + 'static,
) -> anyhow::Result<T> {
  let worker = tokio::task::spawn_blocking(move || {
    // An expired/disconnected request cannot release the slot while its
    // blocking backend read is still running.
    let _permit = permit;
    work()
  });
  tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), worker)
  .await
  .context("Snapshot tree request exceeded 60 seconds; its worker may still be finishing")?
  .context("Vykar tree worker failed")?
}

fn snapshot_tree_slots() -> &'static Arc<tokio::sync::Semaphore> {
  static SLOTS: OnceLock<Arc<tokio::sync::Semaphore>> =
    OnceLock::new();
  SLOTS.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(1)))
}

pub async fn run_backup(
  target: Option<BackupTarget>,
) -> anyhow::Result<BackupRun> {
  let _operation = backup_operation_lock().lock().await;
  run_backup_locked(target).await
}

async fn run_backup_locked(
  target: Option<BackupTarget>,
) -> anyhow::Result<BackupRun> {
  let _repository_roles =
    repository_role_barrier().clone().read_owned().await;
  let mut run = new_run(target.clone(), "Backup running").await?;
  let run_id = run.id.clone();
  let settings = match get_settings().await {
    Ok(settings) => settings,
    Err(error) => {
      let message = format!("{error:#}");
      let _ = finish_run(run, BackupRunState::Failed, message).await;
      cancellation_tokens().lock().unwrap().remove(&run_id);
      return Err(error);
    }
  };
  let result = match target {
    Some(target) => run_target(&settings, &run, target)
      .await
      .map(|partial| (partial, Vec::new())),
    None => run_fleet(&settings, &run)
      .await
      .map(|outcome| (outcome.partial, outcome.retries)),
  };
  let finished = match result {
    Ok((_, _)) if cancellation_requested(&run_id) => {
      finish_run(
        run,
        BackupRunState::Cancelled,
        "Cancellation requested",
      )
      .await
    }
    Ok((_, retries)) if !retries.is_empty() => {
      run.message =
        "Initial fleet pass was partial; retries are active".into();
      let _ = runs_collection()
        .update_one(
          doc! { "id": &run.id },
          doc! { "$set": { "message": &run.message } },
        )
        .await;
      spawn_fleet_retry_finalizer(run.clone(), retries);
      return Ok(run);
    }
    Ok((true, _)) => {
      finish_run(
        run,
        BackupRunState::Partial,
        "Backup completed partially",
      )
      .await
    }
    Ok((false, _)) => {
      finish_run(run, BackupRunState::Complete, "Backup complete")
        .await
    }
    Err(_) if cancellation_requested(&run_id) => {
      finish_run(
        run,
        BackupRunState::Cancelled,
        "Cancellation requested",
      )
      .await
    }
    Err(error) => {
      finish_run(run, BackupRunState::Failed, format!("{error:#}"))
        .await
    }
  };
  cancellation_tokens().lock().unwrap().remove(&run_id);
  let finished = finished?;
  if matches!(
    finished.state,
    BackupRunState::Complete | BackupRunState::Partial
  ) {
    queue_maintenance();
  }
  Ok(finished)
}

async fn run_scheduled_backup() -> anyhow::Result<Option<BackupRun>> {
  let Ok(_operation) = backup_operation_lock().try_lock() else {
    tracing::info!(
      "Skipping scheduled fleet backup because another backup operation is active"
    );
    return Ok(None);
  };
  let active = runs_collection()
    .find_one(doc! {
      "state": {
        "$in": [
          to_bson(&BackupRunState::Queued)?,
          to_bson(&BackupRunState::Running)?,
        ]
      }
    })
    .await?;
  if let Some(active) = active {
    tracing::info!(
      run_id = active.id,
      "Skipping scheduled fleet backup because an earlier run is still active"
    );
    return Ok(None);
  }
  run_backup_locked(None).await.map(Some)
}

fn spawn_fleet_retry_finalizer(
  run: BackupRun,
  retries: Vec<tokio::task::JoinHandle<bool>>,
) {
  tokio::spawn(async move {
    let run_id = run.id.clone();
    let retry_results = futures_util::future::join_all(retries).await;
    let all_complete = retry_results
      .iter()
      .all(|result| matches!(result, Ok(true)));
    let current = runs_collection()
      .find_one(doc! { "id": &run_id })
      .await
      .ok()
      .flatten()
      .unwrap_or(run);
    let (state, message) = fleet_retry_completion(
      cancellation_requested(&run_id),
      all_complete,
    );
    match finish_run(current, state, message).await {
      Ok(finished)
        if fleet_retry_requires_maintenance(&finished.state) =>
      {
        queue_maintenance()
      }
      Ok(_) => {}
      Err(error) => {
        error!(
          "Failed to finalize fleet backup retry run {run_id}: {error:#}"
        );
      }
    }
    cancellation_tokens().lock().unwrap().remove(&run_id);
  });
}

fn fleet_retry_completion(
  cancelled: bool,
  all_complete: bool,
) -> (BackupRunState, &'static str) {
  if cancelled {
    (BackupRunState::Cancelled, "Cancellation requested")
  } else if all_complete {
    (BackupRunState::Complete, "Backup retries completed")
  } else {
    (
      BackupRunState::Partial,
      "Backup retries stopped before every target completed",
    )
  }
}

fn fleet_retry_requires_maintenance(state: &BackupRunState) -> bool {
  matches!(state, BackupRunState::Complete | BackupRunState::Partial)
}

fn maintenance_sender() -> &'static tokio::sync::mpsc::Sender<()> {
  static SENDER: OnceLock<tokio::sync::mpsc::Sender<()>> =
    OnceLock::new();
  SENDER.get_or_init(|| {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    tokio::spawn(async move {
      while receiver.recv().await.is_some() {
        // Manual backups close together share one prune/check/compact cycle.
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        while receiver.try_recv().is_ok() {}
        match run_maintenance().await {
          Ok(()) => clear_maintenance_alert(),
          Err(error) => {
            error!("Backup repository maintenance failed: {error:#}");
            record_maintenance_alert(&error);
          }
        }
      }
    });
    sender
  })
}

fn queue_maintenance() {
  let _ = maintenance_sender().try_send(());
}

async fn record_repository_verification(
  health_id: &str,
  succeeded: bool,
  full: bool,
) -> anyhow::Result<()> {
  let now = komodo_timestamp();
  let update = if succeeded && full {
    doc! { "$set": {
      "healthy": true,
      "checked_at": now,
      "last_full_verification_at": now,
      "verification_failed": false,
    } }
  } else if succeeded {
    // A sample that happens not to encounter previously-recorded corruption
    // cannot prove that corruption is gone. Only a successful full check may
    // clear the failure latch.
    doc! {
      "$set": { "checked_at": now },
      "$setOnInsert": {
        "healthy": true,
        "verification_failed": false,
      }
    }
  } else {
    doc! { "$set": {
      "healthy": succeeded,
      "checked_at": now,
      "verification_failed": !succeeded,
    } }
  };
  health_collection()
    .update_one(doc! { "_id": health_id }, update)
    .with_options(UpdateOptions::builder().upsert(true).build())
    .await?;
  Ok(())
}

async fn run_maintenance() -> anyhow::Result<()> {
  let _operation = backup_operation_lock().lock().await;
  let _repository_roles =
    repository_role_barrier().clone().read_owned().await;
  // A settings save can finish while maintenance waits for an active backup.
  // Read policy only after both guards and retain the role guard through deletion.
  let settings = get_settings().await?;
  let repositories =
    std::iter::once(("primary", settings.primary.clone()))
      .chain(settings.mirror.clone().map(|mirror| ("mirror", mirror)))
      .collect::<Vec<_>>();
  for (health_id, repository) in repositories {
    let previous = health_collection()
      .find_one(doc! { "_id": health_id })
      .await?
      .unwrap_or_default();
    let full_due = full_verification_due(
      &previous,
      komodo_timestamp(),
      settings.advanced.full_verify_every_days,
    );
    let verification_repository = repository.clone();
    let verification_settings = settings.clone();
    let verification = tokio::task::spawn_blocking(move || {
      core_repository(
        &verification_repository,
        &verification_settings,
      )?
      .verify(
        full_due,
        verification_settings.advanced.verify_sample_percent,
      )
    })
    .await
    .context("Vykar verification worker failed")
    .and_then(|result| result);
    let verification = match verification {
      Ok(verification) => verification,
      Err(error) => {
        let _ =
          record_repository_verification(health_id, false, full_due)
            .await;
        return Err(error);
      }
    };
    if !verification.errors.is_empty() {
      record_repository_verification(health_id, false, full_due)
        .await?;
      return Err(anyhow!(
        "Integrity sampling found errors; prune and compaction were not started: {}",
        verification.errors.join("; ")
      ));
    }
    record_repository_verification(health_id, true, full_due).await?;
    let settings_for_worker = settings.clone();
    let maintenance = tokio::task::spawn_blocking(
      move || -> anyhow::Result<()> {
      let vykar = core_repository(&repository, &settings_for_worker)?;
      let inventory = vykar.list_snapshots()?;
      if inventory.hidden > 0 {
        return Err(anyhow!(
          "Vykar hid {} unreadable snapshot(s); destructive maintenance is blocked",
          inventory.hidden
        ));
      }
      let deletions = authenticated_retention_deletions(
        &inventory.snapshots,
        &settings_for_worker,
      );
      vykar.delete_snapshots_if_present(&deletions)?;
      // Retry reconciliation can delete partial/superseded snapshots before
      // retention runs. Always give threshold-based compaction a chance to
      // reclaim those dead packs even when retention deleted nothing new.
      let max_repack = match repository.backend {
        BackupRepositoryBackend::S3 { .. }
        | BackupRepositoryBackend::Sftp { .. } => {
            Some(settings_for_worker.advanced.client_repack_limit_bytes)
        }
        BackupRepositoryBackend::CoreLocal { .. }
        | BackupRepositoryBackend::Rest { .. } => None,
      };
      vykar.compact(
        settings_for_worker.advanced.compact_threshold_percent,
        max_repack,
      )?;
      Ok(())
    },
    )
    .await
    .context("Vykar maintenance worker failed")
    .and_then(|result| result);
    if let Err(error) = maintenance {
      // Any failure after verification makes the preceding evidence unsafe to
      // reuse. Force a full verification before another destructive cycle.
      record_repository_verification(health_id, false, false).await?;
      return Err(error);
    }
  }
  Ok(())
}

fn full_verification_due(
  previous: &RepositoryHealthRecord,
  now: i64,
  every_days: u64,
) -> bool {
  previous.verification_failed
    || previous.last_full_verification_at == 0
    || now.saturating_sub(previous.last_full_verification_at)
      >= every_days.max(1) as i64 * 24 * 60 * 60 * 1000
}

struct FleetRunOutcome {
  partial: bool,
  retries: Vec<tokio::task::JoinHandle<bool>>,
}

fn volume_is_backup_eligible(
  volume: &VolumeListItem,
  include_anonymous_volumes: bool,
) -> bool {
  volume.driver == "local"
    && volume.scope == VolumeScopeEnum::Local
    && (include_anonymous_volumes || !volume.anonymous)
}

async fn run_fleet(
  settings: &BackupSettings,
  run: &BackupRun,
) -> anyhow::Result<FleetRunOutcome> {
  ensure_not_cancelled(&run.id)?;
  *fleet_generation().write().unwrap() = run.id.clone();
  let mut retries = Vec::new();
  let mut targets = Vec::new();
  let mut discovery_retry_servers = HashSet::new();
  let mut partial = false;
  if settings.core_enabled {
    match backup_core(settings, run).await {
      Ok(false) => {}
      Ok(true) => {
        partial = true;
        retries.push(spawn_core_retry(settings.clone(), run.clone()));
      }
      Err(error) => {
        warn!("Core backup failed: {error:#}");
        partial = true;
        retries.push(spawn_core_retry(settings.clone(), run.clone()));
      }
    }
  }
  ensure_not_cancelled(&run.id)?;

  // Keep resource mutations out from Periphery discovery through container
  // quiescing and snapshot commit. Core export acquires this barrier only for
  // its immutable database dump above, so the lock order remains backup
  // operation -> repository role -> mutation barrier.
  let _mutation = mutation_barrier().write().await;
  let servers = if settings.stacks_enabled || settings.volumes_enabled
  {
    find_collect(&db_client().servers, None, None).await?
  } else {
    Vec::new()
  };
  let enabled_server_ids = servers
    .iter()
    .filter(|server| server.config.enabled)
    .map(|server| server.id.clone())
    .collect::<HashSet<_>>();
  if settings.stacks_enabled {
    let mut matched_included_stacks = HashSet::new();
    let stacks =
      find_collect(&db_client().stacks, None, None).await?;
    for stack in stacks {
      if !selection_includes(
        settings.stack_selection.mode,
        &settings.stack_selection.stack_ids,
        &stack.id,
      ) {
        continue;
      }
      if settings.stack_selection.mode == BackupSelectionMode::Include
      {
        matched_included_stacks.insert(stack.id.clone());
      }
      if !stack.config.swarm_id.is_empty() {
        // Selecting one unsupported Stack explicitly must be observable. A
        // silent filter makes an Include run report Complete without ever
        // attempting the resource the operator named.
        if settings.stack_selection.mode
          == BackupSelectionMode::Include
          && settings.stack_selection.stack_ids.contains(&stack.id)
        {
          warn!(
            "Explicitly selected Swarm Stack '{}' is unsupported by backup v1",
            stack.name
          );
          partial = true;
        }
        continue;
      }
      if enabled_server_ids.contains(stack.config.server_id.as_str())
      {
        targets.push(BackupTarget::Stack { stack_id: stack.id });
      } else if settings.stack_selection.mode
        == BackupSelectionMode::Include
        && settings.stack_selection.stack_ids.contains(&stack.id)
      {
        warn!(
          "Explicitly selected Stack '{}' belongs to a missing or disabled Server",
          stack.name
        );
        partial = true;
      }
    }
    if settings.stack_selection.mode == BackupSelectionMode::Include {
      for selected in &settings.stack_selection.stack_ids {
        if matched_included_stacks.contains(selected) {
          continue;
        }
        warn!(
          "Explicitly selected Stack '{selected}' no longer exists"
        );
        partial = true;
      }
    }
  }
  if settings.volumes_enabled {
    let mut matched_included_volumes = HashSet::new();
    // Volume inventory comes from every configured Periphery at run time, so
    // unmanaged local named volumes participate automatically.
    for server in servers {
      if !server.config.enabled {
        continue;
      }
      let client = match periphery_client(&server).await {
        Ok(client) => client,
        Err(error) => {
          warn!(
            "Backup discovery could not connect to {}: {error:#}",
            server.name
          );
          partial = true;
          discovery_retry_servers.insert(server.id.clone());
          continue;
        }
      };
      let poll = client
        .request(periphery_client::api::poll::PollStatus {
          include_stats: false,
          include_docker: true,
        })
        .await;
      let Ok(poll) = poll else {
        partial = true;
        discovery_retry_servers.insert(server.id.clone());
        targets.extend(
          settings
            .volume_selection
            .volumes
            .iter()
            .filter(|volume| volume.server_id == server.id)
            .cloned()
            .map(
              |BackupVolumeTarget {
                 server_id,
                 volume_name,
               }| BackupTarget::Volume {
                server_id,
                volume_name,
              },
            ),
        );
        continue;
      };
      for volume in poll
        .docker
        .into_iter()
        .flat_map(|docker| docker.volumes)
        .filter(|volume| {
          volume_is_backup_eligible(
            volume,
            settings.include_anonymous_volumes,
          )
        })
      {
        let identity = BackupVolumeTarget {
          server_id: server.id.clone(),
          volume_name: volume.name,
        };
        if selection_includes(
          settings.volume_selection.mode,
          &settings.volume_selection.volumes,
          &identity,
        ) {
          if settings.volume_selection.mode
            == BackupSelectionMode::Include
          {
            matched_included_volumes.insert(identity.clone());
          }
          targets.push(BackupTarget::Volume {
            server_id: identity.server_id,
            volume_name: identity.volume_name,
          });
        }
      }
    }
    if settings.volume_selection.mode == BackupSelectionMode::Include
    {
      for selected in &settings.volume_selection.volumes {
        if matched_included_volumes.contains(selected)
          || discovery_retry_servers.contains(&selected.server_id)
        {
          continue;
        }
        warn!(
          "Explicitly selected Volume '{}/{}' is missing, unsupported, or belongs to a missing or disabled Server",
          selected.server_id, selected.volume_name
        );
        partial = true;
      }
    }
  }
  let mut by_server: HashMap<String, Vec<BackupTarget>> =
    HashMap::new();
  for target in targets {
    let server_id = match &target {
      BackupTarget::Stack { stack_id } => {
        let Some(stack) =
          Stack::coll().find_one(id_or_name_filter(stack_id)).await?
        else {
          warn!(
            "Skipping Stack {stack_id} because it was deleted during backup discovery"
          );
          continue;
        };
        stack.config.server_id
      }
      BackupTarget::Volume { server_id, .. } => server_id.clone(),
      BackupTarget::Core | BackupTarget::Unbound { .. } => continue,
    };
    by_server.entry(server_id).or_default().push(target);
  }
  for server_id in discovery_retry_servers {
    by_server.entry(server_id).or_default();
  }
  let semaphore = Arc::new(tokio::sync::Semaphore::new(
    settings.advanced.node_concurrency.max(1) as usize,
  ));
  let mut batches = FuturesUnordered::new();
  for (server_id, targets) in by_server {
    let semaphore = semaphore.clone();
    let settings = settings.clone();
    let run = run.clone();
    batches.push(async move {
      ensure_not_cancelled(&run.id)?;
      let _permit = semaphore.acquire_owned().await?;
      ensure_not_cancelled(&run.id)?;
      let refreshed =
        refresh_node_targets(&settings, &server_id, targets.clone())
          .await;
      let (targets, tasks, refresh_targets, result) = match refreshed {
        Ok(targets) => {
          match build_node_backup_tasks(
            &targets,
            &run.id,
            &server_id,
          )
          .await
          {
            Ok(prepared) => {
              for error in &prepared.errors {
                warn!(
                  "Backup could not prepare a target on node {server_id}: {error}"
                );
              }
              let tasks = prepared.tasks;
              let result = if tasks.is_empty() {
                Ok(NodeBatchOutcome {
                  partial: !prepared.failed_targets.is_empty(),
                  retry_tasks: Vec::new(),
                  retry_blocked: false,
                })
              } else {
                run_node_batch(
                  &settings,
                  &run,
                  &server_id,
                  tasks.clone(),
                )
                .await
              };
              (prepared.failed_targets, tasks, false, result)
            }
            Err(error) => (targets, Vec::new(), false, Err(error)),
          }
        }
        Err(error) => (targets, Vec::new(), true, Err(error)),
      };
      anyhow::Ok((
        server_id,
        targets,
        tasks,
        refresh_targets,
        result,
      ))
    });
  }
  while let Some(result) = batches.next().await {
    ensure_not_cancelled(&run.id)?;
    let (server_id, targets, tasks, refresh_targets, result) =
      result?;
    match result {
      Ok(outcome) => {
        partial |= outcome.partial || !targets.is_empty();
        if !outcome.retry_blocked
          && (!outcome.retry_tasks.is_empty() || !targets.is_empty())
        {
          retries.push(spawn_node_retry(
            settings.clone(),
            run.clone(),
            server_id,
            targets,
            outcome.retry_tasks,
            false,
          ));
        }
      }
      Err(error) => {
        warn!("Backup node {server_id} failed: {error:#}");
        partial = true;
        retries.push(spawn_node_retry(
          settings.clone(),
          run.clone(),
          server_id,
          targets,
          tasks,
          refresh_targets,
        ));
      }
    }
  }
  Ok(FleetRunOutcome { partial, retries })
}

fn fleet_generation() -> &'static RwLock<String> {
  static GENERATION: OnceLock<RwLock<String>> = OnceLock::new();
  GENERATION.get_or_init(Default::default)
}

fn fleet_retry_delay_seconds(completed_attempts: u32) -> Option<u64> {
  (completed_attempts < MAX_FLEET_RETRY_ATTEMPTS)
    .then(|| 2_u64.saturating_pow(completed_attempts.min(8)).min(300))
}

fn spawn_node_retry(
  settings: BackupSettings,
  run: BackupRun,
  server_id: String,
  mut targets: Vec<BackupTarget>,
  mut tasks: Vec<VykarBackupTask>,
  mut refresh_targets: bool,
) -> tokio::task::JoinHandle<bool> {
  tokio::spawn(async move {
    let mut retry = 0_u32;
    loop {
      if *fleet_generation().read().unwrap() != run.id {
        return false;
      }
      let Some(seconds) = fleet_retry_delay_seconds(retry) else {
        warn!(
          "Backup retries for node {server_id} exhausted after {retry} attempts"
        );
        return false;
      };
      tokio::time::sleep(std::time::Duration::from_secs(seconds))
        .await;
      if *fleet_generation().read().unwrap() != run.id {
        return false;
      }
      retry += 1;
      let _ = runs_collection()
        .update_one(
          doc! { "id": &run.id },
          doc! { "$set": { "retry_count": retry as i64 } },
        )
        .await;
      let _operation = backup_operation_lock().lock().await;
      if *fleet_generation().read().unwrap() != run.id {
        return false;
      }
      let _repository_roles =
        repository_role_barrier().clone().read_owned().await;
      if *fleet_generation().read().unwrap() != run.id {
        return false;
      }
      let _mutation = mutation_barrier().write().await;
      if *fleet_generation().read().unwrap() != run.id {
        return false;
      }
      if tasks.is_empty() {
        let refreshed = if refresh_targets {
          match refresh_node_targets(
            &settings,
            &server_id,
            targets.clone(),
          )
          .await
          {
            Ok(targets) => {
              refresh_targets = false;
              targets
            }
            Err(error) => {
              warn!(
                "Backup retry {retry} could not rediscover node {server_id}: {error:#}"
              );
              continue;
            }
          }
        } else {
          targets.clone()
        };
        let prepared = match build_node_backup_tasks(
          &refreshed, &run.id, &server_id,
        )
        .await
        {
          Ok(prepared) => prepared,
          Err(error) => {
            warn!(
              "Backup retry {retry} could not prepare node {server_id}: {error:#}"
            );
            continue;
          }
        };
        for error in &prepared.errors {
          warn!(
            "Backup retry {retry} could not prepare a target on node {server_id}: {error}"
          );
        }
        targets = prepared.failed_targets;
        tasks = prepared.tasks;
        if tasks.is_empty() {
          if targets.is_empty() {
            return true;
          }
          continue;
        }
      }
      if *fleet_generation().read().unwrap() != run.id {
        return false;
      }
      let refreshed = match refresh_retry_task_groups(
        &tasks, &server_id,
      )
      .await
      {
        Ok(refreshed) => refreshed,
        Err(error) => {
          warn!(
            "Backup retry {retry} could not refresh current Stack configuration: {error:#}"
          );
          continue;
        }
      };
      let mut retry_tasks = Vec::new();
      let mut all_complete = !refreshed.blocked;
      for (current_server_id, current_tasks) in refreshed.groups {
        let attempted = current_tasks.clone();
        match run_node_batch(
          &settings,
          &run,
          &current_server_id,
          current_tasks,
        )
        .await
        {
          Ok(outcome) if outcome.retry_blocked => return false,
          Ok(outcome) => {
            all_complete &= !outcome.partial;
            retry_tasks.extend(outcome.retry_tasks);
          }
          Err(error) => {
            all_complete = false;
            retry_tasks.extend(attempted);
            warn!(
              "Backup retry {retry} for node {current_server_id} failed: {error:#}"
            );
          }
        }
      }
      tasks = retry_tasks;
      if tasks.is_empty() && targets.is_empty() {
        return all_complete;
      }
      warn!("Backup retry {retry} remained partial");
    }
  })
}

struct RefreshedRetryTaskGroups {
  groups: HashMap<String, Vec<VykarBackupTask>>,
  blocked: bool,
}

/// Refresh every serialized Stack target immediately before retry and group
/// the tasks by its current Server. Repository retry metadata stays attached
/// to the task, while paths, Repo configuration, and Server placement come
/// from the mutation-locked current resource state.
async fn refresh_retry_task_groups(
  tasks: &[VykarBackupTask],
  volume_server_id: &str,
) -> anyhow::Result<RefreshedRetryTaskGroups> {
  let mut groups: HashMap<String, Vec<VykarBackupTask>> =
    HashMap::new();
  let mut blocked = false;
  for mut task in tasks.iter().cloned() {
    let current_server_id = match &task.target {
      PeripheryBackupTarget::Stack { stack, .. } => {
        let Some(current) = Stack::coll()
          .find_one(id_or_name_filter(&stack.id))
          .await?
        else {
          // A deleted Stack is no longer part of the fleet selection.
          continue;
        };
        if !current.config.swarm_id.is_empty() {
          blocked = true;
          continue;
        }
        let repo = if current.config.linked_repo.is_empty() {
          None
        } else {
          Some(
            resource::get::<Repo>(&current.config.linked_repo)
              .await?,
          )
        };
        let current_server_id = current.config.server_id.clone();
        task.target = PeripheryBackupTarget::Stack {
          stack: Box::new(current),
          repo: repo.map(Box::new),
        };
        current_server_id
      }
      PeripheryBackupTarget::Volume { .. } => {
        volume_server_id.to_string()
      }
    };
    let server = Server::coll()
      .find_one(id_or_name_filter(&current_server_id))
      .await?;
    if !server.is_some_and(|server| server.config.enabled) {
      blocked = true;
      continue;
    }
    task.source_label = authorized_source_label(
      &match &task.target {
        PeripheryBackupTarget::Stack { stack, .. } => {
          BackupTarget::Stack {
            stack_id: stack.id.clone(),
          }
        }
        PeripheryBackupTarget::Volume { volume_name } => {
          BackupTarget::Volume {
            server_id: current_server_id.clone(),
            volume_name: volume_name.clone(),
          }
        }
      },
      &format!("{PERIPHERY_HOSTNAME_PREFIX}{current_server_id}"),
      &task.snapshot_name,
    )?;
    groups.entry(current_server_id).or_default().push(task);
  }
  Ok(RefreshedRetryTaskGroups { groups, blocked })
}

fn spawn_core_retry(
  settings: BackupSettings,
  run: BackupRun,
) -> tokio::task::JoinHandle<bool> {
  tokio::spawn(async move {
    let mut retry = 0_u32;
    loop {
      if *fleet_generation().read().unwrap() != run.id {
        discard_core_repository_retry(&run.id).await;
        return false;
      }
      let Some(seconds) = fleet_retry_delay_seconds(retry) else {
        warn!("Core backup retries exhausted after {retry} attempts");
        discard_core_repository_retry(&run.id).await;
        return false;
      };
      tokio::time::sleep(std::time::Duration::from_secs(seconds))
        .await;
      if *fleet_generation().read().unwrap() != run.id {
        discard_core_repository_retry(&run.id).await;
        return false;
      }
      retry = retry.saturating_add(1);
      let _operation = backup_operation_lock().lock().await;
      if *fleet_generation().read().unwrap() != run.id {
        discard_core_repository_retry(&run.id).await;
        return false;
      }
      let _repository_roles =
        repository_role_barrier().clone().read_owned().await;
      if *fleet_generation().read().unwrap() != run.id {
        discard_core_repository_retry(&run.id).await;
        return false;
      }
      match backup_core(&settings, &run).await {
        Ok(false) => {
          return true;
        }
        Ok(true) => {
          warn!("Core backup retry {retry} remained partial")
        }
        Err(error) => {
          warn!("Core backup retry {retry} failed: {error:#}")
        }
      }
    }
  })
}

async fn discard_core_repository_retry(run_id: &str) {
  let retry =
    core_repository_retries().lock().unwrap().remove(run_id);
  if let Some(retry) = retry {
    let _ = tokio::fs::remove_dir_all(retry.staging).await;
  }
}

async fn refresh_node_targets(
  settings: &BackupSettings,
  server_id: &str,
  targets: Vec<BackupTarget>,
) -> anyhow::Result<Vec<BackupTarget>> {
  let mut targets = targets
    .into_iter()
    .filter(|target| matches!(target, BackupTarget::Stack { .. }))
    .collect::<HashSet<_>>();
  if !settings.volumes_enabled {
    return Ok(targets.into_iter().collect());
  }
  let server = resource::get::<Server>(server_id).await?;
  let poll = periphery_client(&server)
    .await?
    .request(periphery_client::api::poll::PollStatus {
      include_stats: false,
      include_docker: true,
    })
    .await?;
  for volume in poll
    .docker
    .into_iter()
    .flat_map(|docker| docker.volumes)
    .filter(|volume| {
      volume_is_backup_eligible(
        volume,
        settings.include_anonymous_volumes,
      )
    })
  {
    let identity = BackupVolumeTarget {
      server_id: server_id.to_string(),
      volume_name: volume.name,
    };
    if selection_includes(
      settings.volume_selection.mode,
      &settings.volume_selection.volumes,
      &identity,
    ) {
      targets.insert(BackupTarget::Volume {
        server_id: identity.server_id,
        volume_name: identity.volume_name,
      });
    }
  }
  Ok(targets.into_iter().collect())
}

struct NodeTaskPreparation {
  tasks: Vec<VykarBackupTask>,
  failed_targets: Vec<BackupTarget>,
  errors: Vec<String>,
}

async fn build_node_backup_tasks(
  targets: &[BackupTarget],
  run_id: &str,
  server_id: &str,
) -> anyhow::Result<NodeTaskPreparation> {
  let mut tasks = Vec::new();
  let mut failed_targets = Vec::new();
  let mut errors = Vec::new();
  for target in targets {
    let raw_source_label = target.source_label(core_instance_id()?);
    let task = async {
      let periphery_target = match target {
        BackupTarget::Stack { stack_id } => {
          let Some(stack) = Stack::coll()
            .find_one(id_or_name_filter(stack_id))
            .await?
          else {
            // The selection was stale. A deleted Stack has nothing left to
            // discover or retry and must not fail the rest of the fleet.
            return Ok(None);
          };
          let repo = if stack.config.linked_repo.is_empty() {
            None
          } else {
            Some(
              resource::get::<Repo>(&stack.config.linked_repo)
                .await?,
            )
          };
          PeripheryBackupTarget::Stack {
            stack: Box::new(stack),
            repo: repo.map(Box::new),
          }
        }
        BackupTarget::Volume { volume_name, .. } => {
          PeripheryBackupTarget::Volume {
            volume_name: volume_name.clone(),
          }
        }
        BackupTarget::Core | BackupTarget::Unbound { .. } => {
          return Ok(None);
        }
      };
      let snapshot_name = snapshot_name(
        match target {
          BackupTarget::Stack { .. } => "stack",
          BackupTarget::Volume { .. } => "volume",
          _ => "backup",
        },
        run_id,
      );
      anyhow::Ok(Some(VykarBackupTask {
        target: periphery_target,
        source_label: authorized_source_label(
          target,
          &format!("{PERIPHERY_HOSTNAME_PREFIX}{server_id}"),
          &snapshot_name,
        )?,
        snapshot_name,
        mirror_only: false,
        primary_only: false,
        superseded_snapshot_names: Vec::new(),
        retained_snapshots: Vec::new(),
      }))
    }
    .await;
    match task {
      Ok(Some(task)) => tasks.push(task),
      Ok(None) => {}
      Err(error) => {
        failed_targets.push(target.clone());
        errors.push(format!("{raw_source_label}: {error:#}"));
      }
    }
  }
  Ok(NodeTaskPreparation {
    tasks,
    failed_targets,
    errors,
  })
}

struct NodeBatchOutcome {
  partial: bool,
  retry_tasks: Vec<VykarBackupTask>,
  retry_blocked: bool,
}

fn fresh_retry_snapshot_name(
  task: &VykarBackupTask,
  run_id: &str,
) -> String {
  snapshot_name(
    match &task.target {
      PeripheryBackupTarget::Stack { .. } => "stack",
      PeripheryBackupTarget::Volume { .. } => "volume",
    },
    run_id,
  )
}

fn authorize_retry_task(
  task: &mut VykarBackupTask,
  server_id: &str,
) -> anyhow::Result<()> {
  let target = match &task.target {
    PeripheryBackupTarget::Stack { stack, .. } => {
      BackupTarget::Stack {
        stack_id: stack.id.clone(),
      }
    }
    PeripheryBackupTarget::Volume { volume_name } => {
      BackupTarget::Volume {
        server_id: server_id.to_string(),
        volume_name: volume_name.clone(),
      }
    }
  };
  task.source_label = authorized_source_label(
    &target,
    &format!("{PERIPHERY_HOSTNAME_PREFIX}{server_id}"),
    &task.snapshot_name,
  )?;
  Ok(())
}

fn retry_tasks_after_unknown_result(
  tasks: Vec<VykarBackupTask>,
  run_id: &str,
  mirror_configured: bool,
) -> anyhow::Result<Vec<VykarBackupTask>> {
  tasks
    .into_iter()
    .map(|mut task| {
      migrate_legacy_retained_snapshots(&mut task, mirror_configured);
      task.retained_snapshots.push(VykarRetainedSnapshot {
        snapshot_name: task.snapshot_name.clone(),
        // A lost response is ambiguous. Preserve both possible copies until
        // a later authoritative success replaces each repository role.
        retain_primary: true,
        retain_mirror: mirror_configured,
      });
      task.mirror_only = false;
      task.primary_only = false;
      task.snapshot_name = fresh_retry_snapshot_name(&task, run_id);
      Ok(task)
    })
    .collect()
}

fn migrate_legacy_retained_snapshots(
  task: &mut VykarBackupTask,
  mirror_configured: bool,
) {
  task.retained_snapshots.extend(
    std::mem::take(&mut task.superseded_snapshot_names)
      .into_iter()
      .map(|snapshot_name| VykarRetainedSnapshot {
        snapshot_name,
        retain_primary: true,
        retain_mirror: mirror_configured,
      }),
  );
}

async fn delete_node_snapshot_copies(
  settings: &BackupSettings,
  snapshot_name: String,
  delete_primary: bool,
  delete_mirror: bool,
) {
  let mut repositories = Vec::new();
  if delete_primary {
    repositories.push(("primary", settings.primary.clone()));
  }
  if delete_mirror && let Some(mirror) = settings.mirror.clone() {
    repositories.push(("mirror", mirror));
  }
  let settings = settings.clone();
  let cleanup = tokio::task::spawn_blocking(move || {
    for (role, repository) in repositories {
      if let Err(error) = core_repository(&repository, &settings)
        .and_then(|repository| {
          repository.delete_snapshot_if_present(&snapshot_name)
        })
      {
        warn!(
          "Could not remove superseded {role} node snapshot {snapshot_name}: {error:#}"
        );
      }
    }
  })
  .await;
  if let Err(error) = cleanup {
    warn!("Node snapshot cleanup worker failed: {error}");
  }
}

async fn retire_retained_repository_copies(
  settings: &BackupSettings,
  retained: &mut Vec<VykarRetainedSnapshot>,
  primary: bool,
) {
  let snapshots = take_retained_repository_copies(retained, primary);
  for snapshot in snapshots {
    delete_node_snapshot_copies(
      settings, snapshot, primary, !primary,
    )
    .await;
  }
}

fn take_retained_repository_copies(
  retained: &mut Vec<VykarRetainedSnapshot>,
  primary: bool,
) -> Vec<String> {
  let mut snapshots = Vec::new();
  for snapshot in retained.iter_mut() {
    let retain = if primary {
      &mut snapshot.retain_primary
    } else {
      &mut snapshot.retain_mirror
    };
    if *retain {
      *retain = false;
      snapshots.push(snapshot.snapshot_name.clone());
    }
  }
  retained.retain(|snapshot| {
    snapshot.retain_primary || snapshot.retain_mirror
  });
  snapshots
}

async fn run_node_batch(
  settings: &BackupSettings,
  run: &BackupRun,
  server_id: &str,
  tasks: Vec<VykarBackupTask>,
) -> anyhow::Result<NodeBatchOutcome> {
  let server = resource::get::<Server>(server_id).await?;
  let expected = tasks.len();
  let response = periphery_client(&server)
    .await?
    .request(RunVykarBackupBatch {
      tasks: tasks.clone(),
      primary: repository_for_periphery(&settings.primary, false)?,
      mirror: settings
        .mirror
        .as_ref()
        .map(|repository| repository_for_periphery(repository, true))
        .transpose()?,
      advanced: settings.advanced.clone(),
      hostname: format!("komodo-periphery-{}", server.id),
      run_id: run.id.clone(),
      komodo_version: komodo_build_info::version().into(),
      protected_repository_paths: core_local_repository_paths(
        settings,
      )?,
      filters: backup_source_filters(settings),
      stop_containers: settings.stop_containers,
    })
    .await;
  let response = match response {
    Ok(response) => response,
    Err(error) => {
      warn!(
        "Backup node {} returned no authoritative result; the next attempt will use a fresh snapshot name: {error:#}",
        server.name
      );
      return Ok(NodeBatchOutcome {
        partial: true,
        retry_tasks: retry_tasks_after_unknown_result(
          tasks,
          &run.id,
          settings.mirror.is_some(),
        )?
        .into_iter()
        .map(|mut task| {
          authorize_retry_task(&mut task, server_id)?;
          Ok(task)
        })
        .collect::<anyhow::Result<Vec<_>>>()?,
        retry_blocked: false,
      });
    }
  };
  if !response.restart_errors.is_empty() {
    record_operational_alert(format!(
      "Backup restart failed on {}: {}",
      server.name,
      response.restart_errors.join("; ")
    ));
  }
  let result_count = response.results.len();
  let mut results = response
    .results
    .into_iter()
    .map(|result| (result.source_label, result.result))
    .collect::<HashMap<_, _>>();
  let mut retry_tasks = Vec::new();
  for mut task in tasks {
    let Some(result) = results.remove(&task.source_label) else {
      let mut unknown = retry_tasks_after_unknown_result(
        vec![task],
        &run.id,
        settings.mirror.is_some(),
      )?;
      for task in &mut unknown {
        authorize_retry_task(task, server_id)?;
      }
      retry_tasks.extend(unknown);
      continue;
    };
    let mirror_configured = settings.mirror.is_some();
    let primary_complete =
      result.primary.complete && result.primary.error.is_none();
    let mirror_complete = !mirror_configured
      || result.mirror.as_ref().is_some_and(|mirror| {
        mirror.complete && mirror.error.is_none()
      });
    migrate_legacy_retained_snapshots(&mut task, mirror_configured);
    if primary_complete {
      retire_retained_repository_copies(
        settings,
        &mut task.retained_snapshots,
        true,
      )
      .await;
    } else {
      delete_node_snapshot_copies(
        settings,
        task.snapshot_name.clone(),
        true,
        false,
      )
      .await;
    }
    if mirror_configured {
      if mirror_complete {
        retire_retained_repository_copies(
          settings,
          &mut task.retained_snapshots,
          false,
        )
        .await;
      } else {
        delete_node_snapshot_copies(
          settings,
          task.snapshot_name.clone(),
          false,
          true,
        )
        .await;
      }
    }
    if primary_complete || mirror_configured && mirror_complete {
      task.retained_snapshots.push(VykarRetainedSnapshot {
        snapshot_name: task.snapshot_name.clone(),
        retain_primary: primary_complete,
        retain_mirror: mirror_configured && mirror_complete,
      });
    }
    if !(primary_complete && mirror_complete) {
      // A repository-specific retry against rediscovered live paths could put
      // different bytes under one name. Every retry is therefore a fresh,
      // node-quiesced attempt against both repositories. Each role's previous
      // good attempt remains until that same role commits a replacement.
      task.mirror_only = false;
      task.primary_only = false;
      task.snapshot_name = fresh_retry_snapshot_name(&task, &run.id);
      authorize_retry_task(&mut task, server_id)?;
      retry_tasks.push(task);
    }
  }
  let partial = result_count != expected
    || !response.discovery_errors.is_empty()
    || !response.restart_errors.is_empty()
    || !retry_tasks.is_empty();
  if !response.restart_errors.is_empty() {
    // A stranded container requires operator recovery. Retrying would no
    // longer know that it was running before this batch.
    retry_tasks.clear();
  }
  Ok(NodeBatchOutcome {
    partial,
    retry_tasks,
    retry_blocked: !response.restart_errors.is_empty(),
  })
}

async fn run_target(
  settings: &BackupSettings,
  run: &BackupRun,
  target: BackupTarget,
) -> anyhow::Result<bool> {
  ensure_not_cancelled(&run.id)?;
  match target {
    BackupTarget::Core => backup_core(settings, run).await,
    BackupTarget::Stack { stack_id } => {
      backup_stack(settings, run, &stack_id).await
    }
    BackupTarget::Volume {
      server_id,
      volume_name,
    } => backup_volume(settings, run, &server_id, &volume_name).await,
    BackupTarget::Unbound { .. } => {
      Err(anyhow!("Unbound snapshots cannot be backed up"))
    }
  }
}

#[derive(Clone)]
struct CoreRepositoryRetry {
  snapshot_name: String,
  source_label: String,
  source_path: String,
  staging: PathBuf,
  retry_primary: bool,
  retry_mirror: bool,
}

fn core_repository_retries()
-> &'static Mutex<HashMap<String, CoreRepositoryRetry>> {
  static RETRIES: OnceLock<
    Mutex<HashMap<String, CoreRepositoryRetry>>,
  > = OnceLock::new();
  RETRIES.get_or_init(Default::default)
}

fn invalidate_fleet_retries() {
  fleet_generation().write().unwrap().clear();
}

async fn write_core_repository_snapshot(
  repository: BackupRepository,
  settings: &BackupSettings,
  retry: &CoreRepositoryRetry,
  cancellation: Arc<AtomicBool>,
  remove_existing: bool,
) -> anyhow::Result<komodo_backup::BackupResult> {
  let settings_for_worker = settings.clone();
  let retry_for_worker = retry.clone();
  tokio::task::spawn_blocking(move || {
    let repository =
      core_repository(&repository, &settings_for_worker)?;
    if remove_existing {
      repository.delete_snapshot_if_present(
        &retry_for_worker.snapshot_name,
      )?;
    }
    repository.backup_cancellable(
      &retry_for_worker.snapshot_name,
      &retry_for_worker.source_label,
      std::slice::from_ref(&retry_for_worker.source_path),
      Some(cancellation.as_ref()),
    )
  })
  .await
  .context("Core repository backup worker failed")?
}

/// Hash the exact relative file set and file bytes, including the manifest.
/// No worker-visible label can authorize a replacement export with new contents.
fn core_export_digest(root: &Path) -> anyhow::Result<String> {
  fn collect(
    root: &Path,
    directory: &Path,
    files: &mut BTreeMap<String, String>,
  ) -> anyhow::Result<()> {
    if !std::fs::symlink_metadata(directory)?.is_dir() {
      return Err(anyhow!(
        "Core export contains a non-directory or symlink ancestor"
      ));
    }
    for entry in std::fs::read_dir(directory)? {
      let entry = entry?;
      let path = entry.path();
      let kind = entry.file_type()?;
      if kind.is_dir() {
        collect(root, &path, files)?;
      } else if kind.is_file() {
        let mut file = std::fs::File::open(&path)?;
        let mut hasher = Sha256::new();
        let mut buffer = [0_u8; 65536];
        loop {
          let read = file.read(&mut buffer)?;
          if read == 0 {
            break;
          }
          hasher.update(&buffer[..read]);
        }
        let relative = path
          .strip_prefix(root)?
          .to_str()
          .context("Core export path is not valid UTF-8")?
          .to_string();
        files.insert(relative, hex::encode(hasher.finalize()));
      } else {
        return Err(anyhow!(
          "Core export contains a symlink or special file"
        ));
      }
    }
    Ok(())
  }
  let mut files = BTreeMap::new();
  collect(root, root, &mut files)?;
  Ok(hex::encode(Sha256::digest(serde_json::to_vec(&files)?)))
}

async fn retry_core_repositories(
  settings: &BackupSettings,
  run: &BackupRun,
  mut retry: CoreRepositoryRetry,
) -> anyhow::Result<bool> {
  ensure_not_cancelled(&run.id)?;
  let cancellation = cancellation_token(&run.id)
    .unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
  if retry.retry_primary {
    let result = write_core_repository_snapshot(
      settings.primary.clone(),
      settings,
      &retry,
      cancellation.clone(),
      true,
    )
    .await;
    retry.retry_primary =
      !matches!(&result, Ok(result) if !result.partial);
    if let Err(error) = result {
      warn!("Core primary retry failed: {error:#}");
    }
  }
  ensure_not_cancelled(&run.id)?;
  if retry.retry_mirror {
    let mirror = settings
      .mirror
      .clone()
      .context("Mirror is no longer configured")?;
    let result = write_core_repository_snapshot(
      mirror,
      settings,
      &retry,
      cancellation,
      true,
    )
    .await;
    retry.retry_mirror =
      !matches!(&result, Ok(result) if !result.partial);
    if let Err(error) = result {
      warn!("Core mirror retry failed: {error:#}");
    }
  }
  ensure_not_cancelled(&run.id)?;
  if retry.retry_primary || retry.retry_mirror {
    core_repository_retries()
      .lock()
      .unwrap()
      .insert(run.id.clone(), retry);
    return Ok(true);
  }
  core_repository_retries().lock().unwrap().remove(&run.id);
  let _ = tokio::fs::remove_dir_all(&retry.staging).await;
  Ok(false)
}

async fn backup_core(
  settings: &BackupSettings,
  run: &BackupRun,
) -> anyhow::Result<bool> {
  ensure_not_cancelled(&run.id)?;
  let repository_retry = core_repository_retries()
    .lock()
    .unwrap()
    .get(&run.id)
    .cloned();
  if let Some(retry) = repository_retry {
    return retry_core_repositories(settings, run, retry).await;
  }
  let staging = PathBuf::from(CORE_STAGING_PATH).join(&run.id);
  let _ = tokio::fs::remove_dir_all(&staging).await;
  tokio::fs::create_dir_all(&staging).await?;
  let mut staging_cleanup =
    RemoveDirectoryOnDrop::new(staging.clone());
  // A versioned logical dump is produced before upload. Mongo writes resume as
  // soon as the immutable export file is complete.
  {
    let _barrier = mutation_barrier().write().await;
    database::utils::backup_excluding(
      &db_client().db,
      &staging,
      &[SETTINGS_COLLECTION, RUNS_COLLECTION],
    )
    .await?;
  }
  ensure_not_cancelled(&run.id)?;
  let exported_collections = db_client()
    .db
    .list_collection_names()
    .await?
    .into_iter()
    .filter(|name| core_export_includes_collection(name))
    .collect::<Vec<_>>();
  let created_at = komodo_timestamp();
  let manifest = serde_json::json!({
    "schema": "komodo.core-export/v1",
    "version": komodo_build_info::version(),
    "core_instance_id": core_instance_id()?,
    "collections": exported_collections,
    "created_at": created_at,
  });
  tokio::fs::write(
    staging.join("komodo-core-manifest.json"),
    serde_json::to_vec_pretty(&manifest)?,
  )
  .await?;
  let name = snapshot_name("core", &run.id);
  let hostname = format!("komodo-core-{}", core_instance_id()?);
  let digest_root = staging.clone();
  let digest = tokio::task::spawn_blocking(move || {
    core_export_digest(&digest_root)
  })
  .await
  .context("Core export digest worker failed")??;
  let label = crypto::authorize_core_source_label(
    &BackupTarget::Core.source_label(core_instance_id()?),
    &hostname,
    &name,
    &digest,
    created_at,
  )?;
  let path = staging.to_string_lossy().into_owned();
  let mut retry = CoreRepositoryRetry {
    snapshot_name: name,
    source_label: label,
    source_path: path,
    staging: staging.clone(),
    retry_primary: true,
    retry_mirror: settings.mirror.is_some(),
  };
  let cancellation = cancellation_token(&run.id)
    .context("Core backup cancellation token is unavailable")?;
  let primary_result = write_core_repository_snapshot(
    settings.primary.clone(),
    settings,
    &retry,
    cancellation.clone(),
    false,
  )
  .await;
  retry.retry_primary =
    !matches!(&primary_result, Ok(result) if !result.partial);
  ensure_not_cancelled(&run.id)?;
  let mirror_result = if let Some(mirror) = settings.mirror.clone() {
    let result = write_core_repository_snapshot(
      mirror,
      settings,
      &retry,
      cancellation,
      false,
    )
    .await;
    retry.retry_mirror =
      !matches!(&result, Ok(result) if !result.partial);
    Some(result)
  } else {
    retry.retry_mirror = false;
    None
  };
  ensure_not_cancelled(&run.id)?;
  if !retry.retry_primary && !retry.retry_mirror {
    let _ = tokio::fs::remove_dir_all(&staging).await;
    return Ok(false);
  }
  if *fleet_generation().read().unwrap() == run.id {
    warn!(
      retry_primary = retry.retry_primary,
      retry_mirror = retry.retry_mirror,
      "Core repository retry retained the immutable database export"
    );
    core_repository_retries()
      .lock()
      .unwrap()
      .insert(run.id.clone(), retry);
    staging_cleanup.disarm();
    return Ok(true);
  }
  let _ = tokio::fs::remove_dir_all(&staging).await;
  match (primary_result, mirror_result) {
    (Err(error), _) => Err(error),
    (_, Some(Err(error))) => Err(error),
    (Ok(primary), Some(Ok(mirror))) => {
      Ok(primary.partial || mirror.partial)
    }
    (Ok(primary), None) => Ok(primary.partial),
  }
}

async fn backup_stack(
  settings: &BackupSettings,
  run: &BackupRun,
  stack_id: &str,
) -> anyhow::Result<bool> {
  let _mutation = mutation_barrier().write().await;
  let stack = resource::get::<Stack>(stack_id).await?;
  let resolved_stack_id = stack.id.clone();
  if !stack.config.swarm_id.is_empty() {
    return Err(anyhow!(
      "Swarm stacks are not supported by backup v1"
    ));
  }
  let server =
    resource::get::<Server>(&stack.config.server_id).await?;
  let snapshot_name = snapshot_name("stack", &run.id);
  let hostname = format!("{PERIPHERY_HOSTNAME_PREFIX}{}", server.id);
  let repo = if stack.config.linked_repo.is_empty() {
    None
  } else {
    Some(resource::get::<Repo>(&stack.config.linked_repo).await?)
  };
  let response = periphery_client(&server)
    .await?
    .request(RunVykarBackup {
      target: PeripheryBackupTarget::Stack {
        stack: Box::new(stack),
        repo: repo.map(Box::new),
      },
      primary: repository_for_periphery(&settings.primary, false)?,
      mirror: settings
        .mirror
        .as_ref()
        .map(|repository| repository_for_periphery(repository, true))
        .transpose()?,
      advanced: settings.advanced.clone(),
      hostname: hostname.clone(),
      source_label: authorized_source_label(
        &BackupTarget::Stack {
          stack_id: resolved_stack_id,
        },
        &hostname,
        &snapshot_name,
      )?,
      snapshot_name,
      run_id: run.id.clone(),
      komodo_version: komodo_build_info::version().into(),
      protected_repository_paths: core_local_repository_paths(
        settings,
      )?,
      filters: backup_source_filters(settings),
      stop_containers: settings.stop_containers,
      mirror_only: false,
      primary_only: false,
    })
    .await?;
  if !response.restart_errors.is_empty() {
    record_operational_alert(format!(
      "Containers could not be restarted after Stack backup on {} ({}): {}",
      server.name,
      server.id,
      response.restart_errors.join("; ")
    ));
  }
  if let Some(error) = response.primary.error {
    return Err(anyhow!(error));
  }
  Ok(
    response.primary.partial
      || response.mirror.as_ref().is_some_and(|result| {
        result.partial || result.error.is_some()
      })
      || !response.restart_errors.is_empty(),
  )
}

async fn backup_volume(
  settings: &BackupSettings,
  run: &BackupRun,
  server_id: &str,
  volume_name: &str,
) -> anyhow::Result<bool> {
  let _mutation = mutation_barrier().write().await;
  let server = resource::get::<Server>(server_id).await?;
  let snapshot_name = snapshot_name("volume", &run.id);
  let hostname = format!("{PERIPHERY_HOSTNAME_PREFIX}{}", server.id);
  let response = periphery_client(&server)
    .await?
    .request(RunVykarBackup {
      target: PeripheryBackupTarget::Volume {
        volume_name: volume_name.into(),
      },
      primary: repository_for_periphery(&settings.primary, false)?,
      mirror: settings
        .mirror
        .as_ref()
        .map(|repository| repository_for_periphery(repository, true))
        .transpose()?,
      advanced: settings.advanced.clone(),
      hostname: hostname.clone(),
      source_label: authorized_source_label(
        &BackupTarget::Volume {
          server_id: server.id.clone(),
          volume_name: volume_name.into(),
        },
        &hostname,
        &snapshot_name,
      )?,
      snapshot_name,
      run_id: run.id.clone(),
      komodo_version: komodo_build_info::version().into(),
      protected_repository_paths: core_local_repository_paths(
        settings,
      )?,
      filters: backup_source_filters(settings),
      stop_containers: settings.stop_containers,
      mirror_only: false,
      primary_only: false,
    })
    .await?;
  if !response.restart_errors.is_empty() {
    record_operational_alert(format!(
      "Containers could not be restarted after Volume backup on {} ({}): {}",
      server.name,
      server.id,
      response.restart_errors.join("; ")
    ));
  }
  if let Some(error) = response.primary.error {
    return Err(anyhow!(error));
  }
  Ok(
    response.primary.partial
      || response.mirror.as_ref().is_some_and(|result| {
        result.partial || result.error.is_some()
      })
      || !response.restart_errors.is_empty(),
  )
}

pub async fn authorize_target(
  target: &BackupTarget,
  user: &User,
  level: komodo_client::entities::permission::PermissionLevel,
) -> anyhow::Result<()> {
  match target {
    BackupTarget::Core => {
      if user.admin {
        Ok(())
      } else {
        Err(anyhow!("Core backup operations are admin only"))
      }
    }
    BackupTarget::Stack { stack_id } => {
      get_check_permissions::<Stack>(stack_id, user, level.backups())
        .await
        .map(|_| ())
    }
    BackupTarget::Volume { server_id, .. } => {
      get_check_permissions::<Server>(
        server_id,
        user,
        level.backups(),
      )
      .await
      .map(|_| ())
    }
    BackupTarget::Unbound { .. } => {
      if user.admin {
        Ok(())
      } else {
        Err(anyhow!("Unbound snapshot recovery is admin only"))
      }
    }
  }
}

pub async fn authorize_snapshot(
  snapshot_name: &str,
  user: &User,
  level: komodo_client::entities::permission::PermissionLevel,
) -> anyhow::Result<BackupSnapshot> {
  let (snapshots, _) = list_snapshots().await?;
  let snapshot = snapshots
    .into_iter()
    .find(|snapshot| snapshot.name == snapshot_name)
    .context("Snapshot does not exist in the primary repository")?;
  let source_resource_missing = match &snapshot.target {
    BackupTarget::Stack { stack_id } => Stack::coll()
      .find_one(id_or_name_filter(stack_id))
      .await?
      .is_none(),
    BackupTarget::Volume { server_id, .. } => Server::coll()
      .find_one(id_or_name_filter(server_id))
      .await?
      .is_none(),
    BackupTarget::Core | BackupTarget::Unbound { .. } => false,
  };
  if source_resource_missing {
    if !user.admin {
      return Err(anyhow!(
        "Only administrators can recover a snapshot whose source resource was deleted"
      ));
    }
  } else {
    authorize_target(&snapshot.target, user, level).await?;
  }
  Ok(snapshot)
}

pub fn snapshot_server_id(snapshot: &BackupSnapshot) -> Option<&str> {
  snapshot.hostname.strip_prefix(PERIPHERY_HOSTNAME_PREFIX)
}

async fn snapshot_stack_source(
  snapshot: &BackupSnapshot,
) -> anyhow::Result<(
  Stack,
  Option<Stack>,
  Vec<String>,
  BTreeMap<String, String>,
)> {
  let BackupTarget::Stack { stack_id } = &snapshot.target else {
    return Err(anyhow!("Snapshot is not a Stack backup"));
  };
  let current =
    Stack::coll().find_one(id_or_name_filter(stack_id)).await?;
  let manifest_source = snapshot
    .source_paths
    .iter()
    .find(|path| is_backup_manifest_source(&snapshot.name, path))
    .context("Stack snapshot has no embedded recovery manifest")?
    .clone();
  let staging = PathBuf::from(STACK_MANIFEST_STAGING_PATH)
    .join(Uuid::new_v4().to_string());
  tokio::fs::create_dir_all(&staging).await?;
  let _staging_cleanup = RemoveDirectoryOnDrop::new(staging.clone());
  let destination = staging.clone();
  let settings = get_settings().await?;
  let repository = settings.primary.clone();
  let advanced = settings.advanced.clone();
  let hostname = snapshot.hostname.clone();
  let snapshot_name = snapshot.name.clone();
  tokio::task::spawn_blocking(move || {
    VykarRepository::new(
      &repository,
      &hostname,
      &core_cache_dir()?,
      &core_secret_dir()?,
      &advanced,
    )?
    .restore(&snapshot_name, &destination, &[manifest_source])
  })
  .await
  .context("Stack manifest restore worker failed")??;
  let manifest_path =
    find_file_named(&staging, "komodo-backup-manifest.json")
      .context("Stack snapshot recovery manifest is missing")?;
  let manifest: SnapshotBackupManifest =
    serde_json::from_slice(&std::fs::read(manifest_path)?)?;
  if manifest.schema != "komodo.backup-manifest/v1"
    || manifest.version != 1
    || manifest.run_id != snapshot.run_id
    || manifest.source_label != snapshot.source_label
    || manifest.hostname != snapshot.hostname
  {
    return Err(anyhow!(
      "Stack snapshot recovery manifest identity is invalid"
    ));
  }
  let configuration_sha256 = hex::encode(Sha256::digest(
    serde_json::to_vec(&manifest.target)?,
  ));
  let paths_sha256 =
    hex::encode(Sha256::digest(serde_json::to_vec(&manifest.paths)?));
  let path_aliases_sha256 = hex::encode(Sha256::digest(
    serde_json::to_vec(&manifest.path_aliases)?,
  ));
  if configuration_sha256 != manifest.configuration_sha256
    || paths_sha256 != manifest.paths_sha256
    || manifest
      .path_aliases_sha256
      .as_deref()
      .is_some_and(|expected| expected != path_aliases_sha256)
    || !manifest.path_aliases.is_empty()
      && manifest.path_aliases_sha256.is_none()
  {
    return Err(anyhow!(
      "Stack snapshot recovery manifest checksum is invalid"
    ));
  }
  let PeripheryBackupTarget::Stack { stack, .. } = manifest.target
  else {
    return Err(anyhow!(
      "Stack snapshot recovery manifest has the wrong target type"
    ));
  };
  if stack.id != *stack_id {
    return Err(anyhow!(
      "Stack snapshot recovery manifest does not match its source label"
    ));
  }
  Ok((*stack, current, manifest.paths, manifest.path_aliases))
}

pub async fn current_stack_backup_source(
  stack_id: &str,
) -> anyhow::Result<(String, Vec<String>)> {
  let settings = get_settings().await?;
  let stack = resource::get::<Stack>(stack_id).await?;
  let server =
    resource::get::<Server>(&stack.config.server_id).await?;
  let repo = if stack.config.linked_repo.is_empty() {
    None
  } else {
    Some(resource::get::<Repo>(&stack.config.linked_repo).await?)
  };
  let source = periphery_client(&server)
    .await?
    .request(DiscoverBackupSource {
      target: PeripheryBackupTarget::Stack {
        stack: Box::new(stack),
        repo: repo.map(Box::new),
      },
      filters: backup_source_filters(&settings),
      protected_repository_paths: core_local_repository_paths(
        &settings,
      )?,
    })
    .await?;
  Ok((server.id, source.paths))
}

pub fn backup_source_paths_match(
  left: &[String],
  right: &[String],
) -> bool {
  let mut left = left.to_vec();
  let mut right = right.to_vec();
  left.sort();
  right.sort();
  left == right
}

fn stack_restore_requires_recovery(
  snapshot_server_id: &str,
  current_server_id: Option<&str>,
  destination_server_id: &str,
) -> bool {
  current_server_id != Some(snapshot_server_id)
    || destination_server_id != snapshot_server_id
}

pub async fn plan_restore(
  snapshot: BackupSnapshot,
  user: &User,
  request: PlanBackupRestore,
) -> anyhow::Result<BackupRestorePlan> {
  cleanup_expired_restore_plans().await?;
  let _repository_roles =
    repository_role_barrier().clone().read_owned().await;
  let PlanBackupRestore {
    destination_server_id,
    selected_paths,
    mut recovered_stack_name,
    bind_path_mappings,
    destination_volume_name,
    confirm_existing_volume,
    ..
  } = request;
  if snapshot.partial {
    return Err(anyhow!(
      "Partial snapshots are diagnostic-only and cannot be restored"
    ));
  }
  let selected_paths = normalize_selected_paths(&selected_paths)?;
  let (
    snapshot_stack,
    current_stack,
    snapshot_stack_paths,
    snapshot_stack_path_aliases,
  ) = if matches!(&snapshot.target, BackupTarget::Stack { .. }) {
    let (snapshot_stack, current_stack, source_paths, path_aliases) =
      snapshot_stack_source(&snapshot).await?;
    (
      Some(snapshot_stack),
      current_stack,
      source_paths,
      path_aliases,
    )
  } else {
    (None, None, Vec::new(), BTreeMap::new())
  };
  let authenticated_snapshot_stack_paths =
    snapshot_stack_paths.clone();
  let source_resource_missing = match &snapshot.target {
    BackupTarget::Stack { .. } => current_stack.is_none(),
    BackupTarget::Volume { server_id, .. } => Server::coll()
      .find_one(id_or_name_filter(server_id))
      .await?
      .is_none(),
    BackupTarget::Core | BackupTarget::Unbound { .. } => false,
  };
  let destination_server_id = match destination_server_id {
    Some(destination) => Some(destination),
    None => match &snapshot.target {
      BackupTarget::Volume { server_id, .. } => {
        Some(server_id.clone())
      }
      BackupTarget::Stack { .. } => current_stack
        .as_ref()
        .or(snapshot_stack.as_ref())
        .map(|stack| stack.config.server_id.clone()),
      BackupTarget::Core => None,
      BackupTarget::Unbound { .. } => None,
    },
  };
  let mut publish = Vec::new();
  let mut confirmed_bind_path_mappings = HashMap::new();
  let mut recovered_stack_source = None;
  let mut recovered_stack_run_directory = None;
  match &snapshot.target {
    BackupTarget::Core => {
      return Err(anyhow!(
        "Core recovery is available only from the initial setup recovery flow"
      ));
    }
    BackupTarget::Unbound { .. } => {
      return Err(anyhow!(
        "Unbound snapshot recovery requires the administrator recovery flow"
      ));
    }
    BackupTarget::Stack { .. } => {
      let snapshot_stack = snapshot_stack
        .as_ref()
        .context("Stack snapshot metadata is missing")?;
      let destination = destination_server_id
        .clone()
        .unwrap_or_else(|| snapshot_stack.config.server_id.clone());
      let current_server_id = current_stack
        .as_ref()
        .map(|stack| stack.config.server_id.as_str());
      let explicitly_recovering = recovered_stack_name
        .as_deref()
        .is_some_and(|name| !name.trim().is_empty());
      let mut recovering_stack = explicitly_recovering
        || stack_restore_requires_recovery(
          &snapshot_stack.config.server_id,
          current_server_id,
          &destination,
        );
      if !recovering_stack {
        let (_, current_paths) = current_stack_backup_source(
          &current_stack
            .as_ref()
            .context("Current Stack is missing")?
            .id,
        )
        .await
        .context(
          "Failed to discover the current Stack backup roots",
        )?;
        recovering_stack = !backup_source_paths_match(
          &snapshot_stack_paths,
          &current_paths,
        );
      }
      let stack = if recovering_stack {
        snapshot_stack
      } else {
        current_stack.as_ref().unwrap_or(snapshot_stack)
      };
      if recovering_stack {
        if !Stack::user_can_create(user) {
          return Err(anyhow!(
            "Recovered Stack creation requires Stack-create permission"
          ));
        }
        let recovered_name = recovered_stack_name
          .as_deref()
          .context("Recovered Stack name is missing")?;
        let recovered_name = Stack::validated_name(recovered_name);
        if recovered_name.is_empty() {
          return Err(anyhow!(
            "Recovered Stack name must contain Docker-compatible characters"
          ));
        }
        if Stack::coll()
          .find_one(doc! { "name": &recovered_name })
          .await?
          .is_some()
        {
          return Err(anyhow!(
            "A Stack named '{recovered_name}' already exists"
          ));
        }
        recovered_stack_name = Some(recovered_name);
        recovered_stack_source = Some(stack.clone());
      }
      if recovering_stack && bind_path_mappings.is_empty() {
        return Err(anyhow!(
          "Recovered Stack restore requires explicit source-path mappings"
        ));
      }
      if recovering_stack && !selected_paths.is_empty() {
        return Err(anyhow!(
          "Recovered Stack creation requires the complete snapshot"
        ));
      }
      let source_paths = snapshot_stack_paths;
      if source_paths.is_empty() {
        return Err(anyhow!(
          "Snapshot does not contain a Stack run directory"
        ));
      }
      for source in source_paths {
        let destination_path = if !recovering_stack {
          source.clone()
        } else {
          bind_path_mappings
            .get(&source)
            .with_context(|| {
              format!(
                "Recovered Stack restore is missing a destination mapping for '{source}'"
              )
            })?
            .clone()
        };
        if !Path::new(&destination_path).is_absolute() {
          return Err(anyhow!(
            "Restore destination must be absolute: {destination_path}"
          ));
        }
        if recovering_stack {
          confirmed_bind_path_mappings
            .insert(source.clone(), destination_path.clone());
        }
        publish.push(
          periphery_client::api::backup::RestorePublishPath {
            destination_root: None,
            snapshot_path: source.trim_start_matches('/').into(),
            destination: destination_path,
          },
        );
      }
      if recovering_stack {
        recovered_stack_run_directory =
          publish.first().map(|path| path.destination.clone());
        let name = recovered_stack_name
          .as_deref()
          .context("Recovered Stack name is missing")?;
        let mut config:
          komodo_client::entities::stack::PartialStackConfig =
          stack.clone().config.into();
        config.server_id = Some(destination);
        config.swarm_id = Some(String::new());
        config.project_name = Some(name.to_string());
        config.files_on_host = Some(true);
        config.run_directory = recovered_stack_run_directory.clone();
        config.repo = Some(String::new());
        config.linked_repo = Some(String::new());
        Stack::validate_create_config(&mut config, user).await?;
      }
    }
    BackupTarget::Volume {
      server_id,
      volume_name,
    } => {
      let destination_server = destination_server_id
        .clone()
        .unwrap_or_else(|| server_id.clone());
      let destination_name = destination_volume_name
        .clone()
        .unwrap_or_else(|| volume_name.clone());
      if destination_server != *server_id
        && destination_name == *volume_name
      {
        return Err(anyhow!(
          "Cross-node volume restore must use a new destination volume name"
        ));
      }
      // Periphery validates the actual local volume mountpoint immediately
      // before execution; this logical destination is resolved there.
      let source_path = snapshot
        .source_paths
        .iter()
        .find(|path| !is_backup_manifest_source(&snapshot.name, path))
        .context("Snapshot does not contain a volume source path")?;
      publish.push(
        periphery_client::api::backup::RestorePublishPath {
          destination_root: None,
          snapshot_path: source_path.trim_start_matches('/').into(),
          destination: format!(
            "/var/lib/docker/volumes/{destination_name}/_data"
          ),
        },
      );
    }
  }
  if !selected_paths.is_empty() {
    publish = selected_publish_paths(&selected_paths, &publish)?;
    if publish.is_empty() {
      return Err(anyhow!(
        "Selected paths do not belong to a restorable Stack or Volume source"
      ));
    }
  }
  validate_non_overlapping_destinations(&publish)?;
  let volume_confirmation_required = volume_requires_confirmation(
    &snapshot.target,
    destination_server_id.as_deref(),
    destination_volume_name.as_deref(),
  );
  let destination_server = destination_server_id
    .clone()
    .context("Restore destination server is missing")?;
  let server = resource::get::<Server>(&destination_server).await?;
  let target = restore_periphery_target(
    &snapshot.target,
    &destination_server,
    recovered_stack_source.as_ref(),
    recovered_stack_name.as_deref(),
    destination_volume_name.as_deref(),
    publish.first().map(|path| path.destination.as_str()),
  )
  .await?;
  let settings = get_settings().await?;
  let preflight = periphery_client(&server)
    .await?
    .request(PreflightVykarRestore {
      target,
      repository: repository_for_periphery(&settings.primary, false)?,
      protected_repository_paths: core_local_repository_paths(
        &settings,
      )?,
      advanced: settings.advanced,
      hostname: format!("komodo-periphery-{}", server.id),
      snapshot_name: snapshot.name.clone(),
      selected_paths: selected_paths.clone(),
      publish: publish.clone(),
    })
    .await?;
  if volume_confirmation_required
    && !confirm_existing_volume
    && preflight.destination_exists
  {
    return Err(anyhow!(
      "Restore into an explicitly selected or different existing volume requires confirmation"
    ));
  }
  let create_volume_if_missing =
    matches!(&snapshot.target, BackupTarget::Volume { .. })
      && !preflight.destination_exists;
  let plan = BackupRestorePlan {
    id: Uuid::new_v4().to_string(),
    snapshot: snapshot.name,
    source: snapshot.target,
    destination_server_id,
    selected_paths,
    created_paths: preflight.created_paths,
    overwritten_paths: preflight.overwritten_paths,
    deleted_paths: preflight.deleted_paths,
    containers_to_stop: preflight.containers_to_stop,
    expires_at: komodo_timestamp() + 15 * 60 * 1000,
  };
  plans_collection()
    .insert_one(StoredRestorePlan {
      id: plan.id.clone(),
      created_by: user.id.clone(),
      plan: plan.clone(),
      publish,
      recovered_stack_name,
      recovered_stack_execution_started: false,
      recovered_stack_id: None,
      recovered_stack_finalized: false,
      recovered_stack_run_directory,
      destination_volume_name: destination_volume_name.clone(),
      create_volume_if_missing,
      destination_exists: preflight.destination_exists,
      recovered_stack_source,
      source_resource_missing,
      snapshot_stack_source_paths: authenticated_snapshot_stack_paths,
      snapshot_stack_path_aliases: snapshot_stack_path_aliases
        .into_iter()
        .collect(),
      bind_path_mappings: confirmed_bind_path_mappings,
    })
    .await?;
  Ok(plan)
}

fn volume_requires_confirmation(
  source: &BackupTarget,
  destination_server_id: Option<&str>,
  destination_volume_name: Option<&str>,
) -> bool {
  matches!(source, BackupTarget::Volume { server_id, .. }
    if destination_volume_name.is_some()
      || destination_server_id.is_some_and(|destination| destination != server_id))
}

fn selected_publish_paths(
  selected: &[String],
  roots: &[periphery_client::api::backup::RestorePublishPath],
) -> anyhow::Result<
  Vec<periphery_client::api::backup::RestorePublishPath>,
> {
  let mut publish = Vec::new();
  for selected in selected {
    let path = Path::new(selected);
    if path.is_absolute()
      || path.components().any(|component| {
        matches!(component, std::path::Component::ParentDir)
      })
    {
      return Err(anyhow!("Unsafe selected restore path"));
    }
    let Some((root, relative, _)) = roots
      .iter()
      .filter_map(|root| {
        let source = Path::new(root.snapshot_path.trim_matches('/'));
        path.strip_prefix(source).ok().map(|relative| {
          (root, relative, source.components().count())
        })
      })
      .max_by_key(|(_, _, depth)| *depth)
    else {
      continue;
    };
    let destination = Path::new(&root.destination).join(relative);
    publish.push(periphery_client::api::backup::RestorePublishPath {
      destination_root: Some(
        root
          .destination_root
          .clone()
          .unwrap_or_else(|| root.destination.clone()),
      ),
      snapshot_path: selected.clone(),
      destination: destination.to_string_lossy().into_owned(),
    });
  }
  publish
    .sort_by(|left, right| left.destination.cmp(&right.destination));
  publish
    .dedup_by(|left, right| left.destination == right.destination);
  Ok(publish)
}

fn validate_non_overlapping_destinations(
  publish: &[periphery_client::api::backup::RestorePublishPath],
) -> anyhow::Result<()> {
  let mut destinations = Vec::with_capacity(publish.len());
  for item in publish {
    let path = Path::new(&item.destination);
    if !path.is_absolute()
      || path.components().any(|component| {
        matches!(component, std::path::Component::ParentDir)
      })
    {
      return Err(anyhow!(
        "Restore destination must be an absolute normalized path: {}",
        item.destination
      ));
    }
    destinations.push(path.components().collect::<PathBuf>());
  }
  for (index, left) in destinations.iter().enumerate() {
    for right in destinations.iter().skip(index + 1) {
      if left == right
        || left.starts_with(right)
        || right.starts_with(left)
      {
        return Err(anyhow!(
          "Restore destinations overlap: '{}' and '{}'",
          left.display(),
          right.display()
        ));
      }
    }
  }
  Ok(())
}

fn same_restore_preview(
  stored: &StoredRestorePlan,
  current: &PreflightVykarRestoreResponse,
) -> bool {
  fn same_strings(left: &[String], right: &[String]) -> bool {
    let mut left = left.to_vec();
    let mut right = right.to_vec();
    left.sort();
    right.sort();
    left == right
  }

  stored.destination_exists == current.destination_exists
    && same_strings(
      &stored.plan.created_paths,
      &current.created_paths,
    )
    && same_strings(
      &stored.plan.overwritten_paths,
      &current.overwritten_paths,
    )
    && same_strings(
      &stored.plan.deleted_paths,
      &current.deleted_paths,
    )
    && same_strings(
      &stored.plan.containers_to_stop,
      &current.containers_to_stop,
    )
}

async fn restore_periphery_target(
  source: &BackupTarget,
  destination_server: &str,
  recovered_stack_source: Option<&Stack>,
  recovered_stack_name: Option<&str>,
  destination_volume_name: Option<&str>,
  recovered_run_directory: Option<&str>,
) -> anyhow::Result<PeripheryBackupTarget> {
  match source {
    BackupTarget::Stack { stack_id } => {
      let (mut stack, recovering_stack) =
        if let Some(stack) = recovered_stack_source {
          (stack.clone(), true)
        } else {
          (resource::get::<Stack>(stack_id).await?, false)
        };
      if recovering_stack {
        let name = recovered_stack_name
          .filter(|name| !name.trim().is_empty())
          .context("Recovered Stack name is missing")?;
        stack.id.clear();
        stack.name = name.to_string();
        stack.config.server_id = destination_server.to_string();
        stack.config.swarm_id.clear();
        stack.config.project_name = name.to_string();
        stack.config.files_on_host = true;
        stack.config.run_directory = recovered_run_directory
          .context("Recovered Stack run directory is missing")?
          .to_string();
        stack.config.repo.clear();
        stack.config.linked_repo.clear();
        Ok(PeripheryBackupTarget::Stack {
          stack: Box::new(stack),
          repo: None,
        })
      } else {
        let repo = if stack.config.linked_repo.is_empty() {
          None
        } else {
          Some(
            resource::get::<Repo>(&stack.config.linked_repo).await?,
          )
        };
        Ok(PeripheryBackupTarget::Stack {
          stack: Box::new(stack),
          repo: repo.map(Box::new),
        })
      }
    }
    BackupTarget::Volume { volume_name, .. } => {
      Ok(PeripheryBackupTarget::Volume {
        volume_name: destination_volume_name
          .unwrap_or(volume_name)
          .to_string(),
      })
    }
    BackupTarget::Core | BackupTarget::Unbound { .. } => {
      Err(anyhow!("Snapshot target cannot be restored on Periphery"))
    }
  }
}

fn recovered_stack_belongs_to_plan(
  stored: &StoredRestorePlan,
  stack: &Stack,
) -> bool {
  stack.info.recovery_plan_id.as_deref() == Some(stored.id.as_str())
    || (stored.recovered_stack_finalized
      && stored.recovered_stack_id.as_deref()
        == Some(stack.id.as_str()))
}

async fn finalize_recovered_stack_saga(
  stored: &mut StoredRestorePlan,
  server: &Server,
  stack: &Stack,
) -> anyhow::Result<()> {
  let periphery = periphery_client(server).await?;
  if !stored.recovered_stack_finalized {
    let finalized = periphery
      .request(FinalizeVykarRestore {
        journal_id: stored.id.clone(),
        commit: true,
        acknowledge: false,
      })
      .await?;
    if !finalized.complete
      || finalized.rolled_back
      || finalized.critical_error.is_some()
    {
      return Err(anyhow!(
        "Periphery did not confirm recovered Stack restore commit: {}",
        finalized
          .critical_error
          .unwrap_or_else(|| "incomplete finalization".into())
      ));
    }
    plans_collection()
      .update_one(
        doc! { "_id": &stored.id },
        doc! { "$set": {
          "recovered_stack_id": &stack.id,
          "recovered_stack_finalized": true,
        } },
      )
      .await
      .context(
        "Failed to persist recovered Stack finalization outcome",
      )?;
    stored.recovered_stack_id = Some(stack.id.clone());
    stored.recovered_stack_finalized = true;
  }

  let acknowledged = periphery
    .request(FinalizeVykarRestore {
      journal_id: stored.id.clone(),
      commit: true,
      acknowledge: true,
    })
    .await?;
  if !acknowledged.complete
    || acknowledged.rolled_back
    || acknowledged.critical_error.is_some()
  {
    return Err(anyhow!(
      "Periphery did not acknowledge recovered Stack restore commit: {}",
      acknowledged
        .critical_error
        .unwrap_or_else(|| "incomplete acknowledgement".into())
    ));
  }
  Stack::coll()
    .update_one(
      doc! {
        "_id": database::bson::oid::ObjectId::parse_str(&stack.id)?,
        "info.recovery_plan_id": &stored.id,
      },
      doc! { "$unset": { "info.recovery_plan_id": "" } },
    )
    .await
    .context(
      "Failed to clear recovered Stack reconciliation marker",
    )?;
  plans_collection()
    .delete_one(doc! { "_id": &stored.id })
    .await
    .context("Failed to delete completed restore plan")?;
  Ok(())
}

pub async fn execute_restore(
  plan_id: &str,
  user: &User,
) -> anyhow::Result<BackupRun> {
  let _operation = backup_operation_lock().lock().await;
  let _repository_roles =
    repository_role_barrier().clone().read_owned().await;
  let mutation_guard = mutation_barrier().write().await;
  let mut stored = plans_collection()
    .find_one(doc! { "_id": plan_id, "created_by": &user.id })
    .await?
    .context("Restore plan does not exist")?;
  if !stored.plan.selected_paths.is_empty()
    && stored
      .publish
      .iter()
      .any(|path| path.destination_root.is_none())
  {
    return Err(anyhow!(
      "Selected restore plan has no original destination boundary; create a fresh preview"
    ));
  }
  if stored.plan.expires_at < komodo_timestamp()
    && (stored.recovered_stack_name.is_none()
      || !stored.recovered_stack_execution_started)
  {
    plans_collection()
      .delete_one(doc! { "_id": &stored.id })
      .await?;
    return Err(anyhow!("Restore plan has expired"));
  }
  if stored.source_resource_missing {
    if !user.admin {
      return Err(anyhow!(
        "Only administrators can recover a snapshot whose source resource was deleted"
      ));
    }
  } else {
    authorize_target(
      &stored.plan.source,
      user,
      komodo_client::entities::permission::PermissionLevel::Execute,
    )
    .await?;
  }
  let server_id = stored
    .plan
    .destination_server_id
    .clone()
    .or_else(|| match &stored.plan.source {
      BackupTarget::Volume { server_id, .. } => {
        Some(server_id.clone())
      }
      _ => None,
    })
    .context("Restore destination server is missing")?;
  let server = resource::get::<Server>(&server_id).await?;
  let recovering_stack = stored.recovered_stack_name.is_some();
  let source_server_id = match &stored.plan.source {
    BackupTarget::Stack { stack_id } => Some(
      if let Some(stack) = stored.recovered_stack_source.as_ref() {
        stack.config.server_id.clone()
      } else {
        resource::get::<Stack>(stack_id).await?.config.server_id
      },
    ),
    BackupTarget::Volume { server_id, .. } => Some(server_id.clone()),
    BackupTarget::Core | BackupTarget::Unbound { .. } => None,
  };
  if recovering_stack
    || source_server_id.as_deref() != Some(server_id.as_str())
  {
    authorize_target(
      &BackupTarget::Volume {
        server_id: server_id.clone(),
        volume_name: String::new(),
      },
      user,
      komodo_client::entities::permission::PermissionLevel::Execute,
    )
    .await?;
  }
  let recovered_stack = stored.recovered_stack_source.clone();
  if recovered_stack.is_some() {
    if !Stack::user_can_create(user) {
      return Err(anyhow!(
        "Recovered Stack creation requires Stack-create permission"
      ));
    }
    let recovered_name = stored
      .recovered_stack_name
      .as_deref()
      .context("Recovered Stack name is missing")?;
    if Stack::validated_name(recovered_name) != recovered_name {
      return Err(anyhow!(
        "Recovered Stack name is not normalized; create a new preflight"
      ));
    }
    if let Some(existing) = Stack::coll()
      .find_one(doc! { "name": recovered_name })
      .await?
      && !recovered_stack_belongs_to_plan(&stored, &existing)
    {
      return Err(anyhow!(
        "A Stack named '{recovered_name}' now exists; create a new preflight"
      ));
    }
  }
  if let BackupTarget::Stack { stack_id } = &stored.plan.source
    && recovered_stack.is_none()
  {
    let (current_server, current_paths) =
      current_stack_backup_source(stack_id).await.context(
        "Failed to revalidate the current Stack backup roots",
      )?;
    if current_server != server_id
      || !backup_source_paths_match(
        &stored.snapshot_stack_source_paths,
        &current_paths,
      )
    {
      return Err(anyhow!(
        "Stack backup roots changed after confirmation; create and review a new restore preflight"
      ));
    }
  }
  let target = restore_periphery_target(
    &stored.plan.source,
    &server_id,
    recovered_stack.as_ref(),
    stored.recovered_stack_name.as_deref(),
    stored.destination_volume_name.as_deref(),
    stored.recovered_stack_run_directory.as_deref().or_else(|| {
      stored.publish.first().map(|path| path.destination.as_str())
    }),
  )
  .await?;
  let settings = get_settings().await?;
  let refreshed_preview = periphery_client(&server)
    .await?
    .request(PreflightVykarRestore {
      target: target.clone(),
      repository: repository_for_periphery(&settings.primary, false)?,
      protected_repository_paths: core_local_repository_paths(
        &settings,
      )?,
      advanced: settings.advanced.clone(),
      hostname: format!("komodo-periphery-{}", server.id),
      snapshot_name: stored.plan.snapshot.clone(),
      selected_paths: stored.plan.selected_paths.clone(),
      publish: stored.publish.clone(),
    })
    .await?;
  if !same_restore_preview(&stored, &refreshed_preview) {
    return Err(anyhow!(
      "Restore preview changed after confirmation; create and review a new preflight"
    ));
  }
  // Vykar restore does not currently expose cooperative cancellation. Reject
  // cancellation up front instead of advertising a request that cannot stop
  // the snapshot download and then blocking behind the operation lock.
  let run = new_non_cancellable_run(
    Some(stored.plan.source.clone()),
    "Restore running",
  )
  .await?;
  let operation = async {
    let recovered_run_directory =
      stored.recovered_stack_run_directory.clone().or_else(|| {
        stored.publish.first().map(|path| path.destination.clone())
      });
    // Rebuild and validate the exact recovered Stack config under the
    // mutation barrier immediately before Periphery publishes any files.
    // Publication remains reversible until this config is inserted.
    let recovered_creation =
      if let Some(stack) = recovered_stack.as_ref() {
        let name = stored
          .recovered_stack_name
          .clone()
          .context("Recovered stack name is missing")?;
        let mut config:
          komodo_client::entities::stack::PartialStackConfig =
          stack.clone().config.into();
        config.server_id = Some(server_id.clone());
        config.swarm_id = Some(String::new());
        config.project_name = Some(name.clone());
        config.files_on_host = Some(true);
        config.run_directory = recovered_run_directory.clone();
        config.repo = Some(String::new());
        config.linked_repo = Some(String::new());
        Stack::validate_create_config(&mut config, user).await?;
        let existing = Stack::coll()
          .find_one(doc! { "name": &name })
          .await?;
        if stored.plan.expires_at < komodo_timestamp()
          && existing.is_none()
        {
          return Err(anyhow!(
            "Restore plan expired before recovered Stack creation; reconciliation will discard any interrupted publication"
          ));
        }
        if let Some(existing) = &existing {
          if !recovered_stack_belongs_to_plan(&stored, existing) {
            return Err(anyhow!(
              "A Stack named '{name}' now exists and is not linked to this restore plan; create a new preflight"
            ));
          }
          let expected: StackConfig = config.clone().into();
          if to_document(&existing.config)? != to_document(&expected)? {
            return Err(anyhow!(
              "Recovered Stack '{name}' changed before restore finalization"
            ));
          }
        } else if stored.recovered_stack_id.is_some()
          || stored.recovered_stack_finalized
        {
          return Err(anyhow!(
            "Recovered Stack recorded by this restore plan no longer exists"
          ));
        }
        Some((name, config, existing))
      } else {
        None
      };
    let periphery = periphery_client(&server).await?;
    let existing_recovered_stack = recovered_creation
      .as_ref()
      .and_then(|(_, _, existing)| existing.as_ref());
    let journal_id = if recovered_creation.is_some() {
      stored.id.clone()
    } else {
      run.id.clone()
    };
    if recovered_creation.is_some()
      && existing_recovered_stack.is_none()
      && stored.recovered_stack_execution_started
    {
      // A prior attempt can crash after publication but before the marked
      // Stack insert. Reset only that stable plan journal before replaying.
      let reset = periphery
        .request(FinalizeVykarRestore {
          journal_id: journal_id.clone(),
          commit: false,
          acknowledge: true,
        })
        .await?;
      if !reset.complete
        || !reset.rolled_back
        || reset.critical_error.is_some()
      {
        return Err(anyhow!(
          "Previous recovered Stack publication could not be reset: {}",
          reset
            .critical_error
            .unwrap_or_else(|| "incomplete rollback".into())
        ));
      }
    }
    if existing_recovered_stack.is_none() {
      if recovered_creation.is_some() {
        // Persist before the RPC: an interrupted or lost response may already
        // have published files and must be reconciled after Core restarts.
        let updated = plans_collection()
          .update_one(
            doc! { "_id": &stored.id },
            doc! { "$set": { "recovered_stack_execution_started": true } },
          )
          .await?;
        if updated.matched_count != 1 {
          return Err(anyhow!("Restore plan expired before execution"));
        }
        stored.recovered_stack_execution_started = true;
      }
      let response = periphery
        .request(TransactionalVykarRestore {
          target,
          repository: repository_for_periphery(
            &settings.primary,
            false,
          )?,
          protected_repository_paths: core_local_repository_paths(
            &settings,
          )?,
          advanced: settings.advanced,
          hostname: format!("komodo-periphery-{}", server.id),
          snapshot_name: stored.plan.snapshot.clone(),
          selected_paths: stored.plan.selected_paths.clone(),
          publish: stored.publish.clone(),
          expected_preview: refreshed_preview,
          journal_id,
          volume_restore_plan_id: stored.id.clone(),
          create_volume_if_missing: stored.create_volume_if_missing,
          bind_path_mappings: stored.bind_path_mappings.clone(),
          bind_path_aliases: stored
            .snapshot_stack_path_aliases
            .clone(),
          defer_finalize: recovered_creation.is_some(),
        })
        .await?;
      if let Some(error) = response.critical_error {
        record_operational_alert(format!("Restore {} on {} ({}): {error}", stored.id, server.name, server.id));
        return finish_run(
          run.clone(),
          BackupRunState::Failed,
          error,
        )
        .await;
      }
      if !response.complete {
        if recovered_creation.is_some() && response.rolled_back {
          plans_collection()
            .update_one(
              doc! { "_id": &stored.id },
              doc! { "$set": { "recovered_stack_execution_started": false } },
            )
            .await?;
        }
        return finish_run(
          run.clone(),
          BackupRunState::Failed,
          if response.rolled_back {
            "Restore failed and was rolled back"
          } else {
            "Restore did not complete"
          },
        )
        .await;
      }
      if recovered_creation.is_some() != response.finalization_pending {
        return Err(anyhow!(
          "Periphery returned an inconsistent restore finalization state"
        ));
      }
    }
    if let Some((name, config, existing)) = recovered_creation {
      let creation = if let Some(existing) = existing {
        Ok(existing)
      } else {
        let mut info = Stack::default_info().await?;
        info.recovery_plan_id = Some(stored.id.clone());
        match resource::create::<Stack>(
          &name,
          config,
          Some(info),
          user,
        )
        .await
        {
          Ok(stack) => Ok(stack),
          Err(error) => {
            match Stack::coll().find_one(doc! { "name": &name }).await
            {
              Ok(Some(stack))
                if stack.info.recovery_plan_id.as_deref()
                  == Some(stored.id.as_str()) =>
              {
                warn!(
                  "Recovered Stack '{name}' was inserted but post-create bookkeeping failed: {:#}",
                  error.error
                );
                Ok(stack)
              }
              Ok(_) => Err(error.error),
              Err(check_error) => Err(
                anyhow::Error::new(check_error).context(format!(
                  "Recovered Stack creation failed and insertion could not be confirmed: {:#}",
                  error.error
                )),
              ),
            }
          }
        }
      };
      let recovered_stack = match creation {
        Ok(stack) => stack,
        Err(create_error) => {
          let rollback = periphery
            .request(FinalizeVykarRestore {
              journal_id: stored.id.clone(),
              commit: false,
              acknowledge: true,
            })
            .await;
          match rollback {
            Ok(response)
              if response.complete
                && response.rolled_back
                && response.critical_error.is_none() => {
                  plans_collection()
                    .update_one(
                      doc! { "_id": &stored.id },
                      doc! { "$set": { "recovered_stack_execution_started": false } },
                    )
                    .await?;
                }
            Ok(response) => {
              let message = response.critical_error.unwrap_or_else(|| {
                "Periphery did not confirm restore rollback".into()
              });
              record_operational_alert(format!("Restore {} on {} ({}): {message}", stored.id, server.name, server.id));
              return Err(create_error.context(message));
            }
            Err(rollback_error) => {
              let message = format!(
                "Recovered Stack creation failed and restore rollback could not be confirmed: {rollback_error:#}"
              );
              record_operational_alert(format!("Restore {} on {} ({}): {message}", stored.id, server.name, server.id));
              return Err(create_error.context(message));
            }
          }
          return Err(create_error);
        }
      };
      if stored.recovered_stack_id.as_deref()
        != Some(recovered_stack.id.as_str())
      {
        plans_collection()
          .update_one(
            doc! { "_id": &stored.id },
            doc! { "$set": { "recovered_stack_id": &recovered_stack.id } },
          )
          .await
          .context("Failed to persist recovered Stack identity")?;
        stored.recovered_stack_id = Some(recovered_stack.id.clone());
      }
      if let Err(error) = finalize_recovered_stack_saga(
        &mut stored,
        &server,
        &recovered_stack,
      )
      .await
      {
        let message = format!(
          "Recovered Stack finalization requires reconciliation: {error:#}"
        );
        record_operational_alert(format!("Restore {} on {} ({}): {message}", stored.id, server.name, server.id));
        return Err(anyhow!(message));
      }
    }
    // Keep the exclusive mutation barrier through recovered Stack creation
    // and finalization so a competing mutation cannot obscure saga state.
    drop(mutation_guard);
    if let Err(error) = plans_collection()
      .delete_one(doc! { "_id": &stored.id })
      .await
    {
      warn!(
        "Restore completed but its consumed plan could not be deleted: {error:#}"
      );
    }
    finish_run(
      run.clone(),
      BackupRunState::Complete,
      "Restore complete",
    )
    .await
  }
  .await;
  match operation {
    Ok(run) => Ok(run),
    Err(error) => {
      let message = format!("{error:#}");
      let _ = finish_run(run, BackupRunState::Failed, message).await;
      Err(error)
    }
  }
}

pub async fn restore_plan(
  plan_id: &str,
) -> anyhow::Result<BackupRestorePlan> {
  plans_collection()
    .find_one(doc! { "_id": plan_id })
    .await?
    .map(|stored| stored.plan)
    .context("Restore plan does not exist")
}

struct RemoveDirectoryOnDrop {
  path: PathBuf,
  armed: bool,
}

impl RemoveDirectoryOnDrop {
  fn new(path: PathBuf) -> Self {
    Self { path, armed: true }
  }

  fn disarm(&mut self) {
    self.armed = false;
  }
}

impl Drop for RemoveDirectoryOnDrop {
  fn drop(&mut self) {
    if self.armed {
      let _ = std::fs::remove_dir_all(&self.path);
    }
  }
}

fn purge_abandoned_core_staging() -> anyhow::Result<()> {
  for (path, recreate) in [
    CORE_STAGING_PATH,
    CORE_RECOVERY_STAGING_PATH,
    STACK_MANIFEST_STAGING_PATH,
  ]
  .into_iter()
  .map(|path| (path, true))
  .chain(LEGACY_CORE_STAGING_PATHS.map(|path| (path, false)))
  {
    let path = Path::new(path);
    match std::fs::remove_dir_all(path) {
      Ok(()) => {}
      Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
      Err(error) => {
        return Err(anyhow::Error::new(error).context(format!(
          "Failed to purge abandoned Core staging at {}",
          path.display()
        )));
      }
    }
    if recreate {
      std::fs::create_dir_all(path).with_context(|| {
        format!("Failed to create Core staging at {}", path.display())
      })?;
    }
  }
  Ok(())
}

async fn cleanup_expired_restore_plans() -> anyhow::Result<()> {
  plans_collection()
    .delete_many(doc! {
      "plan.expires_at": { "$lt": komodo_timestamp() },
      "$or": [
        { "recovered_stack_name": Bson::Null },
        { "recovered_stack_name": { "$exists": false } },
        { "recovered_stack_execution_started": false },
      ],
    })
    .await?;
  Ok(())
}

async fn reconcile_recovered_stack_restores() -> anyhow::Result<()> {
  let _operation = backup_operation_lock().lock().await;
  let _mutation = mutation_barrier().write().await;
  let collection = plans_collection();
  let plans = find_collect(
    &collection,
    doc! { "recovered_stack_execution_started": { "$ne": false } },
    None,
  )
  .await?;
  let mut errors = Vec::new();
  for mut stored in plans {
    let Some(name) = stored.recovered_stack_name.clone() else {
      continue;
    };
    let outcome = async {
      let server_id = stored
        .plan
        .destination_server_id
        .as_deref()
        .context("Recovered Stack plan has no destination Server")?;
      let server = resource::get::<Server>(server_id).await?;
      let existing = Stack::coll()
        .find_one(doc! { "name": &name })
        .await?;
      if let Some(stack) = existing
        && recovered_stack_belongs_to_plan(&stored, &stack)
      {
        finalize_recovered_stack_saga(
          &mut stored,
          &server,
          &stack,
        )
        .await?;
        return anyhow::Ok(());
      }
      if stored.recovered_stack_finalized
        || stored.recovered_stack_id.is_some()
      {
        return Err(anyhow!(
          "Recovered Stack recorded by plan '{}' is missing or no longer linked",
          stored.id
        ));
      }
      // No marked insert exists, so an interrupted publication must be
      // rolled back. A missing journal is an idempotent acknowledgement that
      // this plan never reached publication or was already reset.
      let rollback = periphery_client(&server)
        .await?
        .request(FinalizeVykarRestore {
          journal_id: stored.id.clone(),
          commit: false,
          acknowledge: true,
        })
        .await?;
      if !rollback.complete
        || !rollback.rolled_back
        || rollback.critical_error.is_some()
      {
        return Err(anyhow!(
          "Interrupted recovered Stack publication could not be rolled back: {}",
          rollback
            .critical_error
            .unwrap_or_else(|| "incomplete rollback".into())
        ));
      }
      if stored.plan.expires_at < komodo_timestamp() {
        plans_collection()
          .delete_one(doc! { "_id": &stored.id })
          .await?;
      } else {
        // A proven rollback no longer needs the destination online. A later
        // explicit execution will set this marker again before publishing.
        plans_collection()
          .update_one(
            doc! { "_id": &stored.id },
            doc! { "$set": { "recovered_stack_execution_started": false } },
          )
          .await?;
      }
      anyhow::Ok(())
    }
    .await;
    if let Err(error) = outcome {
      errors.push(format!("{}: {error:#}", stored.id));
    }
  }
  if errors.is_empty() {
    Ok(())
  } else {
    let message = format!(
      "Recovered Stack restore reconciliation failed: {}",
      errors.join("; ")
    );
    record_operational_alert(message.clone());
    Err(anyhow!(message))
  }
}

fn parse_core_recovery_database(database: &str) -> Option<&str> {
  let generated =
    database.strip_prefix(CORE_RECOVERY_DATABASE_PREFIX)?;
  let (namespace, recovery_id) = generated.split_once('_')?;
  (namespace.len() == 16
    && namespace
      .chars()
      .all(|character| character.is_ascii_hexdigit())
    && recovery_id.len() == 12
    && recovery_id
      .chars()
      .all(|character| character.is_ascii_hexdigit()))
  .then_some(namespace)
}

fn core_recovery_database_namespace(database: &str) -> String {
  parse_core_recovery_database(database)
    .map(str::to_string)
    .unwrap_or_else(|| {
      hex::encode(Sha256::digest(database.as_bytes()))[..16]
        .to_string()
    })
}

fn core_recovery_database_name(current_database: &str) -> String {
  format!(
    "{CORE_RECOVERY_DATABASE_PREFIX}{}_{}",
    core_recovery_database_namespace(current_database),
    &Uuid::new_v4().simple().to_string()[..12]
  )
}

fn is_managed_core_recovery_database(
  current_database: &str,
  candidate: &str,
) -> bool {
  if candidate == current_database {
    return false;
  }
  let current_namespace =
    core_recovery_database_namespace(current_database);
  parse_core_recovery_database(candidate)
    .is_some_and(|namespace| namespace == current_namespace.as_str())
}

fn previous_core_recovery_database() -> anyhow::Result<Option<String>>
{
  let bytes = match read_core_recovery_activation() {
    Ok(Some(bytes)) => bytes,
    Ok(None) => {
      return Ok(None);
    }
    Err(error) => {
      return Err(error)
        .context("Failed to read Core recovery activation record");
    }
  };
  let activation: CoreRecoveryActivation =
    serde_json::from_slice(&bytes)
      .context("Invalid Core recovery activation record")?;
  Ok(activation.previous_database)
}

fn core_recovery_database_is_orphaned(
  current_database: &str,
  candidate: &str,
  active_databases: &HashSet<String>,
  previous_database: Option<&str>,
) -> bool {
  core_recovery_database_can_be_dropped(
    current_database,
    candidate,
    previous_database,
  ) && !active_databases.contains(candidate)
}

fn core_recovery_database_can_be_dropped(
  current_database: &str,
  candidate: &str,
  previous_database: Option<&str>,
) -> bool {
  is_managed_core_recovery_database(current_database, candidate)
    && previous_database != Some(candidate)
}

async fn reconcile_core_recovery_state_inner() -> anyhow::Result<()> {
  let current_database = db_client().db.name();
  let previous_database = previous_core_recovery_database()?;
  let expired = find_collect(
    &core_recovery_collection(),
    doc! { "plan.expires_at": { "$lt": komodo_timestamp() } },
    None,
  )
  .await?;
  for stored in expired {
    if core_recovery_database_can_be_dropped(
      current_database,
      &stored.plan.validation_database,
      previous_database.as_deref(),
    ) {
      db_client()
        .db
        .client()
        .database(&stored.plan.validation_database)
        .drop()
        .await
        .with_context(|| {
          format!(
            "Failed to drop expired Core recovery database '{}'",
            stored.plan.validation_database
          )
        })?;
    }
    core_recovery_collection()
      .delete_one(doc! { "_id": &stored.id })
      .await?;
  }
  let active_databases =
    find_collect(&core_recovery_collection(), None, None)
      .await?
      .into_iter()
      .map(|stored| stored.plan.validation_database)
      .collect::<HashSet<_>>();
  let client = db_client().db.client();
  for database_name in client.list_database_names().await? {
    if core_recovery_database_is_orphaned(
      current_database,
      &database_name,
      &active_databases,
      previous_database.as_deref(),
    ) {
      client
        .database(&database_name)
        .drop()
        .await
        .with_context(|| {
          format!(
            "Failed to drop orphaned Core recovery database '{database_name}'"
          )
        })?;
    }
  }
  Ok(())
}

async fn reconcile_core_recovery_state() -> anyhow::Result<()> {
  let _operation = core_recovery_operation_lock().lock().await;
  reconcile_core_recovery_state_inner().await
}

fn historical_restore_finalization_update(
  plan_id: &str,
  stack: &Stack,
) -> Option<database::bson::Document> {
  // The marked Stack is inserted only after Periphery publishes every root.
  // In a restored Core database it may outlive an already acknowledged receipt.
  (stack.info.recovery_plan_id.as_deref() == Some(plan_id)).then(
    || {
      doc! { "$set": {
        "recovered_stack_execution_started": true,
        "recovered_stack_finalized": true,
        "recovered_stack_id": &stack.id,
      } }
    },
  )
}

async fn normalize_historical_restore_sagas(
  validation: &database::mungos::mongodb::Database,
) -> anyhow::Result<()> {
  let plans =
    validation.collection::<StoredRestorePlan>(PLANS_COLLECTION);
  for stored in find_collect(&plans, None, None).await? {
    if stored.recovered_stack_name.is_none() {
      continue;
    }
    let stack = validation
      .collection::<Stack>("Stack")
      .find_one(doc! { "info.recovery_plan_id": &stored.id })
      .await?;
    if let Some(stack) = stack
      && let Some(update) =
        historical_restore_finalization_update(&stored.id, &stack)
    {
      plans.update_one(doc! { "_id": &stored.id }, update).await?;
    }
  }
  Ok(())
}

pub async fn plan_core_recovery(
  snapshot_name: &str,
  created_by: String,
) -> anyhow::Result<CoreRecoveryPlan> {
  let _operation = core_recovery_operation_lock().lock().await;
  reconcile_core_recovery_state_inner().await?;
  let _repository_roles =
    repository_role_barrier().clone().read_owned().await;
  let snapshot = list_snapshots()
    .await?
    .0
    .into_iter()
    .find(|snapshot| snapshot.name == snapshot_name)
    .context(
      "Core snapshot does not exist in the active primary repository",
    )?;
  if snapshot.target != BackupTarget::Core {
    return Err(anyhow!("Selected snapshot is not a Core backup"));
  }
  if snapshot.partial {
    return Err(anyhow!(
      "Partial Core snapshots cannot be recovered"
    ));
  }

  let settings = get_settings().await?;
  let repository = settings.primary.clone();
  let staging = PathBuf::from(CORE_RECOVERY_STAGING_PATH)
    .join(Uuid::new_v4().to_string());
  tokio::fs::create_dir_all(&staging).await?;
  let _staging_cleanup = RemoveDirectoryOnDrop::new(staging.clone());
  let worker_staging = staging.clone();
  let snapshot_for_worker = snapshot.name.clone();
  let settings_for_worker = settings.clone();
  tokio::task::spawn_blocking(move || {
    core_repository(&repository, &settings_for_worker)?.restore(
      &snapshot_for_worker,
      &worker_staging,
      &[],
    )
  })
  .await
  .context("Core recovery restore worker failed")??;

  let manifest_path =
    find_file_named(&staging, "komodo-core-manifest.json")
      .context("Core snapshot manifest is missing")?;
  let authenticated_root = manifest_path
    .parent()
    .context("Core manifest has no export root")?
    .to_path_buf();
  let (_, expected_digest, expected_created_at) =
    crypto::authenticate_core_source_label(
      &snapshot.source_label,
      &snapshot.hostname,
      &snapshot.name,
    )?;
  let digest_root = authenticated_root.clone();
  let actual_digest = tokio::task::spawn_blocking(move || {
    core_export_digest(&digest_root)
  })
  .await
  .context("Core recovery digest worker failed")??;
  if actual_digest != expected_digest {
    return Err(anyhow!(
      "Core snapshot contents do not match their Core-authorized digest; recovery blocked"
    ));
  }
  let manifest: serde_json::Value =
    serde_json::from_slice(&tokio::fs::read(&manifest_path).await?)?;
  if manifest
    .get("created_at")
    .and_then(serde_json::Value::as_i64)
    != Some(expected_created_at)
  {
    return Err(anyhow!(
      "Core manifest creation time does not match its authorization"
    ));
  }
  let backup_schema = manifest
    .get("schema")
    .and_then(serde_json::Value::as_str)
    .context("Core snapshot manifest has no schema")?
    .to_string();
  if backup_schema != "komodo.core-export/v1" {
    return Err(anyhow!(
      "Unsupported Core backup schema '{backup_schema}'"
    ));
  }
  let backup_version = manifest
    .get("version")
    .and_then(serde_json::Value::as_str)
    .context("Core snapshot manifest has no Komodo version")?
    .to_string();
  if backup_version.split('.').next()
    != komodo_build_info::version().split('.').next()
  {
    return Err(anyhow!(
      "Core backup major version {backup_version} is incompatible with {}",
      komodo_build_info::version()
    ));
  }
  let recovered_core_instance_id = manifest
    .get("core_instance_id")
    .and_then(serde_json::Value::as_str)
    .context("Core snapshot manifest has no stable Core identity")?
    .to_string();
  if recovered_core_instance_id.len() != 32
    || !recovered_core_instance_id
      .chars()
      .all(|character| character.is_ascii_hexdigit())
  {
    return Err(anyhow!(
      "Core snapshot manifest contains an invalid Core identity"
    ));
  }

  let (backup_root, restore_folder) =
    find_core_restore_layout(&authenticated_root)?;
  let current_database = db_client().db.name().to_string();
  let validation_database =
    core_recovery_database_name(&current_database);
  let validation =
    db_client().db.client().database(&validation_database);
  let result = async {
    database::utils::restore(
      &validation,
      &backup_root,
      Some(Path::new(&restore_folder)),
    )
    .await
    .context("Failed to restore the Core validation database")?;
    normalize_historical_restore_sagas(&validation).await
      .context("Failed to normalize historical recovered-Stack receipts")?;
    // Historical recovery plans belong to the old database, not this new
    // activation. Never resurrect their expiry/activation side effects.
    validation.collection::<StoredCoreRecoveryPlan>(CORE_RECOVERY_COLLECTION)
      .delete_many(doc! {})
      .await
      .context("Failed to discard historical Core recovery plans")?;
    // Repository credentials are deliberately excluded from Core snapshots.
    // Carry forward the freshly configured, locally sealed repository settings
    // so recovery can still access its primary after the database switch.
    if let Some(active_settings) = settings_collection()
      .find_one(doc! { "_id": SETTINGS_ID })
      .await?
    {
      validation
        .collection::<SealedBackupSettings>(SETTINGS_COLLECTION)
        .update_one(
          doc! { "_id": SETTINGS_ID },
          doc! { "$set": to_document(&active_settings)? },
        )
        .with_options(UpdateOptions::builder().upsert(true).build())
        .await
        .context(
          "Failed to carry active repository settings into recovery database",
        )?;
    }
    validation
      .collection::<RepositoryHealthRecord>(HEALTH_COLLECTION)
      .delete_many(doc! {})
      .await
      .context(
        "Failed to invalidate repository health in recovery database",
      )?;
    let enabled_admins = validation
      .collection::<komodo_client::entities::user::User>("User")
      .count_documents(doc! { "enabled": true, "admin": true })
      .await?;
    if enabled_admins == 0 {
      return Err(anyhow!(
        "Recovered Core database has no enabled administrator; activation blocked"
      ));
    }

    let plan = CoreRecoveryPlan {
      id: Uuid::new_v4().to_string(),
      snapshot: snapshot.name,
      current_database,
      validation_database,
      backup_schema,
      backup_version,
      expires_at: komodo_timestamp() + 30 * 60 * 1000,
    };
    core_recovery_collection()
      .insert_one(StoredCoreRecoveryPlan {
        id: plan.id.clone(),
        created_by,
        recovered_core_instance_id,
        plan: plan.clone(),
      })
      .await?;
    Ok(plan)
  }
  .await;
  if result.is_err() {
    validation.drop().await.ok();
  }
  result
}

pub async fn execute_core_recovery(
  plan_id: &str,
  user_id: &str,
) -> anyhow::Result<BackupRun> {
  let _operation = core_recovery_operation_lock().lock().await;
  let backup_operation = backup_operation_lock().lock().await;
  let stored = core_recovery_collection()
    .find_one(doc! { "_id": plan_id, "created_by": user_id })
    .await?
    .context("Core recovery plan does not exist")?;
  if stored.plan.expires_at < komodo_timestamp() {
    let previous_database = previous_core_recovery_database()?;
    if core_recovery_database_can_be_dropped(
      db_client().db.name(),
      &stored.plan.validation_database,
      previous_database.as_deref(),
    ) {
      db_client()
        .db
        .client()
        .database(&stored.plan.validation_database)
        .drop()
        .await?;
    }
    core_recovery_collection()
      .delete_one(doc! { "_id": &stored.id })
      .await?;
    return Err(anyhow!("Core recovery plan has expired"));
  }
  if stored.plan.current_database != db_client().db.name() {
    return Err(anyhow!(
      "Core recovery plan belongs to a previous database; create a fresh plan"
    ));
  }
  let validation = db_client()
    .db
    .client()
    .database(&stored.plan.validation_database);
  let enabled_admins = validation
    .collection::<komodo_client::entities::user::User>("User")
    .count_documents(doc! { "enabled": true, "admin": true })
    .await?;
  if enabled_admins == 0 {
    return Err(anyhow!(
      "Validation database no longer has an enabled administrator"
    ));
  }
  // Hold the same exclusive barrier used by every resource mutation through
  // publication of the durable activation pointer and until process exit.
  // Otherwise a mutation can commit to the old database during the delayed
  // restart and disappear from the recovered database.
  let mutation = mutation_barrier().clone().write_owned().await;
  persist_core_recovery_activation(
    &stored.plan.validation_database,
    &stored.recovered_core_instance_id,
    Some(&stored.plan.current_database),
  )?;
  let delete_result = core_recovery_collection()
    .delete_one(doc! { "_id": &stored.id })
    .await;
  // Once the durable pointer is published, restart even if recording the
  // final audit result encounters a transient database error.
  schedule_core_restart();
  // The durable activation pointer is now authoritative. Keep new backup and
  // restore operations blocked until the process restarts into that database.
  std::mem::forget(backup_operation);
  std::mem::forget(mutation);
  delete_result?;
  let run = new_non_cancellable_run(
    Some(BackupTarget::Core),
    "Core recovery activating",
  )
  .await?;
  let run = finish_run(
    run,
    BackupRunState::Complete,
    format!(
      "Core recovery validated; restarting into database '{}' (previous database '{}' retained)",
      stored.plan.validation_database, stored.plan.current_database
    ),
  )
  .await?;
  Ok(run)
}

fn find_file_named(root: &Path, name: &str) -> Option<PathBuf> {
  let entries = std::fs::read_dir(root).ok()?;
  for entry in entries.flatten() {
    let path = entry.path();
    let file_type = entry.file_type().ok()?;
    if file_type.is_file() && entry.file_name() == name {
      return Some(path);
    }
    if file_type.is_dir()
      && let Some(found) = find_file_named(&path, name)
    {
      return Some(found);
    }
  }
  None
}

fn find_core_restore_layout(
  root: &Path,
) -> anyhow::Result<(PathBuf, String)> {
  fn find_dated(root: &Path) -> Option<PathBuf> {
    for entry in std::fs::read_dir(root).ok()?.flatten() {
      let path = entry.path();
      let file_type = entry.file_type().ok()?;
      if file_type.is_dir() {
        if std::fs::read_dir(&path).ok()?.flatten().any(|child| {
          child
            .path()
            .extension()
            .is_some_and(|extension| extension == "gz")
            && child.file_name() != "Stats.gz"
        }) {
          return Some(path);
        }
        if let Some(found) = find_dated(&path) {
          return Some(found);
        }
      }
    }
    None
  }
  let dated = find_dated(root)
    .context("Core snapshot contains no database export")?;
  let backup_root = dated
    .parent()
    .context("Core database export has no parent")?
    .to_path_buf();
  let restore_folder = dated
    .file_name()
    .and_then(|name| name.to_str())
    .context("Core database export path is not valid UTF-8")?
    .to_string();
  Ok((backup_root, restore_folder))
}

pub async fn verify(
  mirror: bool,
  full: bool,
) -> anyhow::Result<BackupRun> {
  let _operation = backup_operation_lock().lock().await;
  let _repository_roles =
    repository_role_barrier().clone().read_owned().await;
  let settings = get_settings().await?;
  verify_repository(settings, mirror, full).await
}

async fn verify_repository(
  settings: BackupSettings,
  mirror: bool,
  full: bool,
) -> anyhow::Result<BackupRun> {
  let repository = if mirror {
    settings
      .mirror
      .clone()
      .context("Mirror is not configured")?
  } else {
    settings.primary.clone()
  };
  let health_id = if mirror { "mirror" } else { "primary" };
  let run =
    new_non_cancellable_run(None, "Repository verification running")
      .await?;
  let operation = async {
    let settings_for_worker = settings.clone();
    let result = tokio::task::spawn_blocking(move || {
      core_repository(&repository, &settings_for_worker)?.verify(
        full,
        settings_for_worker.advanced.verify_sample_percent,
      )
    })
    .await
    .context("Vykar verification worker failed")??;
    if result.errors.is_empty() {
      record_repository_verification(health_id, true, full).await?;
      finish_run(
        run.clone(),
        BackupRunState::Complete,
        "Repository verified",
      )
      .await
    } else {
      record_repository_verification(health_id, false, full).await?;
      finish_run(
        run.clone(),
        BackupRunState::Failed,
        format!("Integrity errors: {}", result.errors.join("; ")),
      )
      .await
    }
  }
  .await;
  match operation {
    Ok(run) => Ok(run),
    Err(error) => {
      let _ =
        record_repository_verification(health_id, false, full).await;
      let message = format!("{error:#}");
      let _ = finish_run(run, BackupRunState::Failed, message).await;
      Err(error)
    }
  }
}

fn mirror_copy_is_sufficient(
  primary_partial: bool,
  mirror_partial: Option<bool>,
) -> bool {
  match mirror_partial {
    None => false,
    Some(false) => true,
    Some(true) => primary_partial,
  }
}

pub async fn promote_mirror(
  allow_primary_unavailable: bool,
) -> anyhow::Result<BackupSettings> {
  let backup_operation = backup_operation_lock().lock().await;
  // Keep the exclusive role barrier from the start of mandatory verification
  // through the settings swap. No unverified mirror write can land in between.
  let repository_roles =
    repository_role_barrier().clone().write_owned().await;
  let mut settings = get_settings().await?;
  let restart_required =
    matches!(
      &settings.primary.backend,
      BackupRepositoryBackend::CoreLocal { .. }
    ) || settings.mirror.as_ref().is_some_and(|repository| {
      matches!(
        &repository.backend,
        BackupRepositoryBackend::CoreLocal { .. }
      )
    });
  let verification =
    verify_repository(settings.clone(), true, true).await?;
  if verification.state != BackupRunState::Complete {
    return Err(anyhow!(
      "Mirror verification failed; promotion blocked"
    ));
  }
  let primary = settings.primary.clone();
  let mirror_for_inventory = settings
    .mirror
    .clone()
    .context("Mirror is not configured")?;
  let inventory_settings = settings.clone();
  let missing = tokio::task::spawn_blocking(move || {
    let mirror = core_repository(
      &mirror_for_inventory,
      &inventory_settings,
    )?
    .list_snapshots()?;
    if mirror.hidden > 0 {
      return Err(anyhow!(
        "Promotion blocked because the mirror inventory is incomplete"
      ));
    }
    let primary = match core_repository(&primary, &inventory_settings)
      .and_then(|repository| repository.list_snapshots())
    {
      Ok(primary) => primary,
      Err(error) if allow_primary_unavailable => {
        warn!(
          "Promoting a fully verified mirror without comparing the unavailable primary inventory: {error:#}"
        );
        return Ok::<_, anyhow::Error>(Vec::new());
      }
      Err(error) => {
        return Err(error.context(
          "Primary inventory is unavailable; retry with explicit disaster-recovery acknowledgement only if the old primary cannot be recovered",
        ));
      }
    };
    if primary.hidden > 0 {
      return Err(anyhow!(
        "Promotion blocked because the primary inventory is incomplete"
      ));
    }
    let mirror_snapshots = mirror
      .snapshots
      .into_iter()
      .map(|snapshot| (snapshot.name, snapshot.partial))
      .collect::<HashMap<_, _>>();
    Ok::<_, anyhow::Error>(
      primary
        .snapshots
        .into_iter()
        .filter(|snapshot| {
          !mirror_copy_is_sufficient(
            snapshot.partial,
            mirror_snapshots.get(&snapshot.name).copied(),
          )
        })
        .map(|snapshot| snapshot.name)
        .collect::<Vec<_>>(),
    )
  })
  .await
  .context("Mirror comparison worker failed")??;
  if !missing.is_empty() {
    return Err(anyhow!(
      "Mirror promotion blocked because it is missing or has incomplete copies of {} primary snapshot(s)",
      missing.len()
    ));
  }
  let mirror =
    settings.mirror.take().context("Mirror is not configured")?;
  settings.mirror =
    Some(std::mem::replace(&mut settings.primary, mirror));
  let saved = save_settings_after_promotion(settings).await?;
  if restart_required {
    schedule_core_restart();
    // Embedded REST handlers capture their data directories at startup. Keep
    // both operation gates closed until restart re-registers the promoted
    // primary and mirror routes with their new role paths.
    std::mem::forget(repository_roles);
    std::mem::forget(backup_operation);
  }
  Ok(saved)
}

pub async fn cancel_run(run_id: &str) -> anyhow::Result<BackupRun> {
  let run = runs_collection()
    .find_one(doc! { "id": run_id })
    .await?
    .context("Backup run does not exist")?;
  if !matches!(
    run.state,
    BackupRunState::Queued | BackupRunState::Running
  ) {
    return Err(anyhow!(
      "Only an active backup run can be cancelled"
    ));
  }
  if !run.cancellable
    || non_cancellable_runs().lock().unwrap().contains(run_id)
  {
    return Err(anyhow!(
      "This backup operation cannot be cancelled once it has started"
    ));
  }
  if let Some(token) = cancellation_token(run_id) {
    token.store(true, Ordering::SeqCst);
  }
  if *fleet_generation().read().unwrap() == run_id {
    fleet_generation().write().unwrap().clear();
  }
  let repository_retry =
    core_repository_retries().lock().unwrap().remove(run_id);
  if let Some(retry) = repository_retry {
    let _ = tokio::fs::remove_dir_all(retry.staging).await;
  }
  let servers = find_collect(&db_client().servers, None, None)
    .await
    .unwrap_or_default();
  futures_util::future::join_all(servers.iter().map(
    |server| async move {
      if let Ok(client) = periphery_client(server).await {
        let _ = client
          .request(CancelVykarOperation {
            operation_id: run_id.to_string(),
          })
          .await;
      }
    },
  ))
  .await;
  // The owner holds this lock for the complete backup operation. Waiting here
  // guarantees Core export/repository workers and the initial fleet batch have
  // observed cancellation before the audit record becomes Cancelled.
  let _operation = backup_operation_lock().lock().await;
  let current = runs_collection()
    .find_one(doc! { "id": run_id })
    .await?
    .context("Backup run disappeared while cancelling")?;
  if !matches!(
    current.state,
    BackupRunState::Queued | BackupRunState::Running
  ) {
    return Ok(current);
  }
  finish_run(run, BackupRunState::Cancelled, "Cancellation requested")
    .await
}

fn embedded_repository_paths() -> &'static RwLock<[Option<PathBuf>; 2]>
{
  static PATHS: OnceLock<RwLock<[Option<PathBuf>; 2]>> =
    OnceLock::new();
  PATHS.get_or_init(Default::default)
}

pub fn embedded_vykar_router(
  path: &Path,
  mirror: bool,
) -> anyhow::Result<axum::Router> {
  std::fs::create_dir_all(path)?;
  embedded_repository_paths().write().unwrap()[usize::from(mirror)] =
    Some(path.to_path_buf());
  let config = vykar_server::config::ServerSection {
    listen: String::new(),
    data_dir: path.to_string_lossy().into_owned(),
    token: crypto::embedded_server_token()?,
    // Periphery only needs to append backup data. Retention, pruning, and
    // deletion stay on Core so a compromised worker cannot destroy history.
    append_only: true,
    log_format: "json".into(),
  };
  Ok(vykar_server::handlers::router(
    vykar_server::state::AppState::new(config, None),
  ))
}

fn next_scheduled_run() -> anyhow::Result<i64> {
  // The background loop refreshes this after settings changes and runs.
  Ok(*next_run_cache().read().unwrap())
}

fn next_run_cache() -> &'static RwLock<i64> {
  static NEXT: OnceLock<RwLock<i64>> = OnceLock::new();
  NEXT.get_or_init(Default::default)
}

fn scheduler_revision() -> &'static tokio::sync::watch::Sender<u64> {
  static REVISION: OnceLock<tokio::sync::watch::Sender<u64>> =
    OnceLock::new();
  REVISION.get_or_init(|| tokio::sync::watch::channel(0).0)
}

fn notify_scheduler() {
  scheduler_revision().send_modify(|revision| {
    *revision = revision.wrapping_add(1);
  });
}

fn compute_next_run(
  settings: &BackupSettings,
) -> anyhow::Result<i64> {
  use croner::parser::{CronParser, Seconds};
  let expression =
    if settings.schedule.split_whitespace().count() == 5 {
      format!("0 {}", settings.schedule)
    } else {
      english_to_cron::str_cron_syntax(&settings.schedule)
        .map_err(|error| {
          anyhow!("Invalid English schedule: {error:?}")
        })?
        .split(' ')
        .take(6)
        .collect::<Vec<_>>()
        .join(" ")
    };
  let cron = CronParser::builder()
    .seconds(Seconds::Required)
    .dom_and_dow(true)
    .build()
    .parse(&expression)?;
  let timezone: chrono_tz::Tz = settings.timezone.parse()?;
  Ok(
    cron
      .find_next_occurrence(
        &chrono::Utc::now().with_timezone(&timezone),
        false,
      )?
      .timestamp_millis(),
  )
}

pub fn spawn_scheduler() {
  let _ = maintenance_sender();
  if let Err(error) = purge_abandoned_core_staging() {
    error!("Failed to purge abandoned Core staging: {error:#}");
  }
  tokio::spawn(async {
    loop {
      if let Err(error) = reconcile_recovered_stack_restores().await {
        error!(
          "Failed to reconcile recovered Stack restores: {error:#}"
        );
      }
      if let Err(error) = cleanup_expired_restore_plans().await {
        error!("Failed to clean expired restore plans: {error:#}");
      }
      if let Err(error) = reconcile_core_recovery_state().await {
        error!(
          "Failed to clean expired Core recovery plans: {error:#}"
        );
      }
      tokio::time::sleep(std::time::Duration::from_secs(300)).await;
    }
  });
  tokio::spawn(async {
    let mut revision = scheduler_revision().subscribe();
    loop {
      let settings = match get_settings().await {
        Ok(settings) => settings,
        Err(error) => {
          error!("Failed to load backup schedule: {error:#}");
          tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {}
            _ = revision.changed() => {}
          }
          continue;
        }
      };
      if !settings.enabled {
        *next_run_cache().write().unwrap() = 0;
        tokio::select! {
          _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {}
          _ = revision.changed() => {}
        }
        continue;
      }
      let next = match compute_next_run(&settings) {
        Ok(next) => next,
        Err(error) => {
          error!("Invalid backup schedule: {error:#}");
          tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {}
            _ = revision.changed() => {}
          }
          continue;
        }
      };
      *next_run_cache().write().unwrap() = next;
      let delay = (next - komodo_timestamp()).max(0) as u64;
      tokio::select! {
        _ = tokio::time::sleep(std::time::Duration::from_millis(delay)) => {}
        _ = revision.changed() => continue,
      }
      let current = match get_settings().await {
        Ok(current)
          if current.enabled
            && current.updated_at == settings.updated_at =>
        {
          current
        }
        Ok(_) => continue,
        Err(error) => {
          error!(
            "Failed to reload backup schedule before run: {error:#}"
          );
          continue;
        }
      };
      // The reloaded settings check above prevents a stale timer from running
      // after disable or reschedule. `current` is intentionally kept alive so
      // this validation cannot be optimized into the earlier snapshot.
      drop(current);
      if let Err(error) = run_scheduled_backup().await {
        error!("Scheduled fleet backup failed: {error:#}");
      }
    }
  });
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn operational_alerts_survive_a_fresh_read() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("operational-alert.json");
    let mut messages = read_operational_alert(&path).unwrap();
    assert!(messages.is_empty());
    append_operational_alert(
      &mut messages,
      "Node A: containers remain stopped".into(),
    );
    persist_operational_alert(&path, &messages).unwrap();
    let mut messages = read_operational_alert(&path).unwrap();
    append_operational_alert(
      &mut messages,
      "Node B: restore needs reconciliation".into(),
    );
    append_operational_alert(
      &mut messages,
      "Node A: containers remain stopped".into(),
    );
    persist_operational_alert(&path, &messages).unwrap();
    assert_eq!(
      read_operational_alert(&path).unwrap(),
      vec![
        "Node A: containers remain stopped",
        "Node B: restore needs reconciliation",
      ]
    );
  }

  #[test]
  fn legacy_operational_alert_is_preserved() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("alert.json");
    std::fs::write(&path, br#"{"message":"Earlier incident"}"#)
      .unwrap();
    let mut messages = read_operational_alert(&path).unwrap();
    append_operational_alert(&mut messages, "New incident".into());
    persist_operational_alert(&path, &messages).unwrap();
    assert_eq!(
      read_operational_alert(&path).unwrap(),
      vec!["Earlier incident", "New incident"]
    );
  }

  #[test]
  fn repository_health_cache_expires_and_tracks_settings_changes() {
    assert!(repository_health_cache_is_fresh(1_000, 500, 2_000));
    assert!(!repository_health_cache_is_fresh(1_000, 1_001, 2_000));
    assert!(!repository_health_cache_is_fresh(1_000, 500, 301_000));
    assert!(!repository_health_cache_is_fresh(0, 0, 1_000));
    assert!(!repository_health_cache_is_fresh(2_000, 500, 1_000));
  }

  #[test]
  fn anonymous_volume_eligibility_requires_explicit_opt_in() {
    let mut volume = VolumeListItem {
      driver: "local".into(),
      scope: VolumeScopeEnum::Local,
      anonymous: true,
      ..Default::default()
    };
    assert!(!volume_is_backup_eligible(&volume, false));
    assert!(volume_is_backup_eligible(&volume, true));
    volume.anonymous = false;
    assert!(volume_is_backup_eligible(&volume, false));
    volume.driver = "remote-plugin".into();
    assert!(!volume_is_backup_eligible(&volume, true));
  }

  #[test]
  fn maintenance_alerts_do_not_replace_operational_alerts() {
    let mut alerts = CriticalAlerts {
      operational: vec!["Containers remain stopped".into()],
      maintenance: Some("Repository prune failed".into()),
      ..Default::default()
    };
    let current = alerts.current().unwrap();
    assert!(current.contains("Containers remain stopped"));
    assert!(current.contains("Repository prune failed"));

    alerts.maintenance = None;
    assert_eq!(
      alerts.current().as_deref(),
      Some("Containers remain stopped")
    );
  }

  #[test]
  fn blank_secret_preserves_sealed_value() {
    let mut proposed = BackupSecret::default();
    let existing = BackupSecret {
      value: "sealed-plaintext".into(),
      configured: false,
    };
    preserve_secret(&mut proposed, &existing);
    assert_eq!(proposed.value, "sealed-plaintext");
  }

  #[test]
  fn initialized_repository_passphrase_cannot_change_in_place() {
    let repository = |passphrase: &str| BackupRepository {
      backend: BackupRepositoryBackend::CoreLocal {
        path: "/backups/repository".into(),
      },
      passphrase: BackupSecret {
        value: passphrase.into(),
        configured: false,
      },
      ..Default::default()
    };
    let existing = repository("original");
    let mut proposed = repository("replacement");
    assert!(
      merge_repository_secrets(&mut proposed, &existing, true, true)
        .is_err()
    );

    let mut redacted = repository("");
    merge_repository_secrets(&mut redacted, &existing, true, true)
      .unwrap();
    assert_eq!(redacted.passphrase.value, "original");
  }

  #[test]
  fn uninitialized_repository_passphrase_can_be_corrected() {
    let repository = |passphrase: &str| BackupRepository {
      backend: BackupRepositoryBackend::CoreLocal {
        path: "/backups/repository".into(),
      },
      passphrase: BackupSecret {
        value: passphrase.into(),
        configured: false,
      },
      ..Default::default()
    };
    let existing = repository("mistyped");
    let mut proposed = repository("corrected");
    merge_repository_secrets(&mut proposed, &existing, true, false)
      .unwrap();
    assert_eq!(proposed.passphrase.value, "corrected");
  }

  #[test]
  fn external_worker_receives_only_distinct_scoped_credentials() {
    let repository = BackupRepository {
      backend: BackupRepositoryBackend::Rest {
        url: "https://backup.example".into(),
        access_token: BackupSecret {
          value: "authoritative-token".into(),
          configured: false,
        },
        worker_access_token: BackupSecret {
          value: "append-only-worker-token".into(),
          configured: false,
        },
        allow_insecure_http: false,
      },
      passphrase: BackupSecret {
        value: "repository-passphrase".into(),
        configured: false,
      },
      ..Default::default()
    };
    let worker =
      repository_for_periphery(&repository, false).unwrap();
    let BackupRepositoryBackend::Rest {
      access_token,
      worker_access_token,
      ..
    } = worker.backend
    else {
      panic!("wrong backend")
    };
    assert_eq!(access_token.value, "append-only-worker-token");
    assert!(worker_access_token.value.is_empty());

    let mut unsafe_repository = repository;
    let BackupRepositoryBackend::Rest {
      worker_access_token,
      ..
    } = &mut unsafe_repository.backend
    else {
      unreachable!()
    };
    worker_access_token.value = "authoritative-token".into();
    assert!(
      repository_for_periphery(&unsafe_repository, false).is_err()
    );
  }

  #[test]
  fn repository_locations_normalize_equivalent_paths_and_urls() {
    let local = |path: &str| BackupRepository {
      backend: BackupRepositoryBackend::CoreLocal {
        path: path.into(),
      },
      ..Default::default()
    };
    assert_eq!(
      repository_location(&local("/backups/repo")),
      repository_location(&local("/backups/./repo/"))
    );

    let rest = |url: &str| BackupRepository {
      backend: BackupRepositoryBackend::Rest {
        url: url.into(),
        access_token: Default::default(),
        worker_access_token: Default::default(),
        allow_insecure_http: false,
      },
      ..Default::default()
    };
    assert_eq!(
      repository_location(&rest("https://backup.example/repo")),
      repository_location(&rest("https://backup.example/repo/"))
    );

    let root = tempfile::tempdir().unwrap();
    let actual = root.path().join("actual");
    std::fs::create_dir(&actual).unwrap();
    let alias = root.path().join("alias");
    std::os::unix::fs::symlink(&actual, &alias).unwrap();
    assert!(
      repositories_share_location(
        &local(&actual.to_string_lossy()),
        &local(&alias.to_string_lossy())
      )
      .unwrap()
    );
    assert!(
      repositories_overlap(
        &local(&actual.to_string_lossy()),
        &local(&actual.join("nested").to_string_lossy())
      )
      .unwrap()
    );
    assert!(
      !repositories_share_location(
        &local(&actual.to_string_lossy()),
        &local(&actual.join("nested").to_string_lossy()),
      )
      .unwrap()
    );
    assert!(
      !repositories_share_location(
        &local(&actual.join("nested").to_string_lossy()),
        &local(&actual.to_string_lossy()),
      )
      .unwrap()
    );
  }

  #[test]
  fn core_local_repositories_reject_internal_work_directories() {
    for path in [
      CORE_PRIVATE_PATH,
      CORE_STAGING_PATH,
      CORE_CACHE_PATH,
      CORE_RECOVERY_STAGING_PATH,
      STACK_MANIFEST_STAGING_PATH,
      "/core-secrets/.komodo-core-staging/repository",
      "/data/backups/.komodo-core-staging/repository",
      "/data/backups",
      "/data",
    ] {
      let repository = BackupRepository {
        name: "reserved".into(),
        backend: BackupRepositoryBackend::CoreLocal {
          path: path.into(),
        },
        ..Default::default()
      };
      assert!(validate_repository_definition(&repository).is_err());
    }
  }

  #[test]
  fn core_sensitive_work_stays_outside_shared_data() {
    for path in [
      CORE_STAGING_PATH,
      CORE_RECOVERY_STAGING_PATH,
      STACK_MANIFEST_STAGING_PATH,
    ] {
      assert!(Path::new(path).starts_with(CORE_PRIVATE_PATH));
      assert!(!paths_overlap(Path::new(path), Path::new("/data")));
    }
    assert!(Path::new(CORE_CACHE_PATH).starts_with("/data"));
  }

  #[test]
  fn exact_core_replays_cannot_advance_retention_order() {
    let replay = BackupSnapshot {
      name: "old-replayed-export".into(),
      created_at: 90_000,
      ..Default::default()
    };
    let recent = BackupSnapshot {
      name: "recent".into(),
      created_at: 20,
      ..Default::default()
    };
    let newest = BackupSnapshot {
      name: "newest".into(),
      created_at: 30,
      ..Default::default()
    };
    // The replay has fresh repository metadata, but its signed export time
    // remains 10. It must never displace either newer genuine recovery point.
    assert_eq!(
      retention_deletions_by_creation_time(
        vec![(&replay, 10), (&recent, 20), (&newest, 30)],
        2,
      ),
      vec!["old-replayed-export"],
    );
    let partial = BackupSnapshot {
      name: "partial".into(),
      partial: true,
      ..Default::default()
    };
    assert_eq!(
      retention_deletions_by_creation_time(
        vec![
          (&replay, 10),
          (&recent, 20),
          (&newest, 30),
          (&partial, 40)
        ],
        2,
      ),
      vec!["old-replayed-export"],
    );
  }

  #[test]
  fn path_overlap_resolution_follows_symlinked_ancestors() {
    use std::os::unix::fs::symlink;

    let root = tempfile::tempdir().unwrap();
    let actual = root.path().join("actual");
    let reserved = actual.join("internal/staging");
    std::fs::create_dir_all(&reserved).unwrap();
    let alias = root.path().join("alias");
    symlink(&actual, &alias).unwrap();
    let repository = resolve_existing_path_ancestor(&alias).unwrap();
    let reserved = resolve_existing_path_ancestor(&reserved).unwrap();
    assert!(paths_overlap(&repository, &reserved));
  }

  #[test]
  fn core_local_repository_paths_must_be_canonical() {
    for path in [
      " /backups/repository",
      "/backups/repository ",
      "/backups/./repository",
      "/backups/other/../repository",
      "/backups//repository",
      "/backups/repository/",
    ] {
      let repository = BackupRepository {
        name: "Primary".into(),
        backend: BackupRepositoryBackend::CoreLocal {
          path: path.into(),
        },
        ..Default::default()
      };
      assert!(
        validate_repository_definition(&repository).is_err(),
        "accepted noncanonical path {path:?}"
      );
    }
  }

  #[test]
  fn selected_restore_publishes_only_selected_subtrees() {
    let roots =
      vec![periphery_client::api::backup::RestorePublishPath {
        destination_root: None,
        snapshot_path: "srv/app".into(),
        destination: "/restore/app".into(),
      }];
    let publish = selected_publish_paths(
      &["srv/app/config".into(), "srv/app/data/file.db".into()],
      &roots,
    )
    .unwrap();
    assert_eq!(publish.len(), 2);
    assert_eq!(publish[0].destination, "/restore/app/config");
    assert_eq!(publish[1].destination, "/restore/app/data/file.db");
    assert!(
      publish.iter().all(|item| item.destination_root.as_deref()
        == Some("/restore/app"))
    );
  }

  #[test]
  fn restore_destinations_must_not_overlap() {
    let publish = vec![
      periphery_client::api::backup::RestorePublishPath {
        destination_root: None,
        snapshot_path: "source/one".into(),
        destination: "/restore/app".into(),
      },
      periphery_client::api::backup::RestorePublishPath {
        destination_root: None,
        snapshot_path: "source/two".into(),
        destination: "/restore/app/data".into(),
      },
    ];
    assert!(validate_non_overlapping_destinations(&publish).is_err());
  }

  #[test]
  fn explicit_volume_recovery_requires_existing_destination_confirmation()
   {
    let source = BackupTarget::Volume {
      server_id: "server".into(),
      volume_name: "data".into(),
    };
    assert!(!volume_requires_confirmation(
      &source,
      Some("server"),
      None
    ));
    assert!(!volume_requires_confirmation(&source, None, None));
    assert!(volume_requires_confirmation(
      &source,
      Some("server"),
      Some("data")
    ));
    assert!(volume_requires_confirmation(
      &source,
      Some("server"),
      Some("new")
    ));
    assert!(volume_requires_confirmation(
      &source,
      Some("other"),
      None
    ));
    assert!(!volume_requires_confirmation(
      &BackupTarget::Core,
      None,
      Some("data")
    ));
  }

  #[test]
  fn unstarted_restore_plans_are_distinct_from_legacy_unknown_state()
  {
    let mut document = serde_json::json!({
      "_id": "plan",
      "plan": BackupRestorePlan::default(),
      "publish": [],
      "recovered_stack_name": "recovered",
    });
    let legacy: StoredRestorePlan =
      serde_json::from_value(document.clone()).unwrap();
    assert!(legacy.recovered_stack_execution_started);
    document["recovered_stack_execution_started"] = false.into();
    let unstarted: StoredRestorePlan =
      serde_json::from_value(document.clone()).unwrap();
    assert!(!unstarted.recovered_stack_execution_started);
    document["recovered_stack_execution_started"] = true.into();
    let started: StoredRestorePlan =
      serde_json::from_value(document).unwrap();
    assert!(started.recovered_stack_execution_started);
  }

  #[test]
  fn restore_preview_must_still_match_at_execution() {
    let stored = StoredRestorePlan {
      id: "plan".into(),
      created_by: "user".into(),
      plan: BackupRestorePlan {
        created_paths: vec!["/new/b".into(), "/new/a".into()],
        overwritten_paths: vec!["/existing".into()],
        ..Default::default()
      },
      publish: Vec::new(),
      recovered_stack_name: None,
      recovered_stack_execution_started: false,
      recovered_stack_id: None,
      recovered_stack_finalized: false,
      recovered_stack_run_directory: None,
      destination_volume_name: None,
      create_volume_if_missing: false,
      destination_exists: true,
      recovered_stack_source: None,
      source_resource_missing: false,
      snapshot_stack_source_paths: Vec::new(),
      snapshot_stack_path_aliases: HashMap::new(),
      bind_path_mappings: HashMap::new(),
    };
    let mut current = PreflightVykarRestoreResponse {
      destination_exists: true,
      created_paths: vec!["/new/a".into(), "/new/b".into()],
      overwritten_paths: vec!["/existing".into()],
      ..Default::default()
    };
    assert!(same_restore_preview(&stored, &current));
    current.deleted_paths.push("/unexpected".into());
    assert!(!same_restore_preview(&stored, &current));
  }

  #[test]
  fn core_recovery_layout_requires_a_database_export() {
    let root = tempfile::tempdir().unwrap();
    assert!(find_core_restore_layout(root.path()).is_err());
    let dated = root.path().join("source/run/2026-01-01_01-00-00");
    std::fs::create_dir_all(&dated).unwrap();
    std::fs::write(dated.join("User.gz"), b"gzip-placeholder")
      .unwrap();
    let (backup_root, restore_folder) =
      find_core_restore_layout(root.path()).unwrap();
    assert_eq!(backup_root, dated.parent().unwrap());
    assert_eq!(restore_folder, "2026-01-01_01-00-00");
  }

  #[test]
  fn core_export_digest_covers_contents_and_exact_file_names() {
    let root = tempfile::tempdir().unwrap();
    let export = root.path().join("dated");
    std::fs::create_dir(&export).unwrap();
    std::fs::write(
      root.path().join("komodo-core-manifest.json"),
      b"manifest",
    )
    .unwrap();
    let file = export.join("User.gz");
    std::fs::write(&file, b"original-admin").unwrap();
    let expected = core_export_digest(root.path()).unwrap();
    assert_eq!(core_export_digest(root.path()).unwrap(), expected);
    std::fs::write(&file, b"replacement-admin").unwrap();
    assert_ne!(core_export_digest(root.path()).unwrap(), expected);
    std::fs::write(&file, b"original-admin").unwrap();
    std::fs::rename(&file, export.join("Other.gz")).unwrap();
    assert_ne!(core_export_digest(root.path()).unwrap(), expected);
    std::fs::rename(export.join("Other.gz"), &file).unwrap();
    std::fs::write(export.join("Injected.gz"), b"injected").unwrap();
    assert_ne!(core_export_digest(root.path()).unwrap(), expected);
    std::os::unix::fs::symlink(&file, export.join("alias.gz"))
      .unwrap();
    assert!(core_export_digest(root.path()).is_err());
  }

  #[test]
  fn historical_restore_normalization_requires_the_exact_stack_marker()
   {
    let mut stack = Stack::default();
    stack.id = "recovered-stack".into();
    assert!(
      historical_restore_finalization_update("plan", &stack)
        .is_none()
    );
    stack.info.recovery_plan_id = Some("unrelated-plan".into());
    assert!(
      historical_restore_finalization_update("plan", &stack)
        .is_none()
    );
    stack.info.recovery_plan_id = Some("plan".into());
    let update =
      historical_restore_finalization_update("plan", &stack).unwrap();
    let fields = update.get_document("$set").unwrap();
    assert!(fields.get_bool("recovered_stack_finalized").unwrap());
    assert!(
      fields
        .get_bool("recovered_stack_execution_started")
        .unwrap()
    );
    assert_eq!(
      fields.get_str("recovered_stack_id").unwrap(),
      "recovered-stack"
    );
  }

  #[test]
  fn core_export_excludes_control_and_in_flight_run_state() {
    assert!(!core_export_includes_collection(SETTINGS_COLLECTION));
    assert!(!core_export_includes_collection(RUNS_COLLECTION));
    assert!(core_export_includes_collection("Stack"));
  }

  #[test]
  fn manifest_source_matching_requires_snapshot_identity() {
    let snapshot = "stack-20260902T010000Z-run-run-id";
    let marker = backup_manifest_source_name(snapshot);
    assert!(is_backup_manifest_source(
      snapshot,
      &format!("/tmp/{marker}")
    ));
    assert!(!is_backup_manifest_source(
      snapshot,
      &format!("/var/lib/docker/volumes/{marker}/_data")
    ));
    assert!(!is_backup_manifest_source(
      "stack-20260902T010001Z-run-other-id",
      &format!("/tmp/{marker}")
    ));
  }

  #[test]
  fn snapshot_server_comes_from_snapshot_hostname() {
    let snapshot = BackupSnapshot {
      hostname: "komodo-periphery-snapshot-server".into(),
      ..Default::default()
    };
    assert_eq!(
      snapshot_server_id(&snapshot),
      Some("snapshot-server")
    );
  }

  #[test]
  fn managed_recovery_database_names_require_exact_generated_suffix()
  {
    let first = core_recovery_database_name("komodo");
    let second = core_recovery_database_name(&first);
    assert!(is_managed_core_recovery_database("komodo", &first));
    assert!(is_managed_core_recovery_database(&first, &second));
    assert!(!is_managed_core_recovery_database(&first, &first));
    assert!(first.len() <= 63);
    assert!(second.len() <= 63);
    assert!(!second.contains(first.as_str()));
    assert!(!is_managed_core_recovery_database(
      "komodo",
      "komodo_recovery_customer"
    ));
    assert!(!is_managed_core_recovery_database(
      "komodo",
      "other_recovery_0123abcdef45"
    ));
  }

  #[test]
  fn previous_recovery_database_is_not_orphaned() {
    let previous = core_recovery_database_name("komodo");
    let current = core_recovery_database_name(&previous);
    let active = HashSet::new();
    assert!(!core_recovery_database_is_orphaned(
      &current,
      &previous,
      &active,
      Some(&previous),
    ));
    assert!(core_recovery_database_is_orphaned(
      &current, &previous, &active, None,
    ));
  }

  #[test]
  fn expired_core_plans_cannot_drop_current_or_rollback_databases() {
    let previous = core_recovery_database_name("komodo");
    let current = core_recovery_database_name(&previous);
    let temporary = core_recovery_database_name(&current);
    for protected in [&current, &previous] {
      assert!(!core_recovery_database_can_be_dropped(
        &current,
        protected,
        Some(&previous)
      ));
    }
    assert!(core_recovery_database_can_be_dropped(
      &current,
      &temporary,
      Some(&previous)
    ));
    assert!(!core_recovery_database_can_be_dropped(
      &current,
      "unrelated",
      None
    ));
  }

  #[tokio::test]
  async fn abandoned_tree_request_keeps_its_worker_slot_until_exit() {
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let permit = slots.clone().try_acquire_owned().unwrap();
    let (started, ready) = tokio::sync::oneshot::channel();
    let (finish, wait) = std::sync::mpsc::channel();
    let job = tokio::spawn(run_snapshot_tree_worker(
      permit,
      std::time::Instant::now() + std::time::Duration::from_secs(60),
      move || {
        let _ = started.send(());
        let _ = wait.recv();
        Ok(())
      },
    ));
    ready.await.unwrap();
    job.abort();
    assert!(job.await.unwrap_err().is_cancelled());
    assert!(slots.clone().try_acquire_owned().is_err());
    finish.send(()).unwrap();
    drop(slots.clone().acquire_owned().await.unwrap());
    assert!(slots.try_acquire_owned().is_ok());
  }

  #[test]
  fn stack_source_path_drift_ignores_only_order() {
    assert!(backup_source_paths_match(
      &["/srv/app".into(), "/srv/data".into()],
      &["/srv/data".into(), "/srv/app".into()],
    ));
    assert!(!backup_source_paths_match(
      &["/srv/app".into(), "/srv/data".into()],
      &["/srv/app-new".into(), "/srv/data".into()],
    ));
  }

  #[test]
  fn recorded_corruption_forces_a_full_verification() {
    let now = 2_000_000;
    let previous = RepositoryHealthRecord {
      last_full_verification_at: now - 1,
      verification_failed: true,
      ..Default::default()
    };
    assert!(full_verification_due(&previous, now, 30));
  }

  #[test]
  fn moved_stack_cannot_be_restored_in_place() {
    assert!(!stack_restore_requires_recovery(
      "snapshot-server",
      Some("snapshot-server"),
      "snapshot-server",
    ));
    assert!(stack_restore_requires_recovery(
      "snapshot-server",
      Some("current-server"),
      "current-server",
    ));
    assert!(stack_restore_requires_recovery(
      "snapshot-server",
      None,
      "snapshot-server",
    ));
  }

  #[test]
  fn fleet_retry_finalization_waits_for_every_retry() {
    assert_eq!(
      fleet_retry_completion(false, true).0,
      BackupRunState::Complete
    );
    assert_eq!(
      fleet_retry_completion(false, false).0,
      BackupRunState::Partial
    );
    assert_eq!(
      fleet_retry_completion(true, true).0,
      BackupRunState::Cancelled
    );
    assert!(fleet_retry_requires_maintenance(
      &BackupRunState::Complete
    ));
    assert!(fleet_retry_requires_maintenance(
      &BackupRunState::Partial
    ));
    assert!(!fleet_retry_requires_maintenance(
      &BackupRunState::Cancelled
    ));
  }

  #[test]
  fn fleet_retries_have_a_bounded_exponential_window() {
    assert_eq!(fleet_retry_delay_seconds(0), Some(2));
    assert_eq!(fleet_retry_delay_seconds(7), Some(256));
    assert_eq!(
      fleet_retry_delay_seconds(MAX_FLEET_RETRY_ATTEMPTS),
      None
    );
  }

  #[test]
  fn complete_primary_requires_a_complete_mirror_copy() {
    assert!(!mirror_copy_is_sufficient(false, None));
    assert!(!mirror_copy_is_sufficient(false, Some(true)));
    assert!(mirror_copy_is_sufficient(false, Some(false)));
    assert!(mirror_copy_is_sufficient(true, Some(true)));
  }

  #[test]
  fn retry_retention_replaces_repository_roles_independently() {
    let mut retained = vec![VykarRetainedSnapshot {
      snapshot_name: "attempt-a".into(),
      retain_primary: true,
      retain_mirror: false,
    }];

    // Attempt B succeeds only on the mirror. Nothing may remove attempt A's
    // primary copy merely because the mirror has a newer successful attempt.
    assert!(
      take_retained_repository_copies(&mut retained, false)
        .is_empty()
    );
    retained.push(VykarRetainedSnapshot {
      snapshot_name: "attempt-b".into(),
      retain_primary: false,
      retain_mirror: true,
    });
    assert!(retained.iter().any(|snapshot| {
      snapshot.snapshot_name == "attempt-a" && snapshot.retain_primary
    }));
    assert!(retained.iter().any(|snapshot| {
      snapshot.snapshot_name == "attempt-b" && snapshot.retain_mirror
    }));

    assert_eq!(
      take_retained_repository_copies(&mut retained, true),
      vec!["attempt-a".to_string()]
    );
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].snapshot_name, "attempt-b");
  }
}
