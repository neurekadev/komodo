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
  bson::{Bson, Document, doc, to_bson, to_document},
  mungos::{
    find::find_collect,
    mongodb::{
      Collection,
      options::{FindOptions, UpdateOptions},
    },
  },
};
use futures_util::{
  StreamExt, TryStreamExt, stream::FuturesUnordered,
};
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
    permission::PermissionLevel,
    repo::Repo,
    server::Server,
    stack::{Stack, StackConfig},
    user::User,
  },
};
use periphery_client::api::backup::{
  BackupSourceFilters, CancelVykarOperation, DiscoverBackupSource,
  FinalizeVykarRestore, FinalizeVykarRestoreResponse,
  GetBackupVolumeInventory, GetVykarBackupCompletion,
  PeripheryBackupTarget, PreflightVykarRestore,
  PreflightVykarRestoreResponse, ProtectedRepositoryPath,
  RunFinalizeVykarRestore, RunTransactionalVykarRestore,
  RunVykarBackup, RunVykarBackupBatch, RunVykarBackupBatchResponse,
  RunVykarBackupResponse, TransactionalVykarRestore,
  TransactionalVykarRestoreResponse, VykarBackupCompletion,
  VykarBackupCompletionState, VykarBackupRepositoryResult,
  VykarBackupTask, VykarRetainedSnapshot,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
  config::core_config,
  connection::PeripheryConnectionArgs,
  helpers::{periphery_client, query::id_or_name_filter},
  periphery::PeripheryClient,
  permission::{get_check_permissions, load_list_permits},
  resource::{self, KomodoResource},
  state::db_client,
};

pub(crate) mod activity;
mod crypto;
mod recovery;
pub(crate) mod recovery_state;

const SETTINGS_ID: &str = "singleton";
const SETTINGS_COLLECTION: &str = "BackupSettings";
const RUNS_COLLECTION: &str = "BackupRun";
const PENDING_WORKERS_COLLECTION: &str = "BackupPendingWorker";
const PLANS_COLLECTION: &str = "BackupRestorePlan";
const CORE_RECOVERY_COLLECTION: &str = "CoreRecoveryPlan";
const HEALTH_COLLECTION: &str = "BackupRepositoryHealth";
const OPERATIONAL_ALERT_PATH: &str =
  "/data/backup-operational-alert.json";
const CORE_PRIVATE_PATH: &str = "/data/core-secrets";
const CORE_STAGING_PATH: &str =
  "/data/core-secrets/.komodo-core-staging";
const CORE_CACHE_PATH: &str = "/data/backups/.komodo-vykar-cache";
const CORE_RECOVERY_STAGING_PATH: &str =
  "/data/core-secrets/.komodo-core-recovery";
const STACK_MANIFEST_STAGING_PATH: &str =
  "/data/core-secrets/.komodo-stack-manifest";
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

/// Intent is durable before dispatch. Keep the original enrolled identity
/// across reconnects, Server edits and Core restarts.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingWorkerBackup {
  #[serde(rename = "_id")]
  operation_id: String,
  run_id: String,
  server: Server,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredRestorePlan {
  #[serde(rename = "_id")]
  id: String,
  /// Restore plans are capabilities scoped to the user who confirmed them.
  #[serde(default)]
  created_by: String,
  /// Aggregate ownership survives prepared publication and Core restart.
  #[serde(default)]
  execution: Option<StoredRestoreExecution>,
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
  sealed_material: String,
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
  /// Written before destructive maintenance; a crash requires a full check.
  #[serde(default)]
  maintenance_in_progress: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredRestoreExecution {
  pending: PendingWorkerBackup,
  journal_id: String,
  deferred: bool,
  /// A failed insert response does not fence a write still running in MongoDB.
  #[serde(default)]
  recovered_stack_creation_started: bool,
  /// Every decision is durably enrolled before sending its mutating RPC.
  #[serde(default)]
  finalizations: Vec<StoredRestoreFinalization>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredRestoreFinalization {
  operation_id: String,
  commit: bool,
  acknowledge: bool,
}

fn repository_health_is_healthy(
  record: &RepositoryHealthRecord,
) -> bool {
  record.healthy
    && !record.verification_failed
    && !record.maintenance_in_progress
}

fn settings_collection() -> Collection<SealedBackupSettings> {
  db_client().db.collection(SETTINGS_COLLECTION)
}

fn runs_collection() -> Collection<BackupRun> {
  db_client().db.collection(RUNS_COLLECTION)
}

fn pending_workers_collection() -> Collection<PendingWorkerBackup> {
  db_client().db.collection(PENDING_WORKERS_COLLECTION)
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
  !matches!(
    name,
    SETTINGS_COLLECTION
      | RUNS_COLLECTION
      | PENDING_WORKERS_COLLECTION
      | PLANS_COLLECTION
  )
}

pub async fn get_settings() -> anyhow::Result<BackupSettings> {
  let Some(record) = settings_collection()
    .find_one(doc! { "_id": SETTINGS_ID })
    .await
    .context("Failed to load backup settings")?
  else {
    return Ok(BackupSettings::default());
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
      BackupSettings::default()
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
  proposed.updated_at = next_settings_revision(
    existing.as_ref().map_or(0, |settings| settings.updated_at),
    komodo_timestamp(),
  );
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

fn next_settings_revision(previous: i64, now: i64) -> i64 {
  now.max(previous.saturating_add(1))
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
  let mut trusted_ids = HashSet::new();
  for worker in &settings.trusted_workers {
    if worker.server_id.trim().is_empty()
      || worker.public_key.trim().is_empty()
      || !trusted_ids.insert(&worker.server_id)
    {
      return Err(anyhow!(
        "Trusted backup workers require unique Server IDs and non-empty pinned public keys"
      ));
    }
  }
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
      for reserved in [CORE_PRIVATE_PATH, CORE_CACHE_PATH].into_iter()
      {
        let reserved = normalize_core_local_path(reserved);
        if komodo_backup::filesystem::paths_overlap(
          &normalized,
          &reserved,
        )? {
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
      validate_s3_repository_url(url)?;
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

/// Vykar treats any repository URL without a recognized scheme as a local
/// filesystem path, so a bare provider hostname like `s3.example.com` would
/// create a repository directory inside the Core container instead of
/// contacting S3. Reject anything that is not a valid S3 endpoint/bucket/prefix
/// URL before it reaches Vykar.
fn validate_s3_repository_url(url: &str) -> anyhow::Result<()> {
  let parsed = url::Url::parse(url.trim()).map_err(|_| {
    anyhow!(
      "S3 repository URL must be s3://endpoint/bucket[/prefix]; Vykar treats anything else as a local filesystem path"
    )
  })?;
  if !matches!(parsed.scheme(), "s3" | "s3+https" | "s3+http") {
    return Err(anyhow!(
      "S3 repository URL must use the s3:// (or s3+http://) scheme"
    ));
  }
  if parsed.host_str().is_none() {
    return Err(anyhow!(
      "S3 repository URL must include an endpoint host"
    ));
  }
  let bucket = parsed
    .path()
    .trim_start_matches('/')
    .split('/')
    .next()
    .unwrap_or("");
  if bucket.is_empty() {
    return Err(anyhow!(
      "S3 repository URL must include a bucket in the path (expected s3://endpoint/bucket[/prefix])"
    ));
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
    ) => komodo_backup::filesystem::paths_same_location(
      &normalize_core_local_path(primary),
      &normalize_core_local_path(mirror),
    ),
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
    ) => komodo_backup::filesystem::paths_overlap(
      &normalize_core_local_path(primary),
      &normalize_core_local_path(mirror),
    ),
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
  Ok(&recovery_state::current()?.identity.core_instance_id)
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

fn require_trusted_backup_worker(
  settings: &BackupSettings,
  server: &Server,
) -> anyhow::Result<()> {
  if server.info.public_key.trim().is_empty()
    || !settings.trusted_workers.iter().any(|worker| {
      worker.server_id == server.id
        && worker.address == server.config.address
        && worker.public_key == server.info.public_key
    })
  {
    return Err(anyhow!(
      "Server {} is not enrolled with its current address and public key as a trusted backup worker; an administrator must verify and enroll it in Backups settings",
      server.id
    ));
  }
  Ok(())
}

struct TrustedBackupClient<'a> {
  client: PeripheryClient,
  server: &'a Server,
}

impl TrustedBackupClient<'_> {
  async fn request<T>(
    &self,
    request: T,
  ) -> anyhow::Result<T::Response>
  where
    T: std::fmt::Debug + Serialize + mogh_resolver::HasResponse,
    T::Response: serde::de::DeserializeOwned,
  {
    self
      .client
      .request_pinned(
        PeripheryConnectionArgs::from_server(self.server),
        request,
      )
      .await
  }
}

async fn trusted_backup_client<'a>(
  settings: &BackupSettings,
  server: &'a Server,
) -> anyhow::Result<TrustedBackupClient<'a>> {
  require_trusted_backup_worker(settings, server)?;
  Ok(TrustedBackupClient {
    client: periphery_client(server).await?,
    server,
  })
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

pub(crate) fn ensure_outside_core_storage(
  path: &Path,
) -> anyhow::Result<()> {
  if komodo_backup::filesystem::paths_overlap(
    path,
    Path::new(CORE_PRIVATE_PATH),
  )? {
    return Err(anyhow!(
      "Stack files cannot access protected Core storage"
    ));
  }
  Ok(())
}

pub(crate) fn file_manager_protected_paths()
-> anyhow::Result<Vec<ProtectedRepositoryPath>> {
  let core_container_id =
    komodo_backup::container::current_container_id().context(
      "Cannot identify Core to protect its recovery files",
    )?;
  Ok(vec![ProtectedRepositoryPath {
    path: CORE_PRIVATE_PATH.into(),
    core_container_id,
  }])
}

fn protected_backup_paths(
  settings: &BackupSettings,
) -> anyhow::Result<Vec<ProtectedRepositoryPath>> {
  let core_container_id = komodo_backup::container::current_container_id()
    .context("Cannot identify the Core Docker container for backup protection; retain Docker's hostname mount or default container-ID hostname")?;
  Ok(protected_core_paths(settings, &core_container_id))
}

fn protected_core_paths(
  settings: &BackupSettings,
  core_container_id: &str,
) -> Vec<ProtectedRepositoryPath> {
  // Core exports its durable recovery material separately. Exclude live state
  // and staging from workload copies, and never stop the backup coordinator.
  std::iter::once(CORE_PRIVATE_PATH.to_string())
    .chain(
      std::iter::once(&settings.primary)
        .chain(settings.mirror.iter())
        .filter_map(|repository| match &repository.backend {
          BackupRepositoryBackend::CoreLocal { path } => {
            Some(path.clone())
          }
          _ => None,
        }),
    )
    .map(|path| ProtectedRepositoryPath {
      path,
      core_container_id: core_container_id.to_string(),
    })
    .collect()
}

fn excluded_target_was_requested(
  settings: &BackupSettings,
  target: &PeripheryBackupTarget,
) -> bool {
  match target {
    PeripheryBackupTarget::Stack { .. } => {
      settings.stack_selection.mode == BackupSelectionMode::Include
    }
    PeripheryBackupTarget::Volume { .. } => {
      settings.volume_selection.mode == BackupSelectionMode::Include
    }
  }
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
  let _actions = activity::quiesce_actions()?;
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
  // Runs before listeners/schedulers. Transfer guards to the reconciler,
  // then let HTTP start so inbound-only workers can reconnect.
  let operation = backup_operation_lock().lock().await;
  let actions = activity::quiesce_actions()
    .expect("Startup reconciliation must precede Action admission");
  let roles = repository_role_barrier().clone().read_owned().await;
  let mutations = mutation_barrier().clone().write_owned().await;
  let pending =
    find_collect(&pending_workers_collection(), None, None).await;
  let restores = plans_collection()
    .find_one(pending_restore_execution_filter())
    .await;
  if let Some(count) = startup_cleanup_ready(
    matches!(&pending, Ok(pending) if pending.is_empty()),
    matches!(&restores, Ok(None)),
    finalize_reconciled_interrupted_runs(),
  )
  .await
  {
    return Ok(count);
  }
  critical_alerts().write().unwrap().reconciliation.insert(
    "startup".into(),
    "Core is completing interrupted-operation audit cleanup or reconciling backup/restore dispatches with their original workers. Mutations and Actions remain blocked until database cleanup and worker recovery succeed. Check database availability and the original enrolled worker connections; replacing worker identity does not prove completion.".into(),
  );
  tokio::spawn(async move {
    let _guards = (operation, actions, roles, mutations);
    loop {
      let result = async {
        let pending =
          find_collect(&pending_workers_collection(), None, None)
            .await?;
        for pending in pending {
          let completion = await_worker_completion(&pending).await;
          if let Some(response) = &completion.result {
            record_worker_restart_errors(
              &pending.server,
              &response.restart_errors,
            );
          }
          if let Some(response) = &completion.batch_result {
            record_worker_restart_errors(
              &pending.server,
              &response.restart_errors,
            );
          }
          acknowledge_worker_completion(&pending).await;
        }
        if pending_workers_collection()
          .find_one(doc! {})
          .await?
          .is_some()
        {
          return Err(anyhow!(
            "Completed backup intent cleanup is still pending"
          ));
        }
        reconcile_pending_restore_plans().await?;
        finalize_reconciled_interrupted_runs().await?;
        anyhow::Ok(())
      }
      .await;
      match result {
        Ok(()) => {
          critical_alerts()
            .write()
            .unwrap()
            .reconciliation
            .remove("startup");
          break;
        }
        Err(error) => warn!(
          "Backup startup reconciliation remains blocked: {error:#}"
        ),
      }
      tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
  });
  Ok(0)
}

async fn startup_cleanup_ready(
  no_backup_intents: bool,
  no_restore_intents: bool,
  finalize: impl std::future::Future<Output = anyhow::Result<u64>>,
) -> Option<u64> {
  if !no_backup_intents || !no_restore_intents {
    return None;
  }
  match finalize.await {
    Ok(count) => Some(count),
    Err(error) => {
      warn!(
        "Interrupted-run audit cleanup will retry under startup reconciliation: {error:#}"
      );
      None
    }
  }
}

async fn finalize_reconciled_interrupted_runs() -> anyhow::Result<u64>
{
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

fn append_backup_history_keys(
  keys: &mut Vec<String>,
  id: String,
  _name: String,
) {
  // Historical name-only records cannot be safely attributed after name reuse.
  // Leave those records administrator-only rather than assigning a new owner.
  keys.push(id);
}

async fn readable_backup_history_keys<T: KomodoResource>(
  user: &User,
) -> anyhow::Result<Vec<String>> {
  let mut permits =
    load_list_permits::<T>(user, PermissionLevel::Read.backups())
      .await?;
  let mut resources = T::coll().find(doc! {}).batch_size(100).await?;
  let mut keys = Vec::new();
  // Check each current resource once, not once per historical backup run.
  while let Some(resource) = resources.try_next().await? {
    if permits.permitted::<T>(&resource).await.unwrap_or(false) {
      append_backup_history_keys(
        &mut keys,
        resource.id,
        resource.name,
      );
    }
  }
  Ok(keys)
}

fn backup_history_filter(
  stack_keys: Vec<String>,
  server_keys: Vec<String>,
) -> Document {
  doc! { "$or": [
    { "target.type": "Stack", "target.params.stack_id": { "$in": stack_keys } },
    { "target.type": "Volume", "target.params.server_id": { "$in": server_keys } },
  ] }
}

pub async fn status(user: &User) -> anyhow::Result<BackupStatus> {
  let history_filter = if user.admin {
    doc! {}
  } else {
    backup_history_filter(
      readable_backup_history_keys::<Stack>(user).await?,
      readable_backup_history_keys::<Server>(user).await?,
    )
  };
  let recent_runs = runs_collection()
    .find(history_filter.clone())
    .sort(doc! { "started_at": -1, "id": -1 })
    .limit(20)
    .await?
    .try_collect()
    .await?;
  let active_runs = find_collect(
    &runs_collection(),
    doc! { "$and": [history_filter, {
      "state": {
        "$in": [
          to_bson(&BackupRunState::Queued)?,
          to_bson(&BackupRunState::Running)?,
        ]
      }
    }] },
    FindOptions::builder()
      .sort(doc! { "started_at": -1 })
      .build(),
  )
  .await
  .unwrap_or_default();
  let (
    (previous_primary, previous_mirror, settings),
    _repository_roles,
    health_refresh,
  ) = load_health_with_refresh_admission(
    repository_role_barrier().clone(),
    repository_health_refresh_slots().clone(),
    || async {
      let primary = health_collection()
        .find_one(doc! { "_id": "primary" })
        .await
        .ok()
        .flatten()
        .unwrap_or_default();
      let mirror = health_collection()
        .find_one(doc! { "_id": "mirror" })
        .await
        .ok()
        .flatten()
        .unwrap_or_default();
      let settings = get_settings()
        .await
        .inspect_err(record_configuration_alert)
        .ok();
      let fresh = settings.as_ref().is_none_or(|settings| {
        repository_health_cache_is_fresh(
          primary.inventory_checked_at,
          settings.updated_at,
          komodo_timestamp(),
        )
      });
      Ok(((primary, mirror, settings), fresh))
    },
  )
  .await?;
  let Some(settings) = settings else {
    return Ok(BackupStatus {
      active_runs,
      recent_runs,
      critical_alert: current_critical_alert(),
      ..Default::default()
    });
  };
  let Some(health_refresh) = health_refresh else {
    return Ok(BackupStatus {
      active_runs,
      recent_runs,
      next_run_at: next_scheduled_run().unwrap_or_default(),
      primary_healthy: repository_health_is_healthy(
        &previous_primary,
      ),
      mirror_healthy: settings
        .mirror
        .as_ref()
        .map(|_| repository_health_is_healthy(&previous_mirror)),
      mirror_lagging_snapshots: if settings.mirror.is_some() {
        previous_primary.mirror_lagging_snapshots
      } else {
        0
      },
      last_full_verification_at: previous_primary
        .last_full_verification_at,
      critical_alert: current_critical_alert(),
    });
  };
  let primary_settings = settings.clone();
  let primary_repository = settings.primary.clone();
  let deadline =
    std::time::Instant::now() + std::time::Duration::from_secs(60);
  let (primary, health_refresh) = run_snapshot_inventory_worker(
    health_refresh,
    deadline,
    move || {
      Ok(
        core_repository(&primary_repository, &primary_settings)
          .and_then(|repository| repository.list_snapshots())
          .map(|inventory| {
            (
              inventory
                .snapshots
                .into_iter()
                .map(|snapshot| (snapshot.name, snapshot.partial))
                .collect::<HashMap<_, _>>(),
              inventory.hidden == 0,
            )
          }),
      )
    },
  )
  .await
  .context("Primary health worker failed")?;
  let primary_inventory_healthy =
    primary.as_ref().is_ok_and(|(_, healthy)| *healthy);
  let primary_healthy = primary_inventory_healthy
    && !previous_primary.verification_failed
    && !previous_primary.maintenance_in_progress;
  let primary_names =
    primary.map(|(names, _)| names).unwrap_or_default();
  let (mirror_healthy, mirror_lagging_snapshots, _health_refresh) =
    if let Some(mirror) = settings.mirror.clone() {
      let mirror_settings = settings.clone();
      let (mirror, health_refresh) = run_snapshot_inventory_worker(
        health_refresh,
        deadline,
        move || {
          Ok(
            core_repository(&mirror, &mirror_settings)
              .and_then(|repository| repository.list_snapshots())
              .map(|inventory| {
                (
                  inventory
                    .snapshots
                    .into_iter()
                    .map(|snapshot| (snapshot.name, snapshot.partial))
                    .collect::<HashMap<_, _>>(),
                  inventory.hidden == 0,
                )
              }),
          )
        },
      )
      .await
      .context("Mirror health worker failed")?;
      let (healthy, lagging) = match mirror {
        Ok((mirror_snapshots, healthy)) => (
          Some(
            healthy
              && !previous_mirror.verification_failed
              && !previous_mirror.maintenance_in_progress,
          ),
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
      };
      (healthy, lagging, health_refresh)
    } else {
      (None, 0, health_refresh)
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

fn repository_health_refresh_slots()
-> &'static Arc<tokio::sync::Semaphore> {
  static SLOTS: OnceLock<Arc<tokio::sync::Semaphore>> =
    OnceLock::new();
  SLOTS.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(1)))
}

/// Cache readers do not consume inventory admission. Drop the role read guard
/// before acquiring admission, then reload under a new guard: another refresh
/// or a repository-role change may have completed in that gap.
async fn load_health_with_refresh_admission<T, F>(
  roles: Arc<tokio::sync::RwLock<()>>,
  slots: Arc<tokio::sync::Semaphore>,
  mut load: impl FnMut() -> F,
) -> anyhow::Result<(
  T,
  tokio::sync::OwnedRwLockReadGuard<()>,
  Option<tokio::sync::OwnedSemaphorePermit>,
)>
where
  F: std::future::Future<Output = anyhow::Result<(T, bool)>>,
{
  let role = roles.clone().read_owned().await;
  let (state, fresh) = load().await?;
  if fresh {
    return Ok((state, role, None));
  }
  drop(role);
  let permit = slots.try_acquire_owned()
    .context("Repository health inventory is still running; retry after it finishes")?;
  let role = roles.read_owned().await;
  let (state, fresh) = load().await?;
  if fresh {
    return Ok((state, role, None));
  }
  Ok((state, role, Some(permit)))
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
  /// Rebuilt from durable dispatch intents after restart, cleared on terminal proof.
  reconciliation: BTreeMap<String, String>,
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
      .chain(self.reconciliation.values())
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

pub async fn list_snapshots() -> anyhow::Result<(
  Vec<BackupSnapshot>,
  u64,
  tokio::sync::OwnedSemaphorePermit,
)> {
  let permit = snapshot_inventory_slots().clone().try_acquire_owned()
    .context("Another snapshot inventory request is still running; retry after it finishes")?;
  let deadline =
    std::time::Instant::now() + std::time::Duration::from_secs(60);
  let settings = get_settings().await?;
  let ((snapshots, hidden), permit) =
    run_snapshot_inventory_worker(permit, deadline, move || {
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
    .await?;
  // The caller retains admission until it has filtered/paged or selected from
  // the inventory, so finished workers cannot accumulate full result vectors.
  Ok((snapshots, hidden, permit))
}

fn snapshot_inventory_slots() -> &'static Arc<tokio::sync::Semaphore>
{
  static SLOTS: OnceLock<Arc<tokio::sync::Semaphore>> =
    OnceLock::new();
  SLOTS.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(1)))
}

async fn run_snapshot_inventory_worker<T: Send + 'static>(
  permit: tokio::sync::OwnedSemaphorePermit,
  deadline: std::time::Instant,
  work: impl FnOnce() -> anyhow::Result<T> + Send + 'static,
) -> anyhow::Result<(T, tokio::sync::OwnedSemaphorePermit)> {
  let worker = tokio::task::spawn_blocking(move || {
    // Cancellation/timeout leaves admission with the actual blocking worker.
    // Success transfers it with the inventory until the caller consumes it.
    let result = work()?;
    anyhow::Ok((result, permit))
  });
  tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), worker)
    .await
    .context("Snapshot inventory request exceeded 60 seconds; its worker may still be finishing")?
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
  type RetentionGroup<'a> = (u64, Vec<(&'a BackupSnapshot, i64)>);
  let mut by_source: HashMap<String, RetentionGroup<'_>> =
    HashMap::new();
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
  user: &User,
) -> anyhow::Result<BackupRun> {
  // Write requests are detached from their HTTP connection. Never let manual
  // requests accumulate invisible work before a run ID can be cancelled.
  let _operation = admit_manual_backup(backup_operation_lock())?;
  let user = crate::helpers::query::get_user(&user.id).await?;
  let target =
    authorize_manual_backup(target.as_ref(), &user).await?;
  if active_backup_run().await?.is_some() {
    return Err(anyhow!(
      "A backup run or its retries are still active; retry after it finishes"
    ));
  }
  run_backup_locked(target, None)
    .await?
    .context("Manual backup was not admitted")
}

fn admit_manual_backup(
  operation: &tokio::sync::Mutex<()>,
) -> anyhow::Result<tokio::sync::MutexGuard<'_, ()>> {
  operation.try_lock().context(
    "Another backup operation is active; this manual request was not queued. Retry after it finishes",
  )
}

async fn authorize_manual_backup(
  target: Option<&BackupTarget>,
  user: &User,
) -> anyhow::Result<Option<BackupTarget>> {
  if !user.enabled {
    return Err(anyhow!("User is no longer enabled"));
  }
  if let Some(target) = target {
    let target = match target {
      BackupTarget::Stack { stack_id } => BackupTarget::Stack {
        stack_id: get_check_permissions::<Stack>(
          stack_id,
          user,
          PermissionLevel::Execute.backups(),
        )
        .await?
        .id,
      },
      BackupTarget::Volume {
        server_id,
        volume_name,
      } => BackupTarget::Volume {
        server_id: get_check_permissions::<Server>(
          server_id,
          user,
          PermissionLevel::Execute.backups(),
        )
        .await?
        .id,
        volume_name: volume_name.clone(),
      },
      target => {
        authorize_target(target, user, PermissionLevel::Execute)
          .await?;
        target.clone()
      }
    };
    // Freeze the ID returned by the very lookup whose permissions were checked.
    Ok(Some(target))
  } else if user.admin {
    Ok(None)
  } else {
    Err(anyhow!("Fleet backup operations are admin only"))
  }
}

async fn active_backup_run() -> anyhow::Result<Option<BackupRun>> {
  Ok(
    runs_collection()
      .find_one(doc! {
        "state": {
          "$in": [
            to_bson(&BackupRunState::Queued)?,
            to_bson(&BackupRunState::Running)?,
          ]
        }
      })
      .await?,
  )
}

async fn run_backup_locked(
  target: Option<BackupTarget>,
  scheduled_revision: Option<i64>,
) -> anyhow::Result<Option<BackupRun>> {
  let _actions = activity::quiesce_actions()?;
  let _repository_roles =
    repository_role_barrier().clone().read_owned().await;
  let settings = get_settings().await?;
  if !schedule_admission_matches(&settings, scheduled_revision) {
    return Ok(None);
  }
  let mut run = new_run(target.clone(), "Backup running").await?;
  let run_id = run.id.clone();
  let result = match target {
    Some(target) => run_target(&settings, &run, target)
      .await
      .map(|partial| (partial, partial, Vec::new())),
    None => run_fleet(&settings, &run).await.map(|outcome| {
      (outcome.partial, outcome.permanent_partial, outcome.retries)
    }),
  };
  let finished = match result {
    Ok((_, _, _)) if cancellation_requested(&run_id) => {
      finish_run(
        run,
        BackupRunState::Cancelled,
        "Cancellation requested",
      )
      .await
    }
    Ok((_, permanent_partial, retries)) if !retries.is_empty() => {
      run.message =
        "Initial fleet pass was partial; retries are active".into();
      let _ = runs_collection()
        .update_one(
          doc! { "id": &run.id },
          doc! { "$set": { "message": &run.message } },
        )
        .await;
      spawn_fleet_retry_finalizer(
        run.clone(),
        permanent_partial,
        retries,
      );
      return Ok(Some(run));
    }
    Ok((true, _, _)) => {
      finish_run(
        run,
        BackupRunState::Partial,
        "Backup completed partially",
      )
      .await
    }
    Ok((false, _, _)) => {
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
  Ok(Some(finished))
}

fn schedule_admission_matches(
  settings: &BackupSettings,
  scheduled_revision: Option<i64>,
) -> bool {
  scheduled_revision.is_none_or(|revision| {
    settings.enabled && settings.updated_at == revision
  })
}

trait WorkerBackupRequest:
  std::fmt::Debug + Serialize + mogh_resolver::HasResponse
{
  fn operation_id(&self) -> &str;
  fn run_id(&self) -> &str;
  fn take_result(
    completion: VykarBackupCompletion,
  ) -> Option<Self::Response>;
  fn restart_errors(response: &Self::Response) -> &[String];
}

impl WorkerBackupRequest for RunVykarBackup {
  fn operation_id(&self) -> &str {
    &self.operation_id
  }
  fn run_id(&self) -> &str {
    &self.run_id
  }
  fn take_result(
    completion: VykarBackupCompletion,
  ) -> Option<RunVykarBackupResponse> {
    completion.result
  }
  fn restart_errors(response: &RunVykarBackupResponse) -> &[String] {
    &response.restart_errors
  }
}

impl WorkerBackupRequest for RunVykarBackupBatch {
  fn operation_id(&self) -> &str {
    &self.operation_id
  }
  fn run_id(&self) -> &str {
    &self.run_id
  }
  fn take_result(
    completion: VykarBackupCompletion,
  ) -> Option<RunVykarBackupBatchResponse> {
    completion.batch_result
  }
  fn restart_errors(
    response: &RunVykarBackupBatchResponse,
  ) -> &[String] {
    &response.restart_errors
  }
}

fn record_worker_restart_errors(server: &Server, errors: &[String]) {
  if !errors.is_empty() {
    record_operational_alert(format!(
      "Backup restart failed on {} ({}): {}",
      server.name,
      server.id,
      errors.join("; ")
    ));
  }
}

fn worker_completion_is_terminal(
  completion: &VykarBackupCompletion,
) -> bool {
  completion.state == VykarBackupCompletionState::Complete
}

/// A lost RPC is not evidence that the worker exited. No overall deadline is
/// safe here: the caller retains its activity, role and filesystem guards
/// until the original worker durably reports completion or fences a late request.
async fn await_worker_completion(
  pending: &PendingWorkerBackup,
) -> VykarBackupCompletion {
  loop {
    let result = async {
      let client = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        periphery_client(&pending.server),
      )
      .await
      .context("Completion connection deadline exceeded")??;
      if cancellation_requested(&pending.run_id) {
        let _ = client
          .request_pinned_with_timeout(
            PeripheryConnectionArgs::from_server(&pending.server),
            CancelVykarOperation {
              operation_id: pending.run_id.clone(),
            },
            std::time::Duration::from_secs(10),
          )
          .await;
      }
      client
        .request_pinned_with_timeout(
          PeripheryConnectionArgs::from_server(&pending.server),
          GetVykarBackupCompletion {
            operation_id: pending.operation_id.clone(),
            run_id: pending.run_id.clone(),
            cancel_if_unknown: true,
            acknowledge: false,
          },
          std::time::Duration::from_secs(10),
        )
        .await
    }
    .await;
    match result {
      Ok(completion)
        if worker_completion_is_terminal(&completion) =>
      {
        return completion;
      }
      Ok(_) => {}
      Err(error) => {
        warn!(
          operation_id = pending.operation_id,
          "Waiting for original backup worker completion: {error:#}"
        );
      }
    }
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
  }
}

async fn acknowledge_worker_completion(
  pending: &PendingWorkerBackup,
) {
  // Removing the Core intent is safe only after terminal proof and after
  // restart failures have been surfaced. Failure merely causes reconciliation
  // to repeat after restart; the worker's terminal identity is never deleted.
  if let Err(error) = pending_workers_collection()
    .delete_one(doc! { "_id": &pending.operation_id })
    .await
  {
    warn!("Could not remove completed backup intent: {error:#}");
    return;
  }
  let acknowledgement = async {
    let client = periphery_client(&pending.server).await?;
    client
      .request_pinned_with_timeout(
        PeripheryConnectionArgs::from_server(&pending.server),
        GetVykarBackupCompletion {
          operation_id: pending.operation_id.clone(),
          run_id: pending.run_id.clone(),
          cancel_if_unknown: false,
          acknowledge: true,
        },
        std::time::Duration::from_secs(10),
      )
      .await
  };
  let _ = tokio::time::timeout(
    std::time::Duration::from_secs(15),
    acknowledgement,
  )
  .await;
}

async fn run_worker_backup<T>(
  settings: &BackupSettings,
  server: &Server,
  request: T,
) -> anyhow::Result<T::Response>
where
  T: WorkerBackupRequest,
  T::Response: serde::de::DeserializeOwned,
{
  ensure_not_cancelled(request.run_id())?;
  let client = trusted_backup_client(settings, server).await?;
  let pending = PendingWorkerBackup {
    operation_id: request.operation_id().into(),
    run_id: request.run_id().into(),
    server: server.clone(),
  };
  pending_workers_collection()
    .insert_one(&pending)
    .await
    .context("Could not persist backup dispatch intent")?;
  let result = match client.request(request).await {
    Ok(response) => Ok(response),
    Err(error) => {
      critical_alerts().write().unwrap().reconciliation.insert(
        pending.operation_id.clone(), format!(
        "Backup {} is waiting for completion from its original worker {} ({}); mutations remain blocked until that worker reconnects and reconciles: {error:#}",
        pending.run_id, server.name, server.id
      ));
      let completion = await_worker_completion(&pending).await;
      match completion.error.clone() {
        Some(error) => Err(anyhow!(error)),
        None => T::take_result(completion).context(
          "Worker completed without the expected backup result",
        ),
      }
    }
  };
  if let Ok(response) = &result {
    record_worker_restart_errors(server, T::restart_errors(response));
  }
  acknowledge_worker_completion(&pending).await;
  critical_alerts()
    .write()
    .unwrap()
    .reconciliation
    .remove(&pending.operation_id);
  result
}

async fn run_scheduled_backup(
  revision: i64,
) -> anyhow::Result<Option<BackupRun>> {
  let Ok(_operation) = backup_operation_lock().try_lock() else {
    tracing::info!(
      "Skipping scheduled fleet backup because another backup operation is active"
    );
    return Ok(None);
  };
  if let Some(active) = active_backup_run().await? {
    tracing::info!(
      run_id = active.id,
      "Skipping scheduled fleet backup because an earlier run is still active"
    );
    return Ok(None);
  }
  run_backup_locked(None, Some(revision)).await
}

fn spawn_fleet_retry_finalizer(
  run: BackupRun,
  permanent_partial: bool,
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
      permanent_partial,
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
  permanent_partial: bool,
) -> (BackupRunState, &'static str) {
  if cancelled {
    (BackupRunState::Cancelled, "Cancellation requested")
  } else if all_complete && !permanent_partial {
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
  health_collection()
    .update_one(
      doc! { "_id": health_id },
      repository_verification_update(
        succeeded,
        full,
        false,
        komodo_timestamp(),
      ),
    )
    .with_options(UpdateOptions::builder().upsert(true).build())
    .await?;
  Ok(())
}

fn repository_verification_update(
  succeeded: bool,
  full: bool,
  maintenance_completed: bool,
  now: i64,
) -> Document {
  if succeeded && full {
    doc! { "$set": {
      "healthy": true,
      "checked_at": now,
      "last_full_verification_at": now,
      "verification_failed": false,
      "maintenance_in_progress": false,
    } }
  } else if succeeded && maintenance_completed {
    // Clear only this completed maintenance cycle. A sample must never clear
    // a real verification failure, even if this helper is used incorrectly.
    doc! {
      "$set": {
        "healthy": true,
        "checked_at": now,
        "maintenance_in_progress": false,
      },
      "$setOnInsert": { "verification_failed": false },
    }
  } else if succeeded {
    // A sample that happens not to encounter previously-recorded corruption
    // or interrupted maintenance cannot prove either state is safe. Only a
    // full check or the owning completed maintenance cycle clears its latch.
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
  }
}

fn repository_maintenance_started_update(now: i64) -> Document {
  doc! { "$set": {
    "healthy": false,
    "checked_at": now,
    "maintenance_in_progress": true,
  } }
}

async fn run_maintenance() -> anyhow::Result<()> {
  let _operation = backup_operation_lock().lock().await;
  let _actions = activity::quiesce_actions()?;
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
    // Persist uncertainty before any deletion/compaction can begin. Process
    // death cannot run an error handler, so a post-error latch is insufficient.
    health_collection()
      .update_one(
        doc! { "_id": health_id },
        repository_maintenance_started_update(komodo_timestamp()),
      )
      .with_options(UpdateOptions::builder().upsert(true).build())
      .await?;
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
    // Publish the successful check only after the entire destructive cycle.
    // A failed write leaves the durable in-progress latch set for restart.
    health_collection()
      .update_one(
        doc! { "_id": health_id },
        repository_verification_update(
          true,
          full_due,
          true,
          komodo_timestamp(),
        ),
      )
      .with_options(UpdateOptions::builder().upsert(true).build())
      .await?;
  }
  Ok(())
}

fn full_verification_due(
  previous: &RepositoryHealthRecord,
  now: i64,
  every_days: u64,
) -> bool {
  previous.verification_failed
    || previous.maintenance_in_progress
    || previous.last_full_verification_at == 0
    || now.saturating_sub(previous.last_full_verification_at)
      >= every_days.max(1) as i64 * 24 * 60 * 60 * 1000
}

struct FleetRunOutcome {
  partial: bool,
  permanent_partial: bool,
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

async fn backup_volume_inventory(
  server: &Server,
) -> anyhow::Result<Vec<VolumeListItem>> {
  let client = tokio::time::timeout(
    std::time::Duration::from_secs(10),
    periphery_client(server),
  )
  .await
  .context("Backup discovery connection exceeded 10 seconds")??;
  client
    .request_with_timeout(
      GetBackupVolumeInventory {},
      std::time::Duration::from_secs(65),
    )
    .await
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
  let mut permanent_partial = false;
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
          permanent_partial = true;
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
        permanent_partial = true;
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
        permanent_partial = true;
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
      ensure_not_cancelled(&run.id)?;
      let volumes = backup_volume_inventory(&server).await;
      let volumes = match volumes {
        Ok(volumes) => volumes,
        Err(error) => {
          warn!(
            "Backup discovery failed on {}: {error:#}",
            server.name
          );
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
        }
      };
      for volume in volumes.into_iter().filter(|volume| {
        volume_is_backup_eligible(
          volume,
          settings.include_anonymous_volumes,
        )
      }) {
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
        permanent_partial = true;
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
                  permanent_partial: false,
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
    // Cancellation stops admission, never drops already dispatched workers.
    // Their completion/restart results must be consumed under the same guards.
    let result = match result {
      Ok(result) => result,
      Err(error) => {
        partial = true;
        permanent_partial |= !cancellation_requested(&run.id);
        warn!("Backup batch was not admitted: {error:#}");
        continue;
      }
    };
    let (server_id, targets, tasks, refresh_targets, result) = result;
    match result {
      Ok(outcome) => {
        partial |= outcome.partial || !targets.is_empty();
        permanent_partial |= outcome.permanent_partial;
        if !cancellation_requested(&run.id)
          && !outcome.retry_blocked
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
        if cancellation_requested(&run.id) {
          continue;
        }
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
  Ok(FleetRunOutcome {
    partial,
    permanent_partial,
    retries,
  })
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
    let mut permanent_partial = false;
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
      let _actions = match activity::quiesce_actions() {
        Ok(guard) => guard,
        Err(error) => {
          warn!("Fleet backup retry {retry} deferred: {error:#}");
          continue;
        }
      };
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
            return !permanent_partial;
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
      permanent_partial |= refreshed.blocked;
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
            permanent_partial |= outcome.permanent_partial;
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
        return all_complete && !permanent_partial;
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
      let _actions = match activity::quiesce_actions() {
        Ok(guard) => guard,
        Err(error) => {
          warn!("Core backup retry {retry} deferred: {error:#}");
          continue;
        }
      };
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
  let volumes = backup_volume_inventory(&server).await?;
  for volume in volumes.into_iter().filter(|volume| {
    volume_is_backup_eligible(
      volume,
      settings.include_anonymous_volumes,
    )
  }) {
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
  permanent_partial: bool,
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
  ensure_not_cancelled(&run.id)?;
  let server = resource::get::<Server>(server_id).await?;
  let expected = tasks.len();
  let response = run_worker_backup(
    settings,
    &server,
    RunVykarBackupBatch {
      operation_id: Uuid::new_v4().to_string(),
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
      protected_repository_paths: protected_backup_paths(settings)?,
      filters: backup_source_filters(settings),
      stop_containers: settings.stop_containers,
    },
  )
  .await;
  let response = match response {
    Ok(response) => response,
    Err(error) => {
      warn!(
        "Backup node {} completed without a successful result; the next attempt will use a fresh snapshot name: {error:#}",
        server.name
      );
      return Ok(NodeBatchOutcome {
        partial: true,
        permanent_partial: false,
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
  let result_count = response.results.len();
  let mut results = response
    .results
    .into_iter()
    .map(|result| (result.source_label, result.result))
    .collect::<HashMap<_, _>>();
  let mut retry_tasks = Vec::new();
  let mut explicitly_excluded = false;
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
    if let Some(reason) = result.excluded {
      warn!(
        source = task.source_label,
        "Backup source excluded: {reason}"
      );
      explicitly_excluded |=
        excluded_target_was_requested(settings, &task.target);
      // Deliberately excluded sources neither create snapshots nor enter the
      // retry/retention path. Existing copies are not deleted as failed writes.
      continue;
    }
    let mirror_configured = settings.mirror.is_some();
    let primary_complete =
      result.primary.complete && result.primary.error.is_none();
    let mirror_complete = !mirror_configured
      || result.mirror.as_ref().is_some_and(|mirror| {
        mirror.complete && mirror.error.is_none()
      });
    let retain_primary =
      repository_attempt_is_retained(&result.primary);
    let retain_mirror = mirror_configured
      && result
        .mirror
        .as_ref()
        .is_some_and(repository_attempt_is_retained);
    migrate_legacy_retained_snapshots(&mut task, mirror_configured);
    if primary_complete {
      retire_retained_repository_copies(
        settings,
        &mut task.retained_snapshots,
        true,
      )
      .await;
    } else if !retain_primary {
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
      } else if !retain_mirror {
        delete_node_snapshot_copies(
          settings,
          task.snapshot_name.clone(),
          false,
          true,
        )
        .await;
      }
    }
    if retain_primary || retain_mirror {
      task.retained_snapshots.push(VykarRetainedSnapshot {
        snapshot_name: task.snapshot_name.clone(),
        retain_primary,
        retain_mirror,
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
    || explicitly_excluded
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
    permanent_partial: explicitly_excluded
      || !response.restart_errors.is_empty(),
    retry_tasks,
    retry_blocked: !response.restart_errors.is_empty(),
  })
}

fn repository_attempt_is_retained(
  result: &VykarBackupRepositoryResult,
) -> bool {
  // A committed partial snapshot is diagnostic evidence, not an absent/failed
  // write. Keep it (and any older complete copy) until complete replacement or
  // normal retention. Partial data must never count as a complete backup.
  result.partial || result.complete && result.error.is_none()
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
  logical_source_label: String,
  hostname: String,
  export_digest: String,
  created_at: i64,
  source_path: String,
  staging: PathBuf,
  retry_primary: bool,
  retry_mirror: bool,
  retained_snapshots: Vec<VykarRetainedSnapshot>,
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
) -> anyhow::Result<komodo_backup::BackupResult> {
  let settings_for_worker = settings.clone();
  let retry_for_worker = retry.clone();
  tokio::task::spawn_blocking(move || {
    let repository =
      core_repository(&repository, &settings_for_worker)?;
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

fn prepare_core_retry_attempt(
  retry: &mut CoreRepositoryRetry,
  name: String,
  mirror: bool,
  authorize: impl FnOnce(
    &str,
    &str,
    &str,
    &str,
    i64,
  ) -> anyhow::Result<String>,
) -> anyhow::Result<()> {
  let label = authorize(
    &retry.logical_source_label,
    &retry.hostname,
    &name,
    &retry.export_digest,
    retry.created_at,
  )?;
  // Every retry is an additional, freshly authenticated attempt over the
  // same immutable export. Keep the old role copies until that role commits
  // its replacement; never delete a partial before attempting another write.
  retry.retained_snapshots.push(VykarRetainedSnapshot {
    snapshot_name: retry.snapshot_name.clone(),
    retain_primary: true,
    retain_mirror: mirror,
  });
  retry.snapshot_name = name;
  retry.source_label = label;
  // Common names preserve primary/mirror correspondence. A successful older
  // role remains retained if its additional copy fails in this round.
  retry.retry_primary = true;
  retry.retry_mirror = mirror;
  Ok(())
}

async fn retry_core_repositories(
  settings: &BackupSettings,
  run: &BackupRun,
  mut retry: CoreRepositoryRetry,
) -> anyhow::Result<bool> {
  ensure_not_cancelled(&run.id)?;
  prepare_core_retry_attempt(
    &mut retry,
    snapshot_name("core", &run.id),
    settings.mirror.is_some(),
    crypto::authorize_core_source_label,
  )?;
  let cancellation = cancellation_token(&run.id)
    .unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
  if retry.retry_primary {
    let result = write_core_repository_snapshot(
      settings.primary.clone(),
      settings,
      &retry,
      cancellation.clone(),
    )
    .await;
    retry.retry_primary =
      !matches!(&result, Ok(result) if !result.partial);
    if !retry.retry_primary {
      retire_retained_repository_copies(
        settings,
        &mut retry.retained_snapshots,
        true,
      )
      .await;
    }
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
    )
    .await;
    retry.retry_mirror =
      !matches!(&result, Ok(result) if !result.partial);
    if !retry.retry_mirror {
      retire_retained_repository_copies(
        settings,
        &mut retry.retained_snapshots,
        false,
      )
      .await;
    }
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
      &[
        SETTINGS_COLLECTION,
        RUNS_COLLECTION,
        PENDING_WORKERS_COLLECTION,
        PLANS_COLLECTION,
      ],
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
    "schema": recovery::EXPORT_SCHEMA,
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
  recovery::write_material(&staging, settings).await?;
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
    logical_source_label: BackupTarget::Core
      .source_label(core_instance_id()?),
    hostname,
    export_digest: digest,
    created_at,
    source_path: path,
    staging: staging.clone(),
    retry_primary: true,
    retry_mirror: settings.mirror.is_some(),
    retained_snapshots: Vec::new(),
  };
  let cancellation = cancellation_token(&run.id)
    .context("Core backup cancellation token is unavailable")?;
  let primary_result = write_core_repository_snapshot(
    settings.primary.clone(),
    settings,
    &retry,
    cancellation.clone(),
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
  let response = run_worker_backup(
    settings,
    &server,
    RunVykarBackup {
      operation_id: Uuid::new_v4().to_string(),
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
      protected_repository_paths: protected_backup_paths(settings)?,
      filters: backup_source_filters(settings),
      stop_containers: settings.stop_containers,
      mirror_only: false,
      primary_only: false,
    },
  )
  .await?;
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
  let response = run_worker_backup(
    settings,
    &server,
    RunVykarBackup {
      operation_id: Uuid::new_v4().to_string(),
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
      protected_repository_paths: protected_backup_paths(settings)?,
      filters: backup_source_filters(settings),
      stop_containers: settings.stop_containers,
      mirror_only: false,
      primary_only: false,
    },
  )
  .await?;
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
  let snapshot = {
    let (snapshots, _, _inventory) = list_snapshots().await?;
    snapshots
      .into_iter()
      .find(|snapshot| snapshot.name == snapshot_name)
      .context("Snapshot does not exist in the primary repository")?
  };
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
  // Share browsing admission and retain it in the actual blocking worker.
  // An abandoned or timed-out plan cannot start another manifest reader.
  let permit = snapshot_tree_slots().clone().try_acquire_owned()
    .context("Another snapshot tree or manifest request is still running; retry after it finishes")?;
  let deadline =
    std::time::Instant::now() + std::time::Duration::from_secs(60);
  let current = tokio::time::timeout_at(
    tokio::time::Instant::from_std(deadline),
    async {
      Stack::coll().find_one(id_or_name_filter(stack_id)).await
    },
  )
  .await
  .context(
    "Stack lookup exceeded the manifest preflight deadline",
  )??;
  let manifest_source = snapshot
    .source_paths
    .iter()
    .find(|path| is_backup_manifest_source(&snapshot.name, path))
    .context("Stack snapshot has no embedded recovery manifest")?
    .clone();
  let settings = tokio::time::timeout_at(
    tokio::time::Instant::from_std(deadline),
    get_settings(),
  )
  .await
  .context(
    "Stack manifest settings exceeded the preflight deadline",
  )??;
  let repository = settings.primary.clone();
  let advanced = settings.advanced.clone();
  let hostname = snapshot.hostname.clone();
  let snapshot_name = snapshot.name.clone();
  let manifest: SnapshotBackupManifest =
    run_snapshot_tree_worker(permit, deadline, move || {
      let bytes = VykarRepository::new(
        &repository,
        &hostname,
        &core_cache_dir()?,
        &core_secret_dir()?,
        &advanced,
      )?
      .read_snapshot_file(
        &snapshot_name,
        &format!(
          "{}/komodo-backup-manifest.json",
          manifest_source.trim_matches('/')
        ),
        deadline,
      )?;
      serde_json::from_slice(&bytes)
        .context("Invalid Stack snapshot recovery manifest")
    })
    .await?;
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
  let client = tokio::time::timeout(
    std::time::Duration::from_secs(10),
    periphery_client(&server),
  )
  .await
  .context(
    "Stack source discovery connection exceeded 10 seconds",
  )??;
  let source = client
    .request_with_timeout(
      DiscoverBackupSource {
        target: PeripheryBackupTarget::Stack {
          stack: Box::new(stack),
          repo: repo.map(Box::new),
        },
        filters: backup_source_filters(&settings),
        protected_repository_paths: protected_backup_paths(
          &settings,
        )?,
      },
      std::time::Duration::from_secs(65),
    )
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
  let _actions = activity::quiesce_actions()?;
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
  let preflight = trusted_backup_client(&settings, &server)
    .await?
    .request(PreflightVykarRestore {
      target,
      repository: repository_for_periphery(&settings.primary, false)?,
      protected_repository_paths: protected_backup_paths(&settings)?,
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
    path_summary: preflight.path_summary,
    containers_to_stop: preflight.containers_to_stop,
    expires_at: komodo_timestamp() + 15 * 60 * 1000,
  };
  plans_collection()
    .insert_one(StoredRestorePlan {
      id: plan.id.clone(),
      created_by: user.id.clone(),
      execution: None,
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
    && stored.plan.path_summary == current.path_summary
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

fn restore_phase_available(
  completion: &VykarBackupCompletion,
) -> bool {
  matches!(
    completion.state,
    VykarBackupCompletionState::Complete
      | VykarBackupCompletionState::Prepared
      | VykarBackupCompletionState::RecoveryRequired
  )
}

fn require_no_pending_recovered_stack_insert(
  execution: &StoredRestoreExecution,
) -> anyhow::Result<()> {
  if execution.recovered_stack_creation_started {
    return Err(anyhow!(
      "Recovered Stack insertion was attempted but no matching Stack is visible. Its database write may still commit, so automatic rollback is unsafe. Reconciliation will commit a delayed matching insertion; otherwise administrator-led recovery must establish that no original insert can still finish before resolving the Stack and worker journals. Mutations remain blocked."
    ));
  }
  Ok(())
}

fn completed_restore_outcome(
  completion: &VykarBackupCompletion,
) -> Option<(BackupRunState, String)> {
  if completion.state != VykarBackupCompletionState::Complete {
    return None;
  }
  let response = completion.restore_result.as_ref();
  if response.is_some_and(|response| response.finalization_pending) {
    return None;
  }
  let complete = completion.error.is_none()
    && response.is_some_and(|response| {
      response.complete
        && !response.rolled_back
        && !response.finalization_pending
        && response.critical_error.is_none()
    });
  let message = completion
    .error
    .clone()
    .or_else(|| {
      response.and_then(|response| response.critical_error.clone())
    })
    .unwrap_or_else(|| {
      if complete {
        "Restore complete".into()
      } else {
        "Restore interrupted or rolled back".into()
      }
    });
  Some((
    if complete {
      BackupRunState::Complete
    } else {
      BackupRunState::Failed
    },
    message,
  ))
}

fn record_restore_reconciliation(
  pending: &PendingWorkerBackup,
  reason: &str,
) {
  critical_alerts().write().unwrap().reconciliation.insert(
    pending.operation_id.clone(),
    format!("Restore {} is awaiting its original worker {} ({}): {reason}. Restore remains non-cancellable and conflicting mutations stay blocked. Reconnect that enrolled worker and repair/restart it if journal recovery is required; do not replace its identity or delete operation records.",
      pending.run_id, pending.server.name, pending.server.id),
  );
}

async fn await_restore_phase(
  pending: &PendingWorkerBackup,
) -> VykarBackupCompletion {
  loop {
    let query = async {
      let client = periphery_client(&pending.server).await?;
      client
        .request_pinned_with_timeout(
          PeripheryConnectionArgs::from_server(&pending.server),
          GetVykarBackupCompletion {
            operation_id: pending.operation_id.clone(),
            run_id: pending.run_id.clone(),
            cancel_if_unknown: true,
            acknowledge: false,
          },
          std::time::Duration::from_secs(10),
        )
        .await
    };
    let result =
      tokio::time::timeout(std::time::Duration::from_secs(15), query)
        .await;
    match result {
      Ok(Ok(completion)) if restore_phase_available(&completion) => {
        let incident = completion
          .restore_result
          .as_ref()
          .and_then(|response| response.critical_error.as_ref())
          .or_else(|| {
            completion
              .finalize_restore_result
              .as_ref()
              .and_then(|response| response.critical_error.as_ref())
          });
        if let Some(incident) = incident {
          record_operational_alert(format!(
            "Restore {} on {} ({}): {incident}",
            pending.run_id, pending.server.name, pending.server.id
          ));
        }
        return completion;
      }
      Ok(Ok(_)) => record_restore_reconciliation(
        pending,
        "operation is still running",
      ),
      Ok(Err(error)) => {
        record_restore_reconciliation(pending, &format!("{error:#}"))
      }
      Err(_) => record_restore_reconciliation(
        pending,
        "completion query timed out",
      ),
    }
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
  }
}

async fn persist_restore_execution(
  stored: &StoredRestorePlan,
) -> anyhow::Result<()> {
  let execution = stored
    .execution
    .as_ref()
    .context("Restore execution identity is missing")?;
  let updated = plans_collection()
    .update_one(
      doc! { "_id": &stored.id },
      doc! { "$set": {
        "execution": to_bson(execution)?,
        "recovered_stack_execution_started": execution.deferred,
      } },
    )
    .await?;
  if updated.matched_count != 1 {
    return Err(anyhow!(
      "Restore plan disappeared before dispatch enrollment"
    ));
  }
  Ok(())
}

async fn dispatch_restore(
  stored: &StoredRestorePlan,
  request: TransactionalVykarRestore,
) -> anyhow::Result<TransactionalVykarRestoreResponse> {
  let execution = stored
    .execution
    .as_ref()
    .context("Restore execution identity is missing")?;
  // Enrollment was persisted before this first send. Never fall back to the
  // older wire name: old workers would mutate without registering a receipt.
  let result = async {
    let client = periphery_client(&execution.pending.server).await?;
    client
      .request_pinned(
        PeripheryConnectionArgs::from_server(
          &execution.pending.server,
        ),
        RunTransactionalVykarRestore(request),
      )
      .await
  }
  .await;
  if let Err(error) = result {
    record_restore_reconciliation(
      &execution.pending,
      &format!("{error:#}"),
    );
  }
  loop {
    let completion = await_restore_phase(&execution.pending).await;
    if completion.state
      == VykarBackupCompletionState::RecoveryRequired
    {
      if execution.deferred {
        return Err(anyhow!(
          "Prepared restore requires guarded saga reconciliation"
        ));
      }
      record_restore_reconciliation(
        &execution.pending,
        "worker journal recovery is required",
      );
      tokio::time::sleep(std::time::Duration::from_secs(5)).await;
      continue;
    }
    if let Some(error) = completion.error {
      return Err(anyhow!(error));
    }
    return completion.restore_result.context(
      "Restore worker returned no authoritative restore result",
    );
  }
}

async fn dispatch_restore_finalization(
  stored: &mut StoredRestorePlan,
  commit: bool,
  acknowledge: bool,
) -> anyhow::Result<FinalizeVykarRestoreResponse> {
  let decision = StoredRestoreFinalization {
    operation_id: Uuid::new_v4().to_string(),
    commit,
    acknowledge,
  };
  stored.execution.as_mut().context(
    "Legacy restore has no original worker identity; automatic finalization is unsafe",
  )?.finalizations.push(decision.clone());
  persist_restore_execution(stored).await?;
  let execution = stored.execution.as_ref().unwrap();
  let mut pending = execution.pending.clone();
  pending.operation_id = decision.operation_id.clone();
  let request = RunFinalizeVykarRestore(FinalizeVykarRestore {
    operation_id: decision.operation_id,
    run_id: pending.run_id.clone(),
    restore_operation_id: execution.pending.operation_id.clone(),
    journal_id: execution.journal_id.clone(),
    commit,
    acknowledge,
  });
  let result = async {
    let client = periphery_client(&pending.server).await?;
    client
      .request_pinned(
        PeripheryConnectionArgs::from_server(&pending.server),
        request,
      )
      .await
  }
  .await;
  if let Err(error) = result {
    record_restore_reconciliation(
      &execution.pending,
      &format!("{error:#}"),
    );
  }
  let completion = await_restore_phase(&pending).await;
  if completion.state != VykarBackupCompletionState::Complete {
    return Err(anyhow!(
      "Restore finalization requires worker journal recovery"
    ));
  }
  if let Some(error) = completion.error {
    return Err(anyhow!(error));
  }
  completion
    .finalize_restore_result
    .context("Worker returned no authoritative finalization result")
}

async fn finish_restore_execution(
  stored: &StoredRestorePlan,
  state: BackupRunState,
  message: impl Into<String>,
) -> anyhow::Result<BackupRun> {
  let execution = stored
    .execution
    .as_ref()
    .context("Restore execution identity is missing")?;
  let run = runs_collection()
    .find_one(doc! { "id": &execution.pending.run_id })
    .await?
    .context("Restore run disappeared during reconciliation")?;
  // Preserve aggregate ownership if either durable Core step fails.
  let run = finish_run(run, state, message).await?;
  plans_collection()
    .delete_one(doc! { "_id": &stored.id })
    .await?;
  acknowledge_worker_completion(&execution.pending).await;
  for decision in &execution.finalizations {
    let mut pending = execution.pending.clone();
    pending.operation_id = decision.operation_id.clone();
    acknowledge_worker_completion(&pending).await;
    critical_alerts()
      .write()
      .unwrap()
      .reconciliation
      .remove(&pending.operation_id);
  }
  critical_alerts()
    .write()
    .unwrap()
    .reconciliation
    .remove(&execution.pending.operation_id);
  Ok(run)
}

async fn reconcile_restore_execution_once(
  stored: &mut StoredRestorePlan,
) -> anyhow::Result<BackupRun> {
  let execution = stored.execution.clone().context(
    "An already-started legacy restore lacks its original worker/dispatch identity. Automatic recovery is blocked: independently stop the original Core and worker, preserve their database/private journals, and complete administrator-led recovery before admitting mutations. Replacing the Server or deleting journals is not proof of completion.",
  )?;
  // Fence not-yet-received decisions and drain every admitted RPC before
  // reading saga state or issuing another commit/rollback decision.
  for decision in &execution.finalizations {
    let mut pending = execution.pending.clone();
    pending.operation_id = decision.operation_id.clone();
    await_restore_phase(&pending).await;
  }
  let completion = await_restore_phase(&execution.pending).await;
  if !execution.deferred {
    let (state, message) = completed_restore_outcome(&completion)
      .context("Worker restore journals still require recovery")?;
    return finish_restore_execution(stored, state, message).await;
  }
  let name = stored
    .recovered_stack_name
    .as_deref()
    .context("Recovered Stack name is missing")?;
  let existing =
    Stack::coll().find_one(doc! { "name": name }).await?;
  let marked = existing
    .filter(|stack| recovered_stack_belongs_to_plan(stored, stack));
  if let Some(stack) = marked {
    if completion
      .restore_result
      .as_ref()
      .is_some_and(|response| response.rolled_back)
    {
      return Err(anyhow!(
        "Recovered Stack exists but worker reports rolled-back publication; administrator recovery is required"
      ));
    }
    finalize_recovered_stack_saga(stored, &stack).await?;
    return finish_restore_execution(
      stored,
      BackupRunState::Complete,
      "Restore complete after reconciliation",
    )
    .await;
  }
  if stored.recovered_stack_id.is_some()
    || stored.recovered_stack_finalized
  {
    return Err(anyhow!(
      "Marked recovered Stack is missing; cannot choose a safe restore decision"
    ));
  }
  // A read on a new connection cannot fence an earlier insert whose response
  // was lost. Only a saga that never attempted creation may choose rollback
  // from the absence of its marker.
  require_no_pending_recovered_stack_insert(&execution)?;
  if completion.state == VykarBackupCompletionState::Complete {
    if completion.restore_result.as_ref().is_some_and(|response| {
      response.complete && !response.rolled_back
    }) {
      return Err(anyhow!(
        "Worker committed recovered files without a matching Stack; administrator recovery is required"
      ));
    }
    return finish_restore_execution(
      stored,
      BackupRunState::Failed,
      completion.error.unwrap_or_else(|| {
        "Restore interrupted before recovered Stack creation".into()
      }),
    )
    .await;
  }
  let rollback =
    dispatch_restore_finalization(stored, false, true).await?;
  if !rollback.complete
    || !rollback.rolled_back
    || rollback.critical_error.is_some()
  {
    return Err(anyhow!(rollback.critical_error.unwrap_or_else(
      || "Restore rollback is not yet complete".into()
    )));
  }
  finish_restore_execution(
    stored,
    BackupRunState::Failed,
    "Restore rolled back before recovered Stack creation",
  )
  .await
}

/// Caller retains operation, Action, role and mutation guards for this whole
/// loop. Database outages and ambiguous worker results cannot reopen admission.
async fn reconcile_restore_execution(
  stored: &mut StoredRestorePlan,
) -> BackupRun {
  loop {
    match reconcile_restore_execution_once(stored).await {
      Ok(run) => return run,
      Err(error) => {
        if let Some(execution) = &stored.execution {
          record_restore_reconciliation(
            &execution.pending,
            &format!("{error:#}"),
          );
        } else {
          critical_alerts()
            .write()
            .unwrap()
            .reconciliation
            .insert(stored.id.clone(), format!("{error:#}"));
        }
      }
    }
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
  }
}

async fn finalize_recovered_stack_saga(
  stored: &mut StoredRestorePlan,
  stack: &Stack,
) -> anyhow::Result<()> {
  if !stored.recovered_stack_finalized {
    let finalized =
      dispatch_restore_finalization(stored, true, false).await?;
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

  let acknowledged =
    dispatch_restore_finalization(stored, true, true).await?;
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
  // Aggregate execution is consumed only after the final run outcome is durable.
  Ok(())
}

pub async fn execute_restore(
  plan_id: &str,
  user: &User,
) -> anyhow::Result<BackupRun> {
  let _operation = backup_operation_lock().lock().await;
  let _actions = activity::quiesce_actions()?;
  let _repository_roles =
    repository_role_barrier().clone().read_owned().await;
  let _mutation_guard = mutation_barrier().write().await;
  let mut stored = plans_collection()
    .find_one(doc! { "_id": plan_id, "created_by": &user.id })
    .await?
    .context("Restore plan does not exist")?;
  if stored.execution.is_some()
    || stored.recovered_stack_name.is_some()
      && stored.recovered_stack_execution_started
  {
    return Ok(reconcile_restore_execution(&mut stored).await);
  }
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
  let refreshed_preview = trusted_backup_client(&settings, &server)
    .await?
    .request(PreflightVykarRestore {
      target: target.clone(),
      repository: repository_for_periphery(&settings.primary, false)?,
      protected_repository_paths: protected_backup_paths(&settings)?,
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
    require_trusted_backup_worker(&settings, &server)?;
    let existing_recovered_stack = recovered_creation
      .as_ref()
      .and_then(|(_, _, existing)| existing.as_ref());
    let journal_id = if recovered_creation.is_some() {
      stored.id.clone()
    } else {
      run.id.clone()
    };
    if existing_recovered_stack.is_some() {
      return Err(anyhow!("Recovered Stack has no original dispatch identity; administrator recovery is required"));
    }
    let operation_id = Uuid::new_v4().to_string();
    stored.execution = Some(StoredRestoreExecution {
      pending: PendingWorkerBackup {
        operation_id: operation_id.clone(),
        run_id: run.id.clone(),
        server: server.clone(),
      },
      journal_id: journal_id.clone(),
      deferred: recovered_creation.is_some(),
      recovered_stack_creation_started: false,
      finalizations: Vec::new(),
    });
    persist_restore_execution(&stored).await?;
    if existing_recovered_stack.is_none() {
      let response = dispatch_restore(&stored, TransactionalVykarRestore {
          operation_id,
          run_id: run.id.clone(),
          target,
          repository: repository_for_periphery(
            &settings.primary,
            false,
          )?,
          protected_repository_paths: protected_backup_paths(
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
        return Err(anyhow!(error));
      }
      if !response.complete {
        return finish_restore_execution(
          &stored,
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
        stored.execution.as_mut().unwrap().recovered_stack_creation_started = true;
        persist_restore_execution(&stored).await?;
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
      // A database error does not prove the marked insert failed. Once creation
      // was enrolled, absence alone can never authorize an automatic rollback.
      let recovered_stack = creation?;
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
    finish_restore_execution(
      &stored,
      BackupRunState::Complete,
      "Restore complete",
    )
    .await
  }
  .await;
  match operation {
    Ok(run) => Ok(run),
    Err(error) if stored.execution.is_some() => {
      if let Some(execution) = &stored.execution {
        record_restore_reconciliation(
          &execution.pending,
          &format!("{error:#}"),
        );
      }
      Ok(reconcile_restore_execution(&mut stored).await)
    }
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
      "execution": Bson::Null,
      "$or": [
        { "recovered_stack_name": Bson::Null },
        { "recovered_stack_name": { "$exists": false } },
        { "recovered_stack_execution_started": false },
      ],
    })
    .await?;
  Ok(())
}

fn pending_restore_execution_filter() -> Document {
  doc! { "$or": [
    { "execution": { "$exists": true, "$ne": Bson::Null } },
    {
      "recovered_stack_name": { "$type": "string" },
      "recovered_stack_execution_started": { "$ne": false },
    },
  ] }
}

async fn reconcile_pending_restore_plans() -> anyhow::Result<()> {
  let plans = find_collect(
    &plans_collection(),
    pending_restore_execution_filter(),
    None,
  )
  .await?;
  for mut stored in plans {
    reconcile_restore_execution(&mut stored).await;
  }
  Ok(())
}

async fn reconcile_recovered_stack_restores() -> anyhow::Result<()> {
  let _operation = backup_operation_lock().lock().await;
  let _actions = activity::quiesce_actions()?;
  let _roles = repository_role_barrier().clone().read_owned().await;
  let _mutation = mutation_barrier().write().await;
  reconcile_pending_restore_plans().await
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
  Ok(
    recovery_state::current()?
      .previous
      .as_ref()
      .map(|previous| previous.database.clone()),
  )
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

fn historical_restore_marker_cleanup(
  plan_id: &str,
  stack: &Stack,
) -> Option<database::bson::Document> {
  // A database import must not revive authority over the old worker.
  (stack.info.recovery_plan_id.as_deref() == Some(plan_id)).then(
    || {
      doc! { "$unset": { "info.recovery_plan_id": "" } }
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
        historical_restore_marker_cleanup(&stored.id, &stack)
    {
      // Imported markers describe historical metadata, not authority to
      // finalize a live worker. Clear only the matching imported marker.
      validation
        .collection::<Stack>("Stack")
        .update_one(
          doc! { "info.recovery_plan_id": &stored.id },
          update,
        )
        .await?;
    }
  }
  // Live coordination and preview capabilities never survive database import.
  plans.delete_many(doc! {}).await?;
  validation
    .collection::<PendingWorkerBackup>(PENDING_WORKERS_COLLECTION)
    .delete_many(doc! {})
    .await?;
  Ok(())
}

pub async fn plan_core_recovery(
  snapshot_name: &str,
  created_by: String,
  provided_repository: Option<BackupRepository>,
) -> anyhow::Result<CoreRecoveryPlan> {
  let _operation = core_recovery_operation_lock().lock().await;
  let _actions = activity::quiesce_actions()?;
  reconcile_core_recovery_state_inner().await?;
  let _repository_roles =
    repository_role_barrier().clone().read_owned().await;
  let repository = recovery::repository(provided_repository).await?;
  let snapshot = {
    let (snapshots, _) =
      recovery::snapshots(repository.clone()).await?;
    snapshots
      .into_iter()
      .find(|snapshot| snapshot.name == snapshot_name)
      .context(
        "Core snapshot does not exist in the selected repository",
      )?
  };
  if snapshot.target != BackupTarget::Core {
    return Err(anyhow!("Selected snapshot is not a Core backup"));
  }
  if snapshot.partial {
    return Err(anyhow!(
      "Partial Core snapshots cannot be recovered"
    ));
  }

  let recovery_source = repository.clone();
  let staging = PathBuf::from(CORE_RECOVERY_STAGING_PATH)
    .join(Uuid::new_v4().to_string());
  tokio::fs::create_dir_all(&staging).await?;
  let _staging_cleanup = RemoveDirectoryOnDrop::new(staging.clone());
  let worker_staging = staging.clone();
  let snapshot_for_worker = snapshot.name.clone();
  let settings_for_worker = BackupSettings::default();
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
    find_file_named(&staging, "komodo-core-manifest.json")?
      .context("Core snapshot manifest is missing")?;
  let authenticated_root = manifest_path
    .parent()
    .context("Core manifest has no export root")?
    .to_path_buf();
  let digest_root = authenticated_root.clone();
  let snapshot_for_worker = snapshot.clone();
  let (mut material, expected_created_at) =
    tokio::task::spawn_blocking(move || {
      recovery::validate_snapshot_material(
        &digest_root,
        &snapshot_for_worker,
      )
    })
    .await
    .context("Core recovery validation worker failed")??;
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
  if backup_schema != recovery::EXPORT_SCHEMA {
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
  if recovered_core_instance_id != material.identity.core_instance_id
    || recovered_core_instance_id.len() != 32
    || !recovered_core_instance_id
      .chars()
      .all(|character| character.is_ascii_hexdigit())
  {
    return Err(anyhow!(
      "Core snapshot manifest contains an invalid Core identity"
    ));
  }

  recovery::configure_source(&mut material, recovery_source)?;
  let sealed_material =
    crypto::seal(&serde_json::to_vec(&material)?)?;
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
    recovery::save_settings(&validation, &material).await?;
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
        sealed_material,
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
  let actions = activity::quiesce_actions()?;
  // Same order as backups: repository role before mutation. Settings saves
  // hold only the role writer and must not acquire a generic mutation reader.
  let repository_roles =
    repository_role_barrier().clone().write_owned().await;
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
  let material = recovery::unseal_material(&stored.sealed_material)?;
  recovery::save_settings(&validation, &material).await?;
  let next_state = recovery_state::current()?.activated(
    material.identity,
    stored.plan.validation_database.clone(),
    stored.plan.current_database.clone(),
  )?;
  recovery_state::activate(&next_state)?;
  // Once the durable pointer is published, restart even if recording the
  // final audit result encounters a transient database error.
  schedule_core_restart();
  // The durable activation pointer is now authoritative. Keep new backup and
  // restore operations blocked until the process restarts into that database.
  std::mem::forget(backup_operation);
  std::mem::forget(mutation);
  std::mem::forget(actions);
  std::mem::forget(repository_roles);
  let mut run = committed_core_recovery_run(&stored.plan);
  let mut warnings = Vec::new();
  core_recovery_audit_step(
    &mut run,
    &mut warnings,
    "recovery plan cleanup",
    async {
      core_recovery_collection()
        .delete_one(doc! { "_id": &stored.id })
        .await?;
      Ok(())
    },
  )
  .await;
  let audit = run.clone();
  core_recovery_audit_step(
    &mut run,
    &mut warnings,
    "recovery audit persistence",
    async {
      runs_collection().insert_one(&audit).await?;
      Ok(())
    },
  )
  .await;
  for warning in warnings {
    warn!("{warning}");
    record_operational_alert(warning);
  }
  Ok(run)
}

fn committed_core_recovery_run(plan: &CoreRecoveryPlan) -> BackupRun {
  let now = komodo_timestamp();
  BackupRun {
    id: Uuid::new_v4().to_string(),
    target: Some(BackupTarget::Core),
    state: BackupRunState::Complete,
    cancellable: false,
    started_at: now,
    finished_at: now,
    message: format!(
      "Core recovery activation committed; restart scheduled into database '{}' (previous database '{}' retained)",
      plan.validation_database, plan.current_database
    ),
    ..Default::default()
  }
}

async fn core_recovery_audit_step(
  run: &mut BackupRun,
  warnings: &mut Vec<String>,
  step: &str,
  operation: impl std::future::Future<Output = anyhow::Result<()>>,
) {
  // Both best-effort audit steps together get at most one second, leaving
  // time to return the committed outcome before the scheduled restart.
  let result = tokio::time::timeout(
    std::time::Duration::from_millis(500),
    operation,
  )
  .await
  .context("Audit step timed out")
  .and_then(|result| result);
  if let Err(error) = result {
    let warning = format!(
      "Core recovery activation is committed and restart remains scheduled; {step} failed: {error:#}"
    );
    run.message.push_str(&format!("; warning: {warning}"));
    warnings.push(warning);
  }
}

fn find_file_named(
  root: &Path,
  name: &str,
) -> anyhow::Result<Option<PathBuf>> {
  let mut pending = vec![(root.to_path_buf(), 0)];
  let mut found = None;
  let mut entries = 0;
  while let Some((directory, depth)) = pending.pop() {
    if depth > 128 {
      return Err(anyhow!(
        "Core export nesting exceeds 128 directories"
      ));
    }
    for entry in std::fs::read_dir(directory)? {
      let entry = entry?;
      entries += 1;
      if entries > 100_000 {
        return Err(anyhow!("Core export has too many entries"));
      }
      let kind = entry.file_type()?;
      if kind.is_dir() {
        pending.push((entry.path(), depth + 1));
      } else if !kind.is_file() {
        return Err(anyhow!(
          "Core export contains a symlink or special file"
        ));
      } else if entry.file_name() == name {
        if found.is_some() {
          return Err(anyhow!(
            "Core snapshot has duplicate manifests"
          ));
        }
        found = Some(entry.path());
      }
    }
  }
  Ok(found)
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
  let _actions = activity::quiesce_actions()?;
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
  let _actions = activity::quiesce_actions()?;
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
  // Startup reconciliation also accepts cancellation, although its in-memory
  // token was lost with the old process.
  cancellation_tokens()
    .lock()
    .unwrap()
    .entry(run_id.to_string())
    .or_insert_with(|| Arc::new(AtomicBool::new(false)))
    .store(true, Ordering::SeqCst);
  if *fleet_generation().read().unwrap() == run_id {
    fleet_generation().write().unwrap().clear();
  }
  let servers = find_collect(&db_client().servers, None, None)
    .await
    .unwrap_or_default();
  let mut servers = servers
    .into_iter()
    .map(|server| (server.id.clone(), server))
    .collect::<HashMap<_, _>>();
  if let Ok(pending) = find_collect(
    &pending_workers_collection(),
    doc! { "run_id": run_id },
    None,
  )
  .await
  {
    for pending in pending {
      // The original dispatch identity overrides subsequently edited settings.
      servers.insert(pending.server.id.clone(), pending.server);
    }
  }
  futures_util::future::join_all(servers.values().map(
    |server| async move {
      let cancellation = async {
        let client = periphery_client(server).await?;
        client
          .request_pinned_with_timeout(
            PeripheryConnectionArgs::from_server(server),
            CancelVykarOperation {
              operation_id: run_id.to_string(),
            },
            std::time::Duration::from_secs(10),
          )
          .await
      };
      let _ = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        cancellation,
      )
      .await;
    },
  ))
  .await;
  // The owner holds this lock for the complete backup operation. Waiting here
  // guarantees Core export/repository workers and the initial fleet batch have
  // observed cancellation before the audit record becomes Cancelled.
  let _operation = backup_operation_lock().lock().await;
  // An active Core retry can still be reading this immutable export. Cleanup
  // follows its actual completion, never acknowledgement of the cancel flag.
  discard_core_repository_retry(run_id).await;
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
      // Carry this exact revision to final admission under the role lease.
      // A save after this read must invalidate the tick before a run exists.
      if let Err(error) =
        run_scheduled_backup(current.updated_at).await
      {
        error!("Scheduled fleet backup failed: {error:#}");
      }
    }
  });
}

pub async fn list_core_recovery_snapshots(
  repository: Option<BackupRepository>,
  page: u64,
  limit: u64,
) -> anyhow::Result<komodo_client::api::read::BackupSnapshotList> {
  let repository = recovery::repository(repository).await?;
  let (mut snapshots, hidden) =
    recovery::snapshots(repository).await?;
  snapshots
    .sort_by_key(|snapshot| std::cmp::Reverse(snapshot.created_at));
  let total = snapshots.len() as u64;
  let limit = limit.clamp(1, 500);
  let snapshots = snapshots
    .into_iter()
    .skip(page.saturating_mul(limit) as usize)
    .take(limit as usize)
    .collect();
  Ok(komodo_client::api::read::BackupSnapshotList {
    snapshots,
    total,
    hidden,
  })
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn scheduled_admission_rechecks_enabled_and_exact_revision() {
    let mut settings = BackupSettings {
      enabled: true,
      updated_at: 10,
      ..Default::default()
    };
    assert!(schedule_admission_matches(&settings, Some(10)));
    settings.enabled = false;
    assert!(!schedule_admission_matches(&settings, Some(10)));
    assert!(schedule_admission_matches(&settings, None));
    settings.enabled = true;
    settings.updated_at = 11;
    assert!(!schedule_admission_matches(&settings, Some(10)));
    assert!(schedule_admission_matches(&settings, Some(11)));
    assert_eq!(next_settings_revision(10, 10), 11);
    assert_eq!(next_settings_revision(11, 9), 12);
  }

  #[test]
  fn worker_result_presence_never_substitutes_for_terminal_proof() {
    let mut receipt = VykarBackupCompletion {
      result: Some(RunVykarBackupResponse::default()),
      ..Default::default()
    };
    assert!(!worker_completion_is_terminal(&receipt));
    receipt.state = VykarBackupCompletionState::Running;
    assert!(!worker_completion_is_terminal(&receipt));
    receipt.state = VykarBackupCompletionState::Complete;
    receipt.result = None;
    receipt.error = Some("Dispatch fenced before admission".into());
    assert!(worker_completion_is_terminal(&receipt));
  }

  #[test]
  fn prepared_and_recovery_required_restore_phases_keep_ownership() {
    let mut completion = VykarBackupCompletion {
      restore_result: Some(TransactionalVykarRestoreResponse {
        complete: true,
        ..Default::default()
      }),
      ..Default::default()
    };
    for state in [
      VykarBackupCompletionState::Prepared,
      VykarBackupCompletionState::RecoveryRequired,
    ] {
      completion.state = state;
      assert!(restore_phase_available(&completion));
      assert!(!worker_completion_is_terminal(&completion));
      assert!(completed_restore_outcome(&completion).is_none());
    }
    completion.state = VykarBackupCompletionState::Complete;
    assert_eq!(
      completed_restore_outcome(&completion).unwrap().0,
      BackupRunState::Complete
    );
    completion.error =
      Some("Worker restarted; interrupted outcome".into());
    assert_eq!(
      completed_restore_outcome(&completion).unwrap().0,
      BackupRunState::Failed
    );
    completion.error = None;
    completion.restore_result.as_mut().unwrap().rolled_back = true;
    assert_eq!(
      completed_restore_outcome(&completion).unwrap().0,
      BackupRunState::Failed
    );
  }

  #[test]
  fn receipt_backed_restore_uses_distinct_transparent_wire_names() {
    use mogh_resolver::HasResponse;
    assert_ne!(
      RunTransactionalVykarRestore::req_type(),
      TransactionalVykarRestore::req_type()
    );
    assert_ne!(
      RunFinalizeVykarRestore::req_type(),
      FinalizeVykarRestore::req_type()
    );
    let request = FinalizeVykarRestore {
      operation_id: "decision".into(),
      run_id: "run".into(),
      restore_operation_id: "original".into(),
      journal_id: "journal".into(),
      commit: false,
      acknowledge: true,
    };
    let expected = serde_json::to_value(&request).unwrap();
    let encoded =
      serde_json::to_value(RunFinalizeVykarRestore(request)).unwrap();
    assert_eq!(encoded, expected);
    assert_eq!(encoded["restore_operation_id"], "original");
  }

  #[tokio::test]
  async fn failed_startup_audit_cleanup_requires_retry_even_without_dispatches()
   {
    assert_eq!(
      startup_cleanup_ready(
        true,
        true,
        std::future::ready(Err(anyhow!("transient database outage")))
      )
      .await,
      None,
    );
    assert_eq!(
      startup_cleanup_ready(true, true, std::future::ready(Ok(3)))
        .await,
      Some(3),
    );
    assert_eq!(
      startup_cleanup_ready(true, false, std::future::ready(Ok(3)))
        .await,
      None,
    );
  }

  #[test]
  fn restore_execution_roundtrip_preserves_original_authority_and_decisions()
   {
    let mut server = Server {
      id: "0123456789abcdef01234567".into(),
      ..Default::default()
    };
    server.config.address = "wss://original.example".into();
    server.info.public_key = "original-key".into();
    let execution = StoredRestoreExecution {
      pending: PendingWorkerBackup {
        operation_id: "original-operation".into(),
        run_id: "original-run".into(),
        server: server.clone(),
      },
      journal_id: "journal".into(),
      deferred: true,
      recovered_stack_creation_started: true,
      finalizations: vec![StoredRestoreFinalization {
        operation_id: "pending-decision".into(),
        commit: true,
        acknowledge: false,
      }],
    };
    let restored: StoredRestoreExecution = serde_json::from_value(
      serde_json::to_value(execution).unwrap(),
    )
    .unwrap();
    server.config.address = "wss://replacement.example".into();
    server.info.public_key = "replacement-key".into();
    assert!(
      !PeripheryConnectionArgs::from_server(&restored.pending.server)
        .matches(PeripheryConnectionArgs::from_server(&server))
    );
    assert_eq!(restored.pending.operation_id, "original-operation");
    assert_eq!(restored.pending.run_id, "original-run");
    assert_eq!(restored.pending.server.id, server.id);
    assert!(restored.recovered_stack_creation_started);
    assert_eq!(
      restored.finalizations[0].operation_id,
      "pending-decision"
    );
    assert!(restored.finalizations[0].commit);
    assert!(!restored.finalizations[0].acknowledge);
    let filter = pending_restore_execution_filter();
    assert!(
      filter.get_array("$or").unwrap()[0]
        .as_document()
        .unwrap()
        .get_document("execution")
        .unwrap()
        .get_bool("$exists")
        .unwrap()
    );
  }

  #[test]
  fn missing_stack_after_attempted_insert_cannot_authorize_rollback()
  {
    let mut execution = StoredRestoreExecution {
      pending: PendingWorkerBackup {
        operation_id: "original-operation".into(),
        run_id: "original-run".into(),
        server: Server::default(),
      },
      journal_id: "journal".into(),
      deferred: true,
      recovered_stack_creation_started: false,
      finalizations: Vec::new(),
    };
    assert!(
      require_no_pending_recovered_stack_insert(&execution).is_ok()
    );
    execution.recovered_stack_creation_started = true;
    let recovered: StoredRestoreExecution = serde_json::from_value(
      serde_json::to_value(execution).unwrap(),
    )
    .unwrap();
    assert!(
      require_no_pending_recovered_stack_insert(&recovered).is_err()
    );
  }

  #[test]
  fn lost_backup_response_recovers_the_original_restart_errors() {
    let receipt = VykarBackupCompletion {
      state: VykarBackupCompletionState::Complete,
      batch_result: Some(RunVykarBackupBatchResponse {
        restart_errors: vec!["container still stopped".into()],
        ..Default::default()
      }),
      ..Default::default()
    };
    assert!(worker_completion_is_terminal(&receipt));
    let response = RunVykarBackupBatch::take_result(receipt).unwrap();
    assert_eq!(
      RunVykarBackupBatch::restart_errors(&response),
      &["container still stopped".to_string()]
    );
  }

  #[test]
  fn recovery_settings_record_preserves_sealed_state_and_init_flags()
  {
    let settings = SealedBackupSettings {
      id: SETTINGS_ID.into(),
      sealed: "current-sealed-settings-not-the-plan-copy".into(),
      updated_at: 123,
      primary_initialized: true,
      mirror_initialized: true,
    };
    let saved: SealedBackupSettings =
      database::bson::from_document(to_document(&settings).unwrap())
        .unwrap();
    assert_eq!(saved.sealed, settings.sealed);
    assert_eq!(saved.updated_at, 123);
    assert!(saved.primary_initialized && saved.mirror_initialized);
    assert!(!core_export_includes_collection(
      PENDING_WORKERS_COLLECTION
    ));
  }

  #[test]
  fn core_retries_resign_fresh_names_over_the_immutable_export() {
    let mut retry = CoreRepositoryRetry {
      snapshot_name: "original-partial".into(),
      source_label: "original-signature".into(),
      logical_source_label: "core/instance".into(),
      hostname: "komodo-core-instance".into(),
      export_digest: "immutable-digest".into(),
      created_at: 123,
      source_path: "/private/export".into(),
      staging: PathBuf::from("/private/export"),
      retry_primary: false,
      retry_mirror: true,
      retained_snapshots: Vec::new(),
    };
    let authorize = |source: &str,
                     host: &str,
                     name: &str,
                     digest: &str,
                     time: i64| {
      assert_eq!(source, "core/instance");
      assert_eq!(host, "komodo-core-instance");
      assert_eq!(digest, "immutable-digest");
      assert_eq!(time, 123);
      Ok(format!("signature-for-{name}"))
    };
    prepare_core_retry_attempt(
      &mut retry,
      "attempt-b".into(),
      true,
      authorize,
    )
    .unwrap();
    assert_eq!(retry.source_label, "signature-for-attempt-b");
    assert_eq!(retry.source_path, "/private/export");
    assert_eq!(retry.retained_snapshots.len(), 1);
    assert!(retry.retained_snapshots[0].retain_primary);
    assert!(retry.retained_snapshots[0].retain_mirror);
    // Failure in B does not retire either role's diagnostic/good A copy.
    prepare_core_retry_attempt(
      &mut retry,
      "attempt-c".into(),
      true,
      authorize,
    )
    .unwrap();
    assert_eq!(retry.retained_snapshots.len(), 2);
    // A complete C primary retires only older primary copies.
    assert_eq!(
      take_retained_repository_copies(
        &mut retry.retained_snapshots,
        true
      ),
      vec!["original-partial".to_string(), "attempt-b".to_string()]
    );
    assert_eq!(retry.retained_snapshots.len(), 2);
    assert!(
      retry
        .retained_snapshots
        .iter()
        .all(|copy| copy.retain_mirror)
    );
    assert_eq!(retry.created_at, 123);
    assert_eq!(retry.export_digest, "immutable-digest");
  }

  #[test]
  fn manual_backup_admission_rejects_busy_requests_without_queueing()
  {
    let operation = tokio::sync::Mutex::new(());
    let running = admit_manual_backup(&operation).unwrap();
    for _ in 0..64 {
      let error = admit_manual_backup(&operation).unwrap_err();
      assert!(error.to_string().contains("not queued"));
    }
    drop(running);
    assert!(admit_manual_backup(&operation).is_ok());
  }

  #[tokio::test]
  async fn manual_backups_do_not_jump_a_waiting_serialized_operation()
  {
    use futures_util::FutureExt;

    let operation = tokio::sync::Mutex::new(());
    let running = operation.lock().await;
    let next_operation = operation.lock();
    tokio::pin!(next_operation);
    assert!(next_operation.as_mut().now_or_never().is_none());
    drop(running);
    assert!(admit_manual_backup(&operation).is_err());
    let next_operation = next_operation.await;
    assert!(admit_manual_backup(&operation).is_err());
    drop(next_operation);
    assert!(admit_manual_backup(&operation).is_ok());
  }

  #[tokio::test]
  async fn manual_backup_authorization_rejects_disabled_or_demoted_admins()
   {
    let mut user = User {
      enabled: true,
      admin: true,
      ..Default::default()
    };
    assert!(authorize_manual_backup(None, &user).await.is_ok());
    assert!(
      authorize_manual_backup(Some(&BackupTarget::Core), &user)
        .await
        .is_ok()
    );
    user.enabled = false;
    assert!(authorize_manual_backup(None, &user).await.is_err());
    assert!(
      authorize_manual_backup(Some(&BackupTarget::Core), &user)
        .await
        .is_err()
    );
    user.enabled = true;
    user.admin = false;
    assert!(authorize_manual_backup(None, &user).await.is_err());
    assert!(
      authorize_manual_backup(Some(&BackupTarget::Core), &user)
        .await
        .is_err()
    );
  }

  #[tokio::test]
  async fn committed_recovery_remains_complete_when_audit_steps_fail()
  {
    let plan = CoreRecoveryPlan {
      current_database: "old-database".into(),
      validation_database: "recovered-database".into(),
      ..Default::default()
    };
    let mut run = committed_core_recovery_run(&plan);
    let id = run.id.clone();
    let mut warnings = Vec::new();
    for step in
      ["recovery plan cleanup", "recovery audit persistence"]
    {
      core_recovery_audit_step(
        &mut run,
        &mut warnings,
        step,
        async { Err(anyhow!("database unavailable")) },
      )
      .await;
    }
    assert_eq!(run.id, id);
    assert_eq!(run.state, BackupRunState::Complete);
    assert!(!run.cancellable);
    assert!(run.finished_at >= run.started_at);
    assert!(
      run
        .message
        .contains("activation committed; restart scheduled")
    );
    assert!(run.message.contains("old-database"));
    assert!(run.message.contains("recovered-database"));
    assert_eq!(warnings.len(), 2);
    assert!(run.message.contains("recovery plan cleanup failed"));
    assert!(
      run.message.contains("recovery audit persistence failed")
    );
  }

  #[tokio::test]
  async fn successful_recovery_audit_does_not_add_warnings() {
    let mut run =
      committed_core_recovery_run(&CoreRecoveryPlan::default());
    let message = run.message.clone();
    let mut warnings = Vec::new();
    core_recovery_audit_step(
      &mut run,
      &mut warnings,
      "recovery audit persistence",
      async { Ok(()) },
    )
    .await;
    assert!(warnings.is_empty());
    assert_eq!(run.message, message);
    assert_eq!(run.state, BackupRunState::Complete);
  }

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

  #[tokio::test]
  async fn fresh_health_readers_do_not_consume_refresh_admission() {
    let roles = Arc::new(tokio::sync::RwLock::new(()));
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let running = slots.clone().try_acquire_owned().unwrap();
    let read = || {
      load_health_with_refresh_admission(
        roles.clone(),
        slots.clone(),
        || std::future::ready(Ok(("cached health", true))),
      )
    };
    let (first, second) = tokio::join!(read(), read());
    for result in [first, second] {
      let (state, _role, admission) = result.unwrap();
      assert_eq!(state, "cached health");
      assert!(admission.is_none());
    }
    assert!(slots.clone().try_acquire_owned().is_err());
    drop(running);
    assert!(roles.try_write().is_ok());
  }

  #[tokio::test]
  async fn health_rechecks_freshness_after_refresh_admission() {
    let roles = Arc::new(tokio::sync::RwLock::new(()));
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let mut loads = 0;
    let (state, _role, admission) =
      load_health_with_refresh_admission(
        roles,
        slots.clone(),
        || {
          loads += 1;
          // A previous refresh/settings change completed between the reads.
          std::future::ready(Ok((loads, loads == 2)))
        },
      )
      .await
      .unwrap();
    assert_eq!(state, 2);
    assert_eq!(loads, 2);
    assert!(admission.is_none());
    assert!(slots.try_acquire_owned().is_ok());
  }

  #[tokio::test]
  async fn stale_health_refresh_is_exclusive_without_leaking_role_reads()
   {
    let roles = Arc::new(tokio::sync::RwLock::new(()));
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let read = || {
      load_health_with_refresh_admission(
        roles.clone(),
        slots.clone(),
        || std::future::ready(Ok(((), false))),
      )
    };
    let (_, role, admission) = read().await.unwrap();
    let admission = admission.expect("stale health needs admission");
    assert!(read().await.is_err());
    assert!(roles.try_write().is_err());
    drop(role);
    // Busy callers released their own role reads before failing admission.
    assert!(roles.try_write().is_ok());
    assert!(slots.clone().try_acquire_owned().is_err());
    drop(admission);
    assert!(slots.try_acquire_owned().is_ok());
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
    assert!(
      repositories_share_location(
        &local(&actual.join("new/repository").to_string_lossy()),
        &local(&alias.join("new/repository").to_string_lossy()),
      )
      .unwrap()
    );
    assert!(
      !repositories_overlap(
        &local(&actual.join("primary").to_string_lossy()),
        &local(&alias.join("mirror").to_string_lossy()),
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
      "/data/core-secrets/.komodo-core-staging/repository",
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
  fn core_sensitive_work_uses_the_protected_shared_subdirectory() {
    for path in [
      CORE_STAGING_PATH,
      CORE_RECOVERY_STAGING_PATH,
      STACK_MANIFEST_STAGING_PATH,
    ] {
      assert!(Path::new(path).starts_with(CORE_PRIVATE_PATH));
      // This checks the configured layout, independent of the test host's
      // mount namespace. Filesystem alias policy has separate isolated cases.
      assert!(Path::new(path).starts_with("/data"));
      assert!(!Path::new("/data").starts_with(path));
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
    assert!(
      komodo_backup::filesystem::paths_overlap(&alias, &reserved)
        .unwrap()
    );
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
  fn s3_repository_urls_must_be_endpoint_bucket_urls() {
    let repository = |url: &str| BackupRepository {
      name: "S3".into(),
      backend: BackupRepositoryBackend::S3 {
        url: url.into(),
        region: "ca-vancouver-1".into(),
        ..Default::default()
      },
      ..Default::default()
    };
    // A bare provider hostname must never silently become a local path.
    for url in [
      "s3.ca-vancouver-1.megas4.com",
      "https://s3.example.com/bucket/prefix",
      "s3://",
      "s3://endpoint",
      "s3://endpoint/",
      "",
    ] {
      assert!(
        validate_repository_definition(&repository(url)).is_err(),
        "accepted unsafe S3 URL {url:?}"
      );
    }
    for url in [
      "s3://s3.us-east-1.amazonaws.com/my-bucket/vykar",
      "s3://s3.ca-vancouver-1.megas4.com/komodo-backups",
      "s3://s3.ca-vancouver-1.megas4.com/komodo-backups/vykar",
      "s3+http://minio.local:9000/my-bucket/vykar",
    ] {
      assert!(
        validate_repository_definition(&repository(url)).is_ok(),
        "rejected valid S3 URL {url:?}"
      );
    }
  }

  #[test]
  fn external_repositories_still_protect_the_core_private_volume() {
    let settings = BackupSettings {
      primary: BackupRepository {
        backend: BackupRepositoryBackend::Rest {
          url: "https://repository.invalid".into(),
          access_token: Default::default(),
          worker_access_token: Default::default(),
          allow_insecure_http: false,
        },
        ..Default::default()
      },
      ..Default::default()
    };
    let paths = protected_core_paths(&settings, &"a".repeat(64));
    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0].path, CORE_PRIVATE_PATH);
    assert_eq!(paths[0].core_container_id, "a".repeat(64));
    let local_paths = protected_core_paths(
      &BackupSettings::default(),
      &"b".repeat(64),
    );
    assert!(
      local_paths
        .iter()
        .any(|path| path.path == CORE_PRIVATE_PATH)
    );
    assert!(
      local_paths.iter().any(|path| path.path == "/backups/vykar")
    );
  }

  #[test]
  fn intentional_exclusions_only_make_explicit_include_runs_partial()
  {
    let mut settings = BackupSettings::default();
    let target = PeripheryBackupTarget::Volume {
      volume_name: "private-secrets".into(),
    };
    assert!(!excluded_target_was_requested(&settings, &target));
    settings.volume_selection.mode = BackupSelectionMode::Exclude;
    assert!(!excluded_target_was_requested(&settings, &target));
    settings.volume_selection.mode = BackupSelectionMode::Include;
    assert!(excluded_target_was_requested(&settings, &target));
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
      execution: None,
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
    // Older plans without a complete digest cannot approve sampled previews.
    current.path_summary = Some(Default::default());
    assert!(!same_restore_preview(&stored, &current));
    let mut summarized = stored.clone();
    summarized.plan.path_summary = current.path_summary.clone();
    assert!(same_restore_preview(&summarized, &current));
    current.path_summary.as_mut().unwrap().sha256 =
      "changed-unlisted-path".into();
    assert!(!same_restore_preview(&summarized, &current));
    current.path_summary = None;
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
    let mut stack = Stack {
      id: "recovered-stack".into(),
      ..Default::default()
    };
    assert!(
      historical_restore_marker_cleanup("plan", &stack).is_none()
    );
    stack.info.recovery_plan_id = Some("unrelated-plan".into());
    assert!(
      historical_restore_marker_cleanup("plan", &stack).is_none()
    );
    stack.info.recovery_plan_id = Some("plan".into());
    let update =
      historical_restore_marker_cleanup("plan", &stack).unwrap();
    let fields = update.get_document("$unset").unwrap();
    assert_eq!(fields.get_str("info.recovery_plan_id").unwrap(), "");
    assert!(!update.contains_key("$set"));
  }

  #[test]
  fn core_export_excludes_control_and_in_flight_run_state() {
    assert!(!core_export_includes_collection(SETTINGS_COLLECTION));
    assert!(!core_export_includes_collection(RUNS_COLLECTION));
    assert!(!core_export_includes_collection(PLANS_COLLECTION));
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

  #[test]
  fn backup_workers_require_explicit_matching_admin_enrollment() {
    use komodo_client::entities::backup::BackupTrustedWorker;
    let mut settings = BackupSettings::default();
    let mut server = Server {
      id: "server-a".into(),
      ..Default::default()
    };
    server.config.address = "wss://trusted.example".into();
    server.info.public_key = "verified-key".into();
    assert!(
      require_trusted_backup_worker(&settings, &server).is_err()
    );
    settings.trusted_workers.push(BackupTrustedWorker {
      server_id: server.id.clone(),
      address: server.config.address.clone(),
      public_key: server.info.public_key.clone(),
    });
    assert!(
      require_trusted_backup_worker(&settings, &server).is_ok()
    );
    let original = server.clone();
    server.id = "server-created-by-non-admin".into();
    assert!(
      require_trusted_backup_worker(&settings, &server).is_err()
    );
    server = original.clone();
    server.config.address = "wss://attacker.example".into();
    assert!(
      require_trusted_backup_worker(&settings, &server).is_err()
    );
    server = original.clone();
    server.info.public_key = "attacker-key".into();
    assert!(
      require_trusted_backup_worker(&settings, &server).is_err()
    );
    server = original;
    server.config.address.clear();
    settings.trusted_workers[0].address.clear();
    assert!(
      require_trusted_backup_worker(&settings, &server).is_ok()
    );
    server.info.public_key.clear();
    settings.trusted_workers[0].public_key.clear();
    assert!(
      require_trusted_backup_worker(&settings, &server).is_err()
    );
  }

  #[test]
  fn backup_history_query_scopes_targets_before_limiting() {
    let filter = backup_history_filter(
      vec!["stack".into()],
      vec!["server".into()],
    );
    assert_eq!(
      filter,
      doc! { "$or": [
        { "target.type": "Stack", "target.params.stack_id": { "$in": ["stack"] } },
        { "target.type": "Volume", "target.params.server_id": { "$in": ["server"] } },
      ] }
    );
    let empty = backup_history_filter(Vec::new(), Vec::new());
    for arm in empty.get_array("$or").unwrap() {
      let arm = arm.as_document().unwrap();
      let field = if arm.get_str("target.type").unwrap() == "Stack" {
        "target.params.stack_id"
      } else {
        "target.params.server_id"
      };
      assert!(
        arm
          .get_document(field)
          .unwrap()
          .get_array("$in")
          .unwrap()
          .is_empty()
      );
    }
    let target = to_document(&BackupTarget::Stack {
      stack_id: "stack".into(),
    })
    .unwrap();
    assert_eq!(target.get_str("type").unwrap(), "Stack");
    assert_eq!(
      target
        .get_document("params")
        .unwrap()
        .get_str("stack_id")
        .unwrap(),
      "stack"
    );
  }

  #[test]
  fn backup_history_names_cannot_alias_another_resource_id() {
    let mut keys = Vec::new();
    append_backup_history_keys(
      &mut keys,
      "mine".into(),
      "my-stack".into(),
    );
    append_backup_history_keys(
      &mut keys,
      "also-mine".into(),
      "0123456789abcdef01234567".into(),
    );
    assert_eq!(keys, ["mine", "also-mine"]);
  }

  #[tokio::test]
  async fn expired_health_inventory_releases_roles_but_retains_worker_admission()
   {
    let roles = Arc::new(tokio::sync::RwLock::new(()));
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let permit = slots.clone().try_acquire_owned().unwrap();
    let (started, ready) = tokio::sync::oneshot::channel();
    let (finish, wait) = std::sync::mpsc::channel();
    let result = async {
      let _role = roles.clone().read_owned().await;
      run_snapshot_inventory_worker(
        permit,
        std::time::Instant::now(),
        move || {
          let _ = started.send(());
          let _ = wait.recv();
          Ok(())
        },
      )
      .await
    }
    .await;
    assert!(result.is_err());
    ready.await.unwrap();
    // Settings/promotion are available even though the actual read is stuck.
    assert!(roles.try_write().is_ok());
    assert!(slots.clone().try_acquire_owned().is_err());
    finish.send(()).unwrap();
    drop(slots.clone().acquire_owned().await.unwrap());
    assert!(slots.try_acquire_owned().is_ok());
  }

  #[tokio::test]
  async fn completed_inventory_keeps_its_slot_until_consumed() {
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let permit = slots.clone().try_acquire_owned().unwrap();
    let (snapshots, permit) = run_snapshot_inventory_worker(
      permit,
      std::time::Instant::now() + std::time::Duration::from_secs(60),
      || Ok(vec![1]),
    )
    .await
    .unwrap();
    assert!(slots.clone().try_acquire_owned().is_err());
    assert_eq!(snapshots, [1]);
    drop(snapshots);
    drop(permit);
    assert!(slots.try_acquire_owned().is_ok());
  }

  #[tokio::test]
  async fn failed_inventory_releases_its_slot() {
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let permit = slots.clone().try_acquire_owned().unwrap();
    let result = run_snapshot_inventory_worker(
      permit,
      std::time::Instant::now() + std::time::Duration::from_secs(60),
      || anyhow::bail!("inventory failed"),
    )
    .await;
    let result: anyhow::Result<((), _)> = result;
    assert!(
      result.unwrap_err().to_string().contains("inventory failed")
    );
    assert!(slots.try_acquire_owned().is_ok());
  }

  #[tokio::test]
  async fn timed_out_inventory_keeps_its_worker_slot_until_exit() {
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let permit = slots.clone().try_acquire_owned().unwrap();
    let (started, ready) = tokio::sync::oneshot::channel();
    let (finish, wait) = std::sync::mpsc::channel();
    let result = run_snapshot_inventory_worker(
      permit,
      std::time::Instant::now(),
      move || {
        let _ = started.send(());
        let _ = wait.recv();
        Ok(())
      },
    )
    .await;
    assert!(
      result
        .unwrap_err()
        .to_string()
        .contains("exceeded 60 seconds")
    );
    ready.await.unwrap();
    assert!(slots.clone().try_acquire_owned().is_err());
    finish.send(()).unwrap();
    drop(slots.clone().acquire_owned().await.unwrap());
    assert!(slots.try_acquire_owned().is_ok());
  }

  #[tokio::test]
  async fn abandoned_inventory_keeps_its_worker_slot_until_exit() {
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let permit = slots.clone().try_acquire_owned().unwrap();
    let (started, ready) = tokio::sync::oneshot::channel();
    let (finish, wait) = std::sync::mpsc::channel();
    let job = tokio::spawn(run_snapshot_inventory_worker(
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

  fn apply_repository_health_update(
    record: &RepositoryHealthRecord,
    update: Document,
  ) -> RepositoryHealthRecord {
    // The existing-record path deliberately ignores $setOnInsert. A fresh
    // deserialize models a restart using only the persisted health document.
    let mut persisted = to_document(record).unwrap();
    persisted.extend(update.get_document("$set").unwrap().clone());
    database::bson::from_document(persisted).unwrap()
  }

  #[test]
  fn interrupted_maintenance_requires_full_verification_after_restart()
   {
    let now = 2_000_000;
    let healthy = RepositoryHealthRecord {
      healthy: true,
      last_full_verification_at: now - 1,
      ..Default::default()
    };
    assert!(!full_verification_due(&healthy, now, 30));
    let interrupted = apply_repository_health_update(
      &healthy,
      repository_maintenance_started_update(now),
    );
    assert!(interrupted.maintenance_in_progress);
    assert!(full_verification_due(&interrupted, now, 30));
    assert!(!repository_health_is_healthy(&interrupted));

    // A successful inventory cannot clear either latch.
    let inventory = apply_repository_health_update(
      &interrupted,
      doc! { "$set": { "healthy": true, "checked_at": now + 1 } },
    );
    assert!(!repository_health_is_healthy(&inventory));
    assert!(full_verification_due(&inventory, now + 1, 30));

    let sampled = apply_repository_health_update(
      &inventory,
      repository_verification_update(true, false, false, now + 2),
    );
    assert!(sampled.maintenance_in_progress);
    assert!(!repository_health_is_healthy(&sampled));
    assert!(full_verification_due(&sampled, now + 2, 30));

    let fully_verified = apply_repository_health_update(
      &sampled,
      repository_verification_update(true, true, false, now + 3),
    );
    assert!(!fully_verified.maintenance_in_progress);
    assert!(repository_health_is_healthy(&fully_verified));
    assert!(!full_verification_due(&fully_verified, now + 3, 30));
  }

  #[test]
  fn completed_maintenance_clears_only_its_own_uncertainty() {
    let now = 2_000_000;
    let healthy = RepositoryHealthRecord {
      healthy: true,
      last_full_verification_at: now - 1,
      ..Default::default()
    };
    let pending = apply_repository_health_update(
      &healthy,
      repository_maintenance_started_update(now),
    );
    let completed = apply_repository_health_update(
      &pending,
      repository_verification_update(true, false, true, now + 1),
    );
    assert!(repository_health_is_healthy(&completed));
    assert!(!completed.maintenance_in_progress);
    assert_eq!(completed.last_full_verification_at, now - 1);

    let failed = apply_repository_health_update(
      &pending,
      repository_verification_update(false, false, false, now + 1),
    );
    assert!(failed.maintenance_in_progress);
    assert!(failed.verification_failed);
    for maintenance_completed in [false, true] {
      let sampled = apply_repository_health_update(
        &failed,
        repository_verification_update(
          true,
          false,
          maintenance_completed,
          now + 2,
        ),
      );
      assert!(sampled.verification_failed);
      assert!(!repository_health_is_healthy(&sampled));
      assert!(full_verification_due(&sampled, now + 2, 30));
    }
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
      fleet_retry_completion(false, true, false).0,
      BackupRunState::Complete
    );
    assert_eq!(
      fleet_retry_completion(false, false, false).0,
      BackupRunState::Partial
    );
    assert_eq!(
      fleet_retry_completion(true, true, false).0,
      BackupRunState::Cancelled
    );
    assert_eq!(
      fleet_retry_completion(false, true, true).0,
      BackupRunState::Partial
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
    for (completed_attempts, delay) in
      [1, 2, 4, 8, 16, 32, 64, 128].into_iter().enumerate()
    {
      assert_eq!(
        fleet_retry_delay_seconds(completed_attempts as u32),
        Some(delay)
      );
    }
    assert_eq!(
      fleet_retry_delay_seconds(MAX_FLEET_RETRY_ATTEMPTS),
      None
    );
    assert_eq!(fleet_retry_delay_seconds(u32::MAX), None);
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

  #[test]
  fn committed_partial_attempts_are_preserved_without_becoming_complete()
   {
    let partial = VykarBackupRepositoryResult {
      partial: true,
      ..Default::default()
    };
    assert!(!partial.complete);
    assert!(repository_attempt_is_retained(&partial));
    assert!(!repository_attempt_is_retained(
      &VykarBackupRepositoryResult::default()
    ));
    assert!(!repository_attempt_is_retained(
      &VykarBackupRepositoryResult {
        complete: true,
        error: Some("failed write".into()),
        ..Default::default()
      }
    ));
    assert!(repository_attempt_is_retained(
      &VykarBackupRepositoryResult {
        complete: true,
        ..Default::default()
      }
    ));
    let mut retained = vec![VykarRetainedSnapshot {
      snapshot_name: "diagnostic-partial".into(),
      retain_primary: repository_attempt_is_retained(&partial),
      retain_mirror: true,
    }];
    assert_eq!(
      take_retained_repository_copies(&mut retained, true),
      vec!["diagnostic-partial".to_string()]
    );
    assert_eq!(retained.len(), 1);
    assert!(retained[0].retain_mirror);
    assert_eq!(
      take_retained_repository_copies(&mut retained, false),
      vec!["diagnostic-partial".to_string()]
    );
    assert!(retained.is_empty());
  }
}
