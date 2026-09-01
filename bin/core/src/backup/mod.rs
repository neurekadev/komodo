use std::{
  collections::{HashMap, HashSet},
  fs::OpenOptions,
  io::Write,
  os::unix::fs::OpenOptionsExt,
  path::{Path, PathBuf},
  sync::{
    Arc, Mutex, OnceLock, RwLock,
    atomic::{AtomicBool, Ordering},
  },
};

use anyhow::{Context, anyhow};
use database::{
  bson::{doc, to_bson, to_document},
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
  SnapshotDirectoryPage, VykarRepository, normalize_selected_paths,
  snapshot_name,
};
use komodo_client::{
  api::write::PlanBackupRestore,
  entities::{
    backup::{
      BackupRepository, BackupRepositoryBackend, BackupRestorePlan,
      BackupRun, BackupRunState, BackupSecret, BackupSettings,
      BackupSnapshot, BackupStatus, BackupTarget, BackupVolumeTarget,
      CoreRecoveryPlan, selection_includes,
    },
    komodo_timestamp,
    repo::Repo,
    server::Server,
    stack::Stack,
    user::User,
  },
};
use periphery_client::api::backup::{
  CancelVykarOperation, PeripheryBackupTarget, PreflightVykarRestore,
  RunVykarBackup, RunVykarBackupBatch, TransactionalVykarRestore,
  VykarBackupTask,
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
    CORE_RECOVERY_ACTIVATION_PATH,
    LEGACY_CORE_RECOVERY_ACTIVATION_PATH, db_client,
  },
};

mod crypto;

const SETTINGS_ID: &str = "singleton";
const SETTINGS_COLLECTION: &str = "BackupSettings";
const RUNS_COLLECTION: &str = "BackupRun";
const PLANS_COLLECTION: &str = "BackupRestorePlan";
const CORE_RECOVERY_COLLECTION: &str = "CoreRecoveryPlan";
const HEALTH_COLLECTION: &str = "BackupRepositoryHealth";
const CORE_STAGING_PATH: &str = "/data/backups/.komodo-core-staging";
const CORE_INSTANCE_ID_PATH: &str = "/data/keys/backup-instance-id";
const LEGACY_CORE_INSTANCE_ID_PATH: &str =
  "/config/keys/backup-instance-id";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SealedBackupSettings {
  #[serde(rename = "_id")]
  id: String,
  sealed: String,
  updated_at: i64,
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
  #[serde(default)]
  recovered_stack_run_directory: Option<String>,
  #[serde(default)]
  destination_volume_name: Option<String>,
  #[serde(default)]
  create_volume_if_missing: bool,
  /// Immutable source metadata used when this plan creates a recovered Stack.
  #[serde(default)]
  recovered_stack_source: Option<Stack>,
  /// Missing source resources are recoverable only by administrators.
  #[serde(default)]
  source_stack_missing: bool,
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
  paths: Vec<String>,
  target: PeripheryBackupTarget,
  configuration_sha256: String,
  paths_sha256: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct RepositoryHealthRecord {
  #[serde(rename = "_id")]
  id: String,
  healthy: bool,
  checked_at: i64,
  #[serde(default)]
  last_full_verification_at: i64,
  /// Remains set after an integrity check fails until a later check succeeds.
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
  let mut settings = get_settings().await?;
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
  save_settings_inner(proposed, true).await
}

async fn save_settings_inner(
  mut proposed: BackupSettings,
  allow_primary_location_change: bool,
) -> anyhow::Result<BackupSettings> {
  validate_settings(&proposed)?;
  let existing = match settings_collection()
    .find_one(doc! { "_id": SETTINGS_ID })
    .await
    .context("Failed to load existing backup settings")?
  {
    Some(record) => {
      let bytes = crypto::open(&record.sealed)?;
      Some(
        serde_json::from_slice::<BackupSettings>(&bytes)
          .context("Failed to decode sealed backup settings")?,
      )
    }
    None => None,
  };
  if let Some(existing) = &existing {
    if !allow_primary_location_change
      && repository_location(&proposed.primary)
        != repository_location(&existing.primary)
    {
      return Err(anyhow!(
        "Primary repository location cannot be changed after initialization; configure a mirror and use verified promotion"
      ));
    }
    merge_repository_secrets(
      &mut proposed.primary,
      &existing.primary,
    )?;
    match (&mut proposed.mirror, &existing.mirror) {
      (Some(proposed), Some(existing)) => {
        merge_repository_secrets(proposed, existing)?
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
  proposed.updated_at = komodo_timestamp();
  let bytes = serde_json::to_vec(&proposed)?;
  let record = SealedBackupSettings {
    id: SETTINGS_ID.into(),
    sealed: crypto::seal(&bytes)?,
    updated_at: proposed.updated_at,
  };
  settings_collection()
    .update_one(
      doc! { "_id": SETTINGS_ID },
      doc! { "$set": to_document(&record)? },
    )
    .with_options(UpdateOptions::builder().upsert(true).build())
    .await
    .context("Failed to persist sealed backup settings")?;
  invalidate_fleet_retries();
  notify_scheduler();
  let mut redacted = proposed;
  redacted.redact();
  Ok(redacted)
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
  validate_repository_definition(&settings.primary)?;
  if let Some(mirror) = &settings.mirror {
    validate_repository_definition(mirror)?;
    if repository_location(&settings.primary)
      == repository_location(mirror)
    {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CoreRecoveryActivation {
  database: String,
  core_instance_id: String,
}

fn is_backup_manifest_source(path: &str) -> bool {
  Path::new(path)
    .file_name()
    .and_then(|name| name.to_str())
    .and_then(|name| name.strip_prefix("komodo-backup-manifest-"))
    .is_some_and(|suffix| {
      suffix.len() == 6
        && suffix
          .chars()
          .all(|character| character.is_ascii_alphanumeric())
    })
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
) -> anyhow::Result<()> {
  preserve_secret(&mut proposed.passphrase, &existing.passphrase);
  match (&mut proposed.backend, &existing.backend) {
    (
      BackupRepositoryBackend::S3 {
        access_key_id,
        secret_access_key,
        ..
      },
      BackupRepositoryBackend::S3 {
        access_key_id: old_access,
        secret_access_key: old_secret,
        ..
      },
    ) => {
      preserve_secret(access_key_id, old_access);
      preserve_secret(secret_access_key, old_secret);
    }
    (
      BackupRepositoryBackend::Sftp { private_key, .. },
      BackupRepositoryBackend::Sftp {
        private_key: old_key,
        ..
      },
    ) => preserve_secret(private_key, old_key),
    (
      BackupRepositoryBackend::Rest { access_token, .. },
      BackupRepositoryBackend::Rest {
        access_token: old_token,
        ..
      },
    ) => preserve_secret(access_token, old_token),
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
  let valid = match &repository.backend {
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
  if valid {
    Ok(())
  } else {
    Err(anyhow!("Repository credentials are required"))
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
  for (path, migrate) in [
    (CORE_RECOVERY_ACTIVATION_PATH, false),
    (LEGACY_CORE_RECOVERY_ACTIVATION_PATH, true),
  ] {
    match std::fs::read(path) {
      Ok(bytes) => {
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
        if migrate {
          persist_core_recovery_activation(
            &activation.database,
            &activation.core_instance_id,
          )?;
        }
        return Ok(activation.core_instance_id);
      }
      Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
      Err(error) => {
        return Err(error)
          .context("Failed to read Core recovery activation record");
      }
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
) -> anyhow::Result<()> {
  if id.len() != 32
    || !id.chars().all(|character| character.is_ascii_hexdigit())
  {
    return Err(anyhow!("Recovered Core backup identity is invalid"));
  }
  if database.is_empty()
    || !database.chars().all(|character| {
      character.is_ascii_alphanumeric()
        || matches!(character, '_' | '-')
    })
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
  })?)?;
  file.sync_all()?;
  std::fs::rename(temporary, destination)?;
  std::fs::File::open(parent)?.sync_all()?;
  Ok(())
}

fn core_cache_dir() -> anyhow::Result<PathBuf> {
  let directory = PathBuf::from("/data/backups/.komodo-vykar-cache");
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
    &settings.advanced,
  )
}

/// Convert Core-local storage to the embedded authenticated REST endpoint for
/// Periphery. Other backends are used directly on the worker.
fn repository_for_periphery(
  repository: &BackupRepository,
  mirror: bool,
) -> anyhow::Result<BackupRepository> {
  let BackupRepositoryBackend::CoreLocal { path } =
    &repository.backend
  else {
    return Ok(repository.clone());
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
      allow_insecure_http: core_config().host.starts_with("http://"),
    },
    passphrase: repository.passphrase.clone(),
  })
}

pub async fn initialize_repositories() -> anyhow::Result<BackupRun> {
  let _repository_roles =
    repository_role_barrier().clone().read_owned().await;
  let settings = get_settings().await?;
  let run = new_run(None, "Initializing repositories").await?;
  let result = async {
    for repository in
      std::iter::once(&settings.primary).chain(settings.mirror.iter())
    {
      let repository = repository.clone();
      let settings = settings.clone();
      tokio::task::spawn_blocking(move || {
        core_repository(&repository, &settings)?.init()
      })
      .await
      .context("Vykar initialization worker failed")??;
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

async fn new_run(
  target: Option<BackupTarget>,
  message: &str,
) -> anyhow::Result<BackupRun> {
  let run = BackupRun {
    id: Uuid::new_v4().to_string(),
    target,
    state: BackupRunState::Running,
    message: message.into(),
    started_at: komodo_timestamp(),
    ..Default::default()
  };
  runs_collection().insert_one(&run).await?;
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
  Ok(run)
}

pub async fn status() -> anyhow::Result<BackupStatus> {
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
  let active_run = recent_runs
    .iter()
    .find(|run| {
      matches!(
        run.state,
        BackupRunState::Queued | BackupRunState::Running
      )
    })
    .cloned();
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
  let settings = get_settings().await?;
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
    active_run,
    recent_runs,
    next_run_at: next_scheduled_run().unwrap_or_default(),
    primary_healthy,
    mirror_healthy,
    mirror_lagging_snapshots,
    last_full_verification_at: previous_primary
      .last_full_verification_at,
    critical_alert: critical_alert().read().unwrap().clone(),
  })
}

fn critical_alert() -> &'static RwLock<Option<String>> {
  static ALERT: OnceLock<RwLock<Option<String>>> = OnceLock::new();
  ALERT.get_or_init(Default::default)
}

const MAINTENANCE_ALERT_PREFIX: &str = "Backup maintenance blocked:";

fn clear_maintenance_alert() {
  let mut alert = critical_alert().write().unwrap();
  if alert.as_deref().is_some_and(|message| {
    message.starts_with(MAINTENANCE_ALERT_PREFIX)
  }) {
    *alert = None;
  }
}

/// Blocks application mutations only while Core creates immutable export
/// staging. Uploads happen after the write guard is released.
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

fn cancellation_tokens()
-> &'static Mutex<HashMap<String, Arc<AtomicBool>>> {
  static TOKENS: OnceLock<Mutex<HashMap<String, Arc<AtomicBool>>>> =
    OnceLock::new();
  TOKENS.get_or_init(Default::default)
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
    Ok((inventory.snapshots, inventory.hidden))
  })
  .await
  .context("Vykar inventory worker failed")?
}

pub async fn list_directory(
  snapshot: String,
  parent: String,
  search: String,
  page: u64,
  limit: u64,
) -> anyhow::Result<SnapshotDirectoryPage> {
  let settings = get_settings().await?;
  tokio::task::spawn_blocking(move || {
    core_repository(&settings.primary, &settings)?
      .list_directory(&snapshot, &parent, &search, page, limit)
  })
  .await
  .context("Vykar tree worker failed")?
}

pub async fn run_backup(
  target: Option<BackupTarget>,
) -> anyhow::Result<BackupRun> {
  let _operation = backup_operation_lock().lock().await;
  let _repository_roles =
    repository_role_barrier().clone().read_owned().await;
  let run = new_run(target.clone(), "Backup running").await?;
  let run_id = run.id.clone();
  let _cancellation = register_cancellation_token(&run_id);
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
    Some(target) => run_target(&settings, &run, target).await,
    None => run_fleet(&settings, &run).await,
  };
  let finished = if cancellation_requested(&run_id) {
    finish_run(
      run,
      BackupRunState::Cancelled,
      "Cancellation requested",
    )
    .await
  } else {
    match result {
      Ok(partial) if partial => {
        finish_run(
          run,
          BackupRunState::Partial,
          "Backup completed partially",
        )
        .await
      }
      Ok(_) => {
        finish_run(run, BackupRunState::Complete, "Backup complete")
          .await
      }
      Err(error) => {
        finish_run(run, BackupRunState::Failed, format!("{error:#}"))
          .await
      }
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
        match get_settings().await {
          Ok(settings) => match run_maintenance(settings).await {
            Ok(()) => clear_maintenance_alert(),
            Err(error) => {
              error!(
                "Backup repository maintenance failed: {error:#}"
              );
              *critical_alert().write().unwrap() =
                Some(format!("{MAINTENANCE_ALERT_PREFIX} {error:#}"));
            }
          },
          Err(error) => error!(
            "Failed to load backup maintenance settings: {error:#}"
          ),
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

async fn run_maintenance(
  settings: BackupSettings,
) -> anyhow::Result<()> {
  let _operation = backup_operation_lock().lock().await;
  let _repository_roles =
    repository_role_barrier().clone().read_owned().await;
  let repositories =
    std::iter::once(("primary", settings.primary.clone()))
      .chain(settings.mirror.clone().map(|mirror| ("mirror", mirror)))
      .collect::<Vec<_>>();
  for (health_id, repository) in repositories {
    let previous = health_collection()
      .find_one(doc! { "_id": health_id })
      .await?
      .unwrap_or_default();
    let full_due = previous.last_full_verification_at == 0
      || komodo_timestamp() - previous.last_full_verification_at
        >= settings.advanced.full_verify_every_days.max(1) as i64
          * 24
          * 60
          * 60
          * 1000;
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
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
      let vykar = core_repository(&repository, &settings_for_worker)?;
      let inventory = vykar.list_snapshots()?;
      if inventory.hidden > 0 {
        return Err(anyhow!(
          "Vykar hid {} unreadable snapshot(s); destructive maintenance is blocked",
          inventory.hidden
        ));
      }
      let mut retention = HashMap::new();
      for snapshot in inventory.snapshots {
          let keep = match snapshot.target {
            BackupTarget::Core => settings_for_worker.core_keep_last,
            BackupTarget::Stack { .. } => settings_for_worker.stack_keep_last,
            BackupTarget::Volume { .. } => settings_for_worker.volume_keep_last,
          BackupTarget::Unbound { .. } => continue,
        };
        retention.insert(snapshot.source_label, keep);
      }
      let pruned = vykar.prune_complete_snapshots(&retention)?;
      if pruned.snapshots_deleted == 0 {
        return Ok(());
      }
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
    })
    .await
    .context("Vykar maintenance worker failed")??;
  }
  Ok(())
}

async fn run_fleet(
  settings: &BackupSettings,
  run: &BackupRun,
) -> anyhow::Result<bool> {
  ensure_not_cancelled(&run.id)?;
  *fleet_generation().write().unwrap() = run.id.clone();
  let mut targets = Vec::new();
  let mut discovery_retry_servers = HashSet::new();
  let mut partial = false;
  if settings.core_enabled {
    targets.push(BackupTarget::Core);
  }
  if settings.stacks_enabled {
    let stacks =
      find_collect(&db_client().stacks, None, None).await?;
    targets.extend(stacks.into_iter().filter_map(|stack| {
      (stack.config.swarm_id.is_empty()
        && selection_includes(
          settings.stack_selection.mode,
          &settings.stack_selection.stack_ids,
          &stack.id,
        ))
      .then_some(BackupTarget::Stack { stack_id: stack.id })
    }));
  }
  if settings.volumes_enabled {
    // Volume inventory comes from every configured Periphery at run time, so
    // unmanaged local named volumes participate automatically.
    let servers =
      find_collect(&db_client().servers, None, None).await?;
    for server in servers {
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
        .filter(|volume| volume.driver == "local")
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
          targets.push(BackupTarget::Volume {
            server_id: identity.server_id,
            volume_name: identity.volume_name,
          });
        }
      }
    }
  }
  if targets.contains(&BackupTarget::Core) {
    match backup_core(settings, run).await {
      Ok(false) => {}
      Ok(true) => {
        partial = true;
        spawn_core_retry(settings.clone(), run.clone());
      }
      Err(error) => {
        warn!("Core backup failed: {error:#}");
        partial = true;
        spawn_core_retry(settings.clone(), run.clone());
      }
    }
  }
  ensure_not_cancelled(&run.id)?;

  let mut by_server: HashMap<String, Vec<BackupTarget>> =
    HashMap::new();
  for target in targets
    .into_iter()
    .filter(|target| *target != BackupTarget::Core)
  {
    let server_id = match &target {
      BackupTarget::Stack { stack_id } => {
        resource::get::<Stack>(stack_id).await?.config.server_id
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
          match build_node_backup_tasks(&targets, &run.id).await {
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
          spawn_node_retry(
            settings.clone(),
            run.clone(),
            server_id,
            targets,
            outcome.retry_tasks,
            false,
          );
        }
      }
      Err(error) => {
        warn!("Backup node {server_id} failed: {error:#}");
        partial = true;
        spawn_node_retry(
          settings.clone(),
          run.clone(),
          server_id,
          targets,
          tasks,
          refresh_targets,
        );
      }
    }
  }
  Ok(partial)
}

fn fleet_generation() -> &'static RwLock<String> {
  static GENERATION: OnceLock<RwLock<String>> = OnceLock::new();
  GENERATION.get_or_init(Default::default)
}

fn spawn_node_retry(
  settings: BackupSettings,
  run: BackupRun,
  server_id: String,
  mut targets: Vec<BackupTarget>,
  mut tasks: Vec<VykarBackupTask>,
  mut refresh_targets: bool,
) {
  tokio::spawn(async move {
    let mut retry = 0_u64;
    loop {
      if *fleet_generation().read().unwrap() != run.id {
        return;
      }
      let seconds =
        2_u64.saturating_pow(retry.min(8) as u32).min(300);
      tokio::time::sleep(std::time::Duration::from_secs(seconds))
        .await;
      if *fleet_generation().read().unwrap() != run.id {
        return;
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
        return;
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
          &refreshed, &run.id,
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
            queue_maintenance();
            return;
          }
          continue;
        }
      }
      if *fleet_generation().read().unwrap() != run.id {
        return;
      }
      let _repository_roles =
        repository_role_barrier().clone().read_owned().await;
      if *fleet_generation().read().unwrap() != run.id {
        return;
      }
      match run_node_batch(&settings, &run, &server_id, tasks.clone())
        .await
      {
        Ok(outcome) if outcome.retry_blocked => return,
        Ok(outcome)
          if outcome.retry_tasks.is_empty() && targets.is_empty() =>
        {
          if !outcome.partial {
            queue_maintenance();
          }
          return;
        }
        Ok(outcome) => {
          tasks = outcome.retry_tasks;
          warn!(
            "Backup retry {retry} for node {server_id} remained partial"
          );
        }
        Err(error) => warn!(
          "Backup retry {retry} for node {server_id} failed: {error:#}"
        ),
      }
    }
  });
}

fn spawn_core_retry(settings: BackupSettings, run: BackupRun) {
  tokio::spawn(async move {
    let mut retry = 0_u32;
    loop {
      if *fleet_generation().read().unwrap() != run.id {
        let retry =
          core_repository_retries().lock().unwrap().remove(&run.id);
        if let Some(retry) = retry {
          let _ = tokio::fs::remove_dir_all(retry.staging).await;
        }
        return;
      }
      let seconds = 2_u64.saturating_pow(retry.min(8)).min(300);
      tokio::time::sleep(std::time::Duration::from_secs(seconds))
        .await;
      if *fleet_generation().read().unwrap() != run.id {
        let retry =
          core_repository_retries().lock().unwrap().remove(&run.id);
        if let Some(retry) = retry {
          let _ = tokio::fs::remove_dir_all(retry.staging).await;
        }
        return;
      }
      retry = retry.saturating_add(1);
      let _operation = backup_operation_lock().lock().await;
      if *fleet_generation().read().unwrap() != run.id {
        continue;
      }
      let _repository_roles =
        repository_role_barrier().clone().read_owned().await;
      if *fleet_generation().read().unwrap() != run.id {
        let retry =
          core_repository_retries().lock().unwrap().remove(&run.id);
        if let Some(retry) = retry {
          let _ = tokio::fs::remove_dir_all(retry.staging).await;
        }
        return;
      }
      match backup_core(&settings, &run).await {
        Ok(false) => {
          queue_maintenance();
          return;
        }
        Ok(true) => {
          warn!("Core backup retry {retry} remained partial")
        }
        Err(error) => {
          warn!("Core backup retry {retry} failed: {error:#}")
        }
      }
    }
  });
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
    .filter(|volume| volume.driver == "local")
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
  if settings.volume_selection.mode
    == komodo_client::entities::backup::BackupSelectionMode::Include
  {
    targets.extend(
      settings
        .volume_selection
        .volumes
        .iter()
        .filter(|volume| volume.server_id == server_id)
        .map(|volume| BackupTarget::Volume {
          server_id: volume.server_id.clone(),
          volume_name: volume.volume_name.clone(),
        }),
    );
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
) -> anyhow::Result<NodeTaskPreparation> {
  let core_instance_id = core_instance_id()?;
  let mut tasks = Vec::new();
  let mut failed_targets = Vec::new();
  let mut errors = Vec::new();
  for target in targets {
    let source_label = target.source_label(core_instance_id);
    let task = async {
      let periphery_target = match target {
        BackupTarget::Stack { stack_id } => {
          let stack = resource::get::<Stack>(stack_id).await?;
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
      anyhow::Ok(Some(VykarBackupTask {
        target: periphery_target,
        source_label: source_label.clone(),
        snapshot_name: snapshot_name(
          match target {
            BackupTarget::Stack { .. } => "stack",
            BackupTarget::Volume { .. } => "volume",
            _ => "backup",
          },
          run_id,
        ),
        mirror_only: false,
        primary_only: false,
        superseded_snapshot_names: Vec::new(),
      }))
    }
    .await;
    match task {
      Ok(Some(task)) => tasks.push(task),
      Ok(None) => {}
      Err(error) => {
        failed_targets.push(target.clone());
        errors.push(format!("{source_label}: {error:#}"));
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

fn retry_tasks_after_unknown_result(
  tasks: Vec<VykarBackupTask>,
  run_id: &str,
) -> Vec<VykarBackupTask> {
  tasks
    .into_iter()
    .map(|mut task| {
      task
        .superseded_snapshot_names
        .push(task.snapshot_name.clone());
      task.mirror_only = false;
      task.primary_only = false;
      task.snapshot_name = fresh_retry_snapshot_name(&task, run_id);
      task
    })
    .collect()
}

async fn delete_node_snapshot_copies(
  settings: &BackupSettings,
  snapshot_name: String,
) {
  let repositories = std::iter::once(settings.primary.clone())
    .chain(settings.mirror.clone())
    .collect::<Vec<_>>();
  let settings = settings.clone();
  let cleanup = tokio::task::spawn_blocking(move || {
    for repository in repositories {
      if let Err(error) = core_repository(&repository, &settings)
        .and_then(|repository| {
          repository.delete_snapshot_if_present(&snapshot_name)
        })
      {
        warn!(
          "Could not remove superseded node snapshot {snapshot_name}: {error:#}"
        );
      }
    }
  })
  .await;
  if let Err(error) = cleanup {
    warn!("Node snapshot cleanup worker failed: {error}");
  }
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
      komodo_version: env!("CARGO_PKG_VERSION").into(),
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
        retry_tasks: retry_tasks_after_unknown_result(tasks, &run.id),
        retry_blocked: false,
      });
    }
  };
  if !response.restart_errors.is_empty() {
    *critical_alert().write().unwrap() = Some(format!(
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
      retry_tasks.push(task);
      continue;
    };
    let primary_complete =
      result.primary.complete && result.primary.error.is_none();
    let mirror_complete = settings.mirror.is_none()
      || result.mirror.as_ref().is_some_and(|mirror| {
        mirror.complete && mirror.error.is_none()
      });
    let current_complete = primary_complete
      || settings.mirror.is_some() && mirror_complete;
    if primary_complete && mirror_complete {
      for superseded in
        std::mem::take(&mut task.superseded_snapshot_names)
      {
        delete_node_snapshot_copies(settings, superseded).await;
      }
    } else {
      let attempted = task.snapshot_name.clone();
      if current_complete {
        for superseded in
          std::mem::take(&mut task.superseded_snapshot_names)
        {
          delete_node_snapshot_copies(settings, superseded).await;
        }
        task.superseded_snapshot_names.push(attempted);
      } else {
        // Neither repository has an authoritative copy. Remove any committed
        // partials and retain the previous successful attempt, if one exists.
        delete_node_snapshot_copies(settings, attempted).await;
      }
      // A repository-specific retry against rediscovered live paths could put
      // different bytes under one name. Every retry is therefore a fresh,
      // node-quiesced attempt against both repositories; the previous good
      // attempt is retained until its replacement commits somewhere.
      task.mirror_only = false;
      task.primary_only = false;
      task.snapshot_name = fresh_retry_snapshot_name(&task, &run.id);
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
  let manifest = serde_json::json!({
    "schema": "komodo.core-export/v1",
    "version": env!("CARGO_PKG_VERSION"),
    "core_instance_id": core_instance_id()?,
    "collections": exported_collections,
    "created_at": komodo_timestamp(),
  });
  tokio::fs::write(
    staging.join("komodo-core-manifest.json"),
    serde_json::to_vec_pretty(&manifest)?,
  )
  .await?;
  let label = BackupTarget::Core.source_label(core_instance_id()?);
  let name = snapshot_name("core", &run.id);
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
  let Some(mirror) = settings.mirror.clone() else {
    let _ = tokio::fs::remove_dir_all(&staging).await;
    return primary_result.map(|result| result.partial);
  };
  let mirror_result = write_core_repository_snapshot(
    mirror,
    settings,
    &retry,
    cancellation,
    false,
  )
  .await;
  retry.retry_mirror =
    !matches!(&mirror_result, Ok(result) if !result.partial);
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
    (_, Err(error)) => Err(error),
    (Ok(primary), Ok(mirror)) => {
      Ok(primary.partial || mirror.partial)
    }
  }
}

async fn backup_stack(
  settings: &BackupSettings,
  run: &BackupRun,
  stack_id: &str,
) -> anyhow::Result<bool> {
  let stack = resource::get::<Stack>(stack_id).await?;
  if !stack.config.swarm_id.is_empty() {
    return Err(anyhow!(
      "Swarm stacks are not supported by backup v1"
    ));
  }
  let server =
    resource::get::<Server>(&stack.config.server_id).await?;
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
      hostname: format!("komodo-periphery-{}", server.id),
      source_label: BackupTarget::Stack {
        stack_id: stack_id.into(),
      }
      .source_label(core_instance_id()?),
      snapshot_name: snapshot_name("stack", &run.id),
      run_id: run.id.clone(),
      komodo_version: env!("CARGO_PKG_VERSION").into(),
      stop_containers: settings.stop_containers,
      mirror_only: false,
      primary_only: false,
    })
    .await?;
  if !response.restart_errors.is_empty() {
    *critical_alert().write().unwrap() = Some(format!(
      "Containers could not be restarted after stack backup: {}",
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
  let server = resource::get::<Server>(server_id).await?;
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
      hostname: format!("komodo-periphery-{}", server.id),
      source_label: BackupTarget::Volume {
        server_id: server_id.into(),
        volume_name: volume_name.into(),
      }
      .source_label(core_instance_id()?),
      snapshot_name: snapshot_name("volume", &run.id),
      run_id: run.id.clone(),
      komodo_version: env!("CARGO_PKG_VERSION").into(),
      stop_containers: settings.stop_containers,
      mirror_only: false,
      primary_only: false,
    })
    .await?;
  if !response.restart_errors.is_empty() {
    *critical_alert().write().unwrap() = Some(format!(
      "Containers could not be restarted after volume backup: {}",
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
  if let BackupTarget::Stack { stack_id } = &snapshot.target
    && Stack::coll()
      .find_one(id_or_name_filter(stack_id))
      .await?
      .is_none()
  {
    if !user.admin {
      return Err(anyhow!(
        "Only administrators can recover a snapshot whose Stack resource was deleted"
      ));
    }
  } else {
    authorize_target(&snapshot.target, user, level).await?;
  }
  Ok(snapshot)
}

async fn snapshot_stack_source(
  snapshot: &BackupSnapshot,
) -> anyhow::Result<(Stack, bool)> {
  let BackupTarget::Stack { stack_id } = &snapshot.target else {
    return Err(anyhow!("Snapshot is not a Stack backup"));
  };
  if let Some(stack) =
    Stack::coll().find_one(id_or_name_filter(stack_id)).await?
  {
    return Ok((stack, false));
  }

  let manifest_source = snapshot
    .source_paths
    .iter()
    .find(|path| is_backup_manifest_source(path))
    .context("Stack snapshot has no embedded recovery manifest")?
    .clone();
  let staging = PathBuf::from("/data/backups/.komodo-stack-manifest")
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
    || manifest.run_id.is_empty()
    || manifest.source_label != snapshot.source_label
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
  if configuration_sha256 != manifest.configuration_sha256
    || paths_sha256 != manifest.paths_sha256
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
  Ok((*stack, true))
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
  let (snapshot_stack, source_stack_missing) =
    if matches!(&snapshot.target, BackupTarget::Stack { .. }) {
      let (stack, missing) = snapshot_stack_source(&snapshot).await?;
      (Some(stack), missing)
    } else {
      (None, false)
    };
  let destination_server_id = match destination_server_id {
    Some(destination) => Some(destination),
    None => match &snapshot.target {
      BackupTarget::Volume { server_id, .. } => {
        Some(server_id.clone())
      }
      BackupTarget::Stack { .. } => snapshot_stack
        .as_ref()
        .map(|stack| stack.config.server_id.clone()),
      BackupTarget::Core => None,
      BackupTarget::Unbound { .. } => None,
    },
  };
  let mut publish = Vec::new();
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
      let stack = snapshot_stack
        .as_ref()
        .context("Stack snapshot metadata is missing")?;
      let destination = destination_server_id
        .clone()
        .unwrap_or_else(|| stack.config.server_id.clone());
      let recovering_stack =
        source_stack_missing || destination != stack.config.server_id;
      if destination != stack.config.server_id && !user.admin {
        return Err(anyhow!(
          "Cross-node Stack restore with host path mappings is administrator only"
        ));
      }
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
      if destination != stack.config.server_id
        && bind_path_mappings.is_empty()
      {
        return Err(anyhow!(
          "Cross-node stack restore requires explicit bind path mappings"
        ));
      }
      if recovering_stack && !selected_paths.is_empty() {
        return Err(anyhow!(
          "Recovered Stack creation requires the complete snapshot"
        ));
      }
      let source_paths = snapshot
        .source_paths
        .iter()
        .filter(|path| !is_backup_manifest_source(path))
        .cloned()
        .collect::<Vec<_>>();
      if source_paths.is_empty() {
        return Err(anyhow!(
          "Snapshot does not contain a Stack run directory"
        ));
      }
      for source in source_paths {
        let destination_path = if destination
          == stack.config.server_id
        {
          source.clone()
        } else {
          bind_path_mappings
            .get(&source)
            .with_context(|| {
              format!(
                "Cross-node Stack restore is missing a destination mapping for '{source}'"
              )
            })?
            .clone()
        };
        if !Path::new(&destination_path).is_absolute() {
          return Err(anyhow!(
            "Restore destination must be absolute: {destination_path}"
          ));
        }
        publish.push(
          periphery_client::api::backup::RestorePublishPath {
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
        .find(|path| !is_backup_manifest_source(path))
        .context("Snapshot does not contain a volume source path")?;
      publish.push(
        periphery_client::api::backup::RestorePublishPath {
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
  let volume_destination_changed = matches!(
    &snapshot.target,
    BackupTarget::Volume {
      server_id,
      volume_name,
    } if destination_server_id.as_deref() != Some(server_id)
      || destination_volume_name.as_ref().is_some_and(|name| name != volume_name)
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
      advanced: settings.advanced,
      hostname: format!("komodo-periphery-{}", server.id),
      snapshot_name: snapshot.name.clone(),
      selected_paths: selected_paths.clone(),
      publish: publish.clone(),
    })
    .await?;
  if volume_destination_changed
    && !confirm_existing_volume
    && preflight.destination_exists
  {
    return Err(anyhow!(
      "Cross-node restore into an existing volume requires explicit confirmation"
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
      recovered_stack_run_directory,
      destination_volume_name: destination_volume_name.clone(),
      create_volume_if_missing,
      recovered_stack_source,
      source_stack_missing,
    })
    .await?;
  Ok(plan)
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

pub async fn execute_restore(
  plan_id: &str,
  user: &User,
) -> anyhow::Result<BackupRun> {
  let _operation = backup_operation_lock().lock().await;
  let _repository_roles =
    repository_role_barrier().clone().read_owned().await;
  let stored = plans_collection()
    .find_one(doc! { "_id": plan_id, "created_by": &user.id })
    .await?
    .context("Restore plan does not exist")?;
  if stored.plan.expires_at < komodo_timestamp() {
    plans_collection()
      .delete_one(doc! { "_id": &stored.id })
      .await?;
    return Err(anyhow!("Restore plan has expired"));
  }
  if stored.source_stack_missing {
    if !user.admin {
      return Err(anyhow!(
        "Only administrators can recover a snapshot whose Stack resource was deleted"
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
  if source_server_id.as_deref() != Some(server_id.as_str()) {
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
    if Stack::coll()
      .find_one(doc! { "name": recovered_name })
      .await?
      .is_some()
    {
      return Err(anyhow!(
        "A Stack named '{recovered_name}' now exists; create a new preflight"
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
  let run =
    new_run(Some(stored.plan.source.clone()), "Restore running")
      .await?;
  let operation = async {
    let settings = get_settings().await?;
    let recovered_run_directory =
      stored.recovered_stack_run_directory.clone().or_else(|| {
        stored.publish.first().map(|path| path.destination.clone())
      });
    let response = periphery_client(&server)
      .await?
      .request(TransactionalVykarRestore {
        target,
        repository: repository_for_periphery(
          &settings.primary,
          false,
        )?,
        advanced: settings.advanced,
        hostname: format!("komodo-periphery-{}", server.id),
        snapshot_name: stored.plan.snapshot,
        selected_paths: stored.plan.selected_paths,
        publish: stored.publish,
        journal_id: run.id.clone(),
        volume_restore_plan_id: stored.id.clone(),
        create_volume_if_missing: stored.create_volume_if_missing,
      })
      .await?;
    if let Some(error) = response.critical_error {
      *critical_alert().write().unwrap() = Some(error.clone());
      return finish_run(
        run.clone(),
        BackupRunState::Failed,
        error,
      )
      .await;
    }
    if !response.complete {
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
    if let Some(stack) = recovered_stack {
      let name = stored
        .recovered_stack_name
        .context("Recovered stack name is missing")?;
      let mut config:
        komodo_client::entities::stack::PartialStackConfig =
        stack.config.into();
      config.server_id = Some(server_id);
      config.swarm_id = Some(String::new());
      config.project_name = Some(name.clone());
      config.files_on_host = Some(true);
      config.run_directory = recovered_run_directory;
      config.repo = Some(String::new());
      config.linked_repo = Some(String::new());
      if let Err(error) =
        resource::create::<Stack>(&name, config, None, user).await
      {
        if Stack::coll()
          .find_one(doc! { "name": &name })
          .await?
          .is_none()
        {
          return Err(error.error);
        }
        warn!(
          "Recovered Stack '{name}' was inserted but post-create bookkeeping failed: {:#}",
          error.error
        );
      }
    }
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
  let path = Path::new(CORE_STAGING_PATH);
  match std::fs::remove_dir_all(path) {
    Ok(()) => {}
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
    Err(error) => {
      return Err(
        anyhow::Error::new(error)
          .context("Failed to purge abandoned Core staging"),
      );
    }
  }
  std::fs::create_dir_all(path)
    .context("Failed to create Core staging directory")?;
  Ok(())
}

async fn cleanup_expired_restore_plans() -> anyhow::Result<()> {
  plans_collection()
    .delete_many(
      doc! { "plan.expires_at": { "$lt": komodo_timestamp() } },
    )
    .await?;
  Ok(())
}

async fn cleanup_expired_core_recovery_plans() -> anyhow::Result<()> {
  let _operation = core_recovery_operation_lock().lock().await;
  let expired = find_collect(
    &core_recovery_collection(),
    doc! { "plan.expires_at": { "$lt": komodo_timestamp() } },
    None,
  )
  .await?;
  for stored in expired {
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
    core_recovery_collection()
      .delete_one(doc! { "_id": &stored.id })
      .await?;
  }
  Ok(())
}

pub async fn plan_core_recovery(
  snapshot_name: &str,
  created_by: String,
) -> anyhow::Result<CoreRecoveryPlan> {
  cleanup_expired_core_recovery_plans().await?;
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
  let staging = PathBuf::from("/data/backups/.komodo-core-recovery")
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
  let manifest: serde_json::Value =
    serde_json::from_slice(&tokio::fs::read(&manifest_path).await?)?;
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
    != env!("CARGO_PKG_VERSION").split('.').next()
  {
    return Err(anyhow!(
      "Core backup major version {backup_version} is incompatible with {}",
      env!("CARGO_PKG_VERSION")
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
    find_core_restore_layout(&staging)?;
  let current_database = db_client().db.name().to_string();
  let validation_database = format!(
    "{}_recovery_{}",
    current_database,
    &Uuid::new_v4().simple().to_string()[..12]
  );
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
  let stored = core_recovery_collection()
    .find_one(doc! { "_id": plan_id, "created_by": user_id })
    .await?
    .context("Core recovery plan does not exist")?;
  if stored.plan.expires_at < komodo_timestamp() {
    db_client()
      .db
      .client()
      .database(&stored.plan.validation_database)
      .drop()
      .await?;
    core_recovery_collection()
      .delete_one(doc! { "_id": &stored.id })
      .await?;
    return Err(anyhow!("Core recovery plan has expired"));
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
  persist_core_recovery_activation(
    &stored.plan.validation_database,
    &stored.recovered_core_instance_id,
  )?;
  let delete_result = core_recovery_collection()
    .delete_one(doc! { "_id": &stored.id })
    .await;
  // Once the durable pointer is published, restart even if recording the
  // final audit result encounters a transient database error.
  tokio::spawn(async {
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    std::process::exit(75);
  });
  delete_result?;
  let run =
    new_run(Some(BackupTarget::Core), "Core recovery activating")
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
  let run = new_run(None, "Repository verification running").await?;
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

pub async fn promote_mirror() -> anyhow::Result<BackupSettings> {
  // Keep the exclusive role barrier from the start of mandatory verification
  // through the settings swap. No unverified mirror write can land in between.
  let _repository_roles =
    repository_role_barrier().clone().write_owned().await;
  let mut settings = get_settings().await?;
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
    let primary = core_repository(&primary, &inventory_settings)?
      .list_snapshots()?;
    let mirror = core_repository(
      &mirror_for_inventory,
      &inventory_settings,
    )?
    .list_snapshots()?;
    if primary.hidden > 0 || mirror.hidden > 0 {
      return Err(anyhow!(
        "Promotion blocked because a repository inventory is incomplete"
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
  save_settings_after_promotion(settings).await
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
    append_only: false,
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
      if let Err(error) = cleanup_expired_restore_plans().await {
        error!("Failed to clean expired restore plans: {error:#}");
      }
      if let Err(error) = cleanup_expired_core_recovery_plans().await
      {
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
      if let Err(error) = run_backup(None).await {
        error!("Scheduled fleet backup failed: {error:#}");
      }
    }
  });
}

#[cfg(test)]
mod tests {
  use super::*;

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
        allow_insecure_http: false,
      },
      ..Default::default()
    };
    assert_eq!(
      repository_location(&rest("https://backup.example/repo")),
      repository_location(&rest("https://backup.example/repo/"))
    );
  }

  #[test]
  fn selected_restore_publishes_only_selected_subtrees() {
    let roots =
      vec![periphery_client::api::backup::RestorePublishPath {
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
  }

  #[test]
  fn restore_destinations_must_not_overlap() {
    let publish = vec![
      periphery_client::api::backup::RestorePublishPath {
        snapshot_path: "source/one".into(),
        destination: "/restore/app".into(),
      },
      periphery_client::api::backup::RestorePublishPath {
        snapshot_path: "source/two".into(),
        destination: "/restore/app/data".into(),
      },
    ];
    assert!(validate_non_overlapping_destinations(&publish).is_err());
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
  fn core_export_excludes_control_and_in_flight_run_state() {
    assert!(!core_export_includes_collection(SETTINGS_COLLECTION));
    assert!(!core_export_includes_collection(RUNS_COLLECTION));
    assert!(core_export_includes_collection("Stack"));
  }

  #[test]
  fn manifest_source_matching_requires_the_exact_tempdir_pattern() {
    assert!(is_backup_manifest_source(
      "/tmp/komodo-backup-manifest-aB12z9"
    ));
    assert!(!is_backup_manifest_source(
      "/var/lib/docker/volumes/data-komodo-backup-manifest-aB12z9/_data"
    ));
    assert!(!is_backup_manifest_source(
      "/tmp/komodo-backup-manifest-not-a-tempdir"
    ));
  }

  #[test]
  fn complete_primary_requires_a_complete_mirror_copy() {
    assert!(!mirror_copy_is_sufficient(false, None));
    assert!(!mirror_copy_is_sufficient(false, Some(true)));
    assert!(mirror_copy_is_sufficient(false, Some(false)));
    assert!(mirror_copy_is_sufficient(true, Some(true)));
  }
}
