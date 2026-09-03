use std::{
  collections::{BTreeMap, BTreeSet, HashMap, HashSet},
  fs::OpenOptions,
  io::{Read, Write},
  os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
  path::{Path, PathBuf},
  sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicBool, Ordering},
  },
  time::{Duration, Instant},
};

use anyhow::{Context, anyhow};
use command::{CommandOptions, run_komodo_standard_command};
use komodo_backup::{
  VykarPatternMatcher, VykarRepository, backup_manifest_source_name,
};
use komodo_client::entities::docker::{
  container::{ContainerListItem, ContainerStateStatusEnum},
  volume::{VolumeScopeEnum, is_anonymous_volume},
};
use mogh_resolver::Resolve;
use periphery_client::api::backup::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use shell_escape::unix::escape;

use crate::{config::periphery_config, state::docker_client};

use super::Args;

const COMPOSE_PROJECT_LABEL: &str = "com.docker.compose.project";
const RESTORE_PLAN_VOLUME_LABEL: &str = "komodo.restore-plan";
const PENDING_CANCELLATION_TTL: Duration = Duration::from_secs(60);
const MAX_PENDING_CANCELLATIONS: usize = 1_024;
const RESTORE_PREFLIGHT_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_RESTORE_PREVIEW_ROWS: usize = 10_000;
const MAX_RESTORE_PREVIEW_BYTES: usize = 1024 * 1024;
const MAX_RESTORE_PREVIEW_DEPTH: usize =
  komodo_backup::MAX_RESTORE_PATH_DEPTH;

fn preflight_slots() -> &'static Arc<tokio::sync::Semaphore> {
  static SLOTS: OnceLock<Arc<tokio::sync::Semaphore>> =
    OnceLock::new();
  SLOTS.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(1)))
}

struct RestorePreviewBudget {
  inventory: komodo_backup::RestoreInventoryBudget,
  rows: usize,
  path_bytes: usize,
}

impl RestorePreviewBudget {
  fn new(deadline: Instant) -> Self {
    Self {
      inventory: komodo_backup::RestoreInventoryBudget::new(deadline),
      rows: 0,
      path_bytes: 0,
    }
  }

  fn push(
    &mut self,
    output: &mut Vec<String>,
    path: &Path,
  ) -> anyhow::Result<()> {
    let path = path.to_string_lossy();
    if self.rows >= MAX_RESTORE_PREVIEW_ROWS
      || path.len()
        > MAX_RESTORE_PREVIEW_BYTES.saturating_sub(self.path_bytes)
    {
      return Err(anyhow!(
        "Restore preview exceeds 10,000 changed paths or 1 MiB of path text; select a smaller subtree before confirming"
      ));
    }
    self.rows += 1;
    self.path_bytes += path.len();
    output.push(path.into_owned());
    Ok(())
  }
}

#[derive(Deserialize)]
struct BackupComposeConfig {
  #[serde(default)]
  services: HashMap<String, BackupComposeService>,
}

#[derive(Default, Deserialize)]
struct BackupComposeService {
  #[serde(default)]
  volumes: Vec<BackupComposeMount>,
}

#[derive(Clone, Deserialize)]
#[serde(untagged)]
enum BackupComposeMount {
  Short(String),
  Long {
    #[serde(rename = "type")]
    mount_type: Option<String>,
    source: Option<String>,
    target: Option<String>,
  },
}

#[derive(Default)]
struct OperationCancellationRegistry {
  active: HashMap<String, Arc<AtomicBool>>,
  pending: HashMap<String, Instant>,
}

impl OperationCancellationRegistry {
  fn prune_pending(&mut self, now: Instant) {
    self.pending.retain(|_, expires_at| *expires_at > now);
  }
}

fn cancellation_registry()
-> &'static Mutex<OperationCancellationRegistry> {
  static REGISTRY: OnceLock<Mutex<OperationCancellationRegistry>> =
    OnceLock::new();
  REGISTRY.get_or_init(Default::default)
}

fn operation_cancellation_token(
  operation_id: &str,
) -> Arc<AtomicBool> {
  cancellation_registry()
    .lock()
    .unwrap()
    .active
    .get(operation_id)
    .expect("backup operation cancellation was not registered")
    .clone()
}

struct OperationCancellationRegistration(String);

impl Drop for OperationCancellationRegistration {
  fn drop(&mut self) {
    cancellation_registry()
      .lock()
      .unwrap()
      .active
      .remove(&self.0);
  }
}

fn register_operation_cancellation(
  operation_id: &str,
) -> (Arc<AtomicBool>, OperationCancellationRegistration) {
  let mut registry = cancellation_registry().lock().unwrap();
  let now = Instant::now();
  registry.prune_pending(now);
  let cancelled = registry.pending.remove(operation_id).is_some();
  let token = Arc::new(AtomicBool::new(cancelled));
  registry
    .active
    .insert(operation_id.to_string(), token.clone());
  (
    token,
    OperationCancellationRegistration(operation_id.to_string()),
  )
}

fn operation_cancelled(operation_id: &str) -> bool {
  cancellation_registry()
    .lock()
    .unwrap()
    .active
    .get(operation_id)
    .is_some_and(|token| token.load(Ordering::SeqCst))
}

fn request_operation_cancellation(operation_id: &str) -> bool {
  let mut registry = cancellation_registry().lock().unwrap();
  if let Some(token) = registry.active.get(operation_id) {
    token.store(true, Ordering::SeqCst);
    return true;
  }

  // Cancellation and backup requests use separate HTTP connections, so the
  // cancellation can arrive first. Retain only a short-lived, size-bounded
  // marker; registration consumes it atomically under this same lock.
  let now = Instant::now();
  registry.prune_pending(now);
  if registry.pending.len() >= MAX_PENDING_CANCELLATIONS
    && let Some(oldest) = registry
      .pending
      .iter()
      .min_by_key(|(_, expires_at)| **expires_at)
      .map(|(operation_id, _)| operation_id.clone())
  {
    registry.pending.remove(&oldest);
  }
  registry
    .pending
    .insert(operation_id.to_string(), now + PENDING_CANCELLATION_TTL);
  true
}

fn backup_operation_lock() -> &'static tokio::sync::Mutex<()> {
  static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
  LOCK.get_or_init(Default::default)
}

#[derive(Debug)]
struct ExcludedBackupSource(String);

impl std::fmt::Display for ExcludedBackupSource {
  fn fmt(
    &self,
    formatter: &mut std::fmt::Formatter<'_>,
  ) -> std::fmt::Result {
    formatter.write_str(&self.0)
  }
}

impl std::error::Error for ExcludedBackupSource {}

impl Resolve<Args> for DiscoverBackupSource {
  async fn resolve(
    self,
    _: &Args,
  ) -> anyhow::Result<DiscoverBackupSourceResponse> {
    discover_source(
      &self.target,
      &self.protected_repository_paths,
      &self.filters,
    )
    .await
  }
}

impl Resolve<Args> for RunVykarBackup {
  async fn resolve(
    self,
    _: &Args,
  ) -> anyhow::Result<RunVykarBackupResponse> {
    let _operation = backup_operation_lock().lock().await;
    let _filesystem = protected_filesystem_guard()?;
    ensure_no_pending_recovery()?;
    let (_cancellation, _cancellation_registration) =
      register_operation_cancellation(&self.run_id);
    let discovered = discover_source(
      &self.target,
      &self.protected_repository_paths,
      &self.filters,
    )
    .await?;
    let container_journal = if self.stop_containers {
      persist_container_quiesce_journal(
        &self.run_id,
        &discovered.running_containers,
      )?
    } else {
      None
    };
    let mut stopped: Vec<String> = Vec::new();
    if self.stop_containers {
      for container in &discovered.running_containers {
        if let Err(error) =
          run_container_command("stop", container).await
        {
          let (restarted, restart_errors) =
            restart_quiesced_containers(
              container_journal.as_deref(),
              &stopped,
            )
            .await?;
          if !restart_errors.is_empty() {
            return Ok(RunVykarBackupResponse {
              primary: VykarBackupRepositoryResult {
                error: Some(format!(
                  "Failed to quiesce every affected container: {error:#}"
                )),
                ..Default::default()
              },
              stopped_containers: stopped,
              restarted_containers: restarted,
              restart_errors,
              ..Default::default()
            });
          }
          return Err(error.context(
            "Failed to quiesce every affected container; already stopped containers were restarted",
          ));
        }
        stopped.push(container.clone());
      }
    }

    let result = if operation_cancelled(&self.run_id) {
      Err(anyhow!("Backup cancelled before repository write"))
    } else {
      run_backup_repositories(&self, &discovered.paths).await
    };

    let (restarted, restart_errors) = restart_quiesced_containers(
      container_journal.as_deref(),
      &stopped,
    )
    .await?;

    let (primary, mirror) = match result {
      Ok(result) => result,
      Err(error) if !restart_errors.is_empty() => {
        return Ok(RunVykarBackupResponse {
          primary: VykarBackupRepositoryResult {
            error: Some(format!("{error:#}")),
            ..Default::default()
          },
          stopped_containers: stopped,
          restarted_containers: restarted,
          restart_errors,
          ..Default::default()
        });
      }
      Err(error) => return Err(error),
    };
    Ok(RunVykarBackupResponse {
      excluded: None,
      primary,
      mirror,
      stopped_containers: stopped,
      restarted_containers: restarted,
      restart_errors,
    })
  }
}

impl Resolve<Args> for RunVykarBackupBatch {
  async fn resolve(
    self,
    _: &Args,
  ) -> anyhow::Result<RunVykarBackupBatchResponse> {
    let _operation = backup_operation_lock().lock().await;
    let _filesystem = protected_filesystem_guard()?;
    ensure_no_pending_recovery()?;
    let (_cancellation, _cancellation_registration) =
      register_operation_cancellation(&self.run_id);
    let mut discovered = Vec::new();
    let mut results = Vec::new();
    let mut discovery_errors = Vec::new();
    let mut running = BTreeSet::new();
    for task in self.tasks {
      match discover_source(
        &task.target,
        &self.protected_repository_paths,
        &self.filters,
      )
      .await
      {
        Ok(source) => {
          running.extend(source.running_containers.iter().cloned());
          discovered.push((task, source.paths));
        }
        Err(error) if error.is::<ExcludedBackupSource>() => {
          results.push(VykarBackupTaskResult {
            source_label: task.source_label,
            result: RunVykarBackupResponse {
              excluded: Some(error.to_string()),
              ..Default::default()
            },
          });
        }
        Err(error) => discovery_errors
          .push(format!("{}: {error:#}", task.source_label)),
      }
    }
    let running = running.into_iter().collect::<Vec<_>>();
    let container_journal = if self.stop_containers {
      persist_container_quiesce_journal(&self.run_id, &running)?
    } else {
      None
    };
    let mut stopped: Vec<String> = Vec::new();
    if self.stop_containers {
      for container in running {
        if let Err(error) =
          run_container_command("stop", &container).await
        {
          let (_, restart_errors) = restart_quiesced_containers(
            container_journal.as_deref(),
            &stopped,
          )
          .await?;
          if !restart_errors.is_empty() {
            return Ok(RunVykarBackupBatchResponse {
              discovery_errors: vec![format!(
                "Failed to quiesce every affected container on the node: {error:#}"
              )],
              restart_errors,
              ..Default::default()
            });
          }
          return Err(error.context(
            "Failed to quiesce every affected container on the node",
          ));
        }
        stopped.push(container);
      }
    }

    for (task, paths) in discovered {
      if operation_cancelled(&self.run_id) {
        break;
      }
      let request = RunVykarBackup {
        target: task.target,
        primary: self.primary.clone(),
        mirror: self.mirror.clone(),
        advanced: self.advanced.clone(),
        hostname: self.hostname.clone(),
        source_label: task.source_label.clone(),
        snapshot_name: task.snapshot_name,
        run_id: self.run_id.clone(),
        komodo_version: self.komodo_version.clone(),
        protected_repository_paths: self
          .protected_repository_paths
          .clone(),
        filters: self.filters.clone(),
        stop_containers: false,
        mirror_only: task.mirror_only,
        primary_only: task.primary_only,
      };
      match run_backup_repositories(&request, &paths).await {
        Ok((primary, mirror)) => {
          results.push(VykarBackupTaskResult {
            source_label: task.source_label,
            result: RunVykarBackupResponse {
              primary,
              mirror,
              ..Default::default()
            },
          })
        }
        Err(error) => discovery_errors
          .push(format!("{}: {error:#}", task.source_label)),
      }
    }

    let (_, restart_errors) = restart_quiesced_containers(
      container_journal.as_deref(),
      &stopped,
    )
    .await?;
    Ok(RunVykarBackupBatchResponse {
      results,
      discovery_errors,
      restart_errors,
    })
  }
}

async fn run_backup_repositories(
  request: &RunVykarBackup,
  source_paths: &[String],
) -> anyhow::Result<(
  VykarBackupRepositoryResult,
  Option<VykarBackupRepositoryResult>,
)> {
  if request.primary_only && request.mirror_only {
    return Err(anyhow!(
      "A backup retry cannot be both primary-only and mirror-only"
    ));
  }
  if (request.primary_only || request.mirror_only)
    && request.mirror.is_none()
  {
    return Err(anyhow!(
      "Repository-specific retry requested without a configured mirror"
    ));
  }
  let manifest_staging = backup_manifest_staging_dir();
  std::fs::create_dir_all(&manifest_staging).with_context(|| {
    format!(
      "Failed to create backup manifest staging root {}",
      manifest_staging.display()
    )
  })?;
  let manifest_dir = manifest_staging
    .join(backup_manifest_source_name(&request.snapshot_name));
  // Operations are serialized. Removing a same-snapshot directory here
  // recovers staging left by a process exit before the drop guard ran.
  remove_path(&manifest_dir)?;
  std::fs::create_dir(&manifest_dir).with_context(|| {
    format!(
      "Failed to create backup manifest staging directory {}",
      manifest_dir.display()
    )
  })?;
  let _manifest_cleanup =
    RemovePathsOnDrop(vec![manifest_dir.clone()]);
  write_manifest(request, source_paths, &manifest_dir)?;
  let mut paths = source_paths.to_vec();
  paths.push(manifest_dir.to_string_lossy().into_owned());

  let primary = if request.mirror_only {
    VykarBackupRepositoryResult {
      complete: true,
      ..Default::default()
    }
  } else {
    run_repository_backup(
      request.primary.clone(),
      request,
      paths.clone(),
    )
    .await
  };
  if operation_cancelled(&request.run_id) {
    return Err(anyhow!("Backup cancelled before mirror write"));
  }
  let mirror = if request.primary_only {
    request
      .mirror
      .as_ref()
      .map(|_| VykarBackupRepositoryResult {
        complete: true,
        ..Default::default()
      })
  } else if let Some(repository) = request.mirror.clone() {
    Some(run_repository_backup(repository, request, paths).await)
  } else {
    None
  };
  Ok((primary, mirror))
}

fn backup_manifest_staging_dir() -> PathBuf {
  periphery_config()
    .stack_dir()
    .join(".komodo-vykar")
    .join("backup-manifests")
}

async fn run_repository_backup(
  repository: komodo_client::entities::backup::BackupRepository,
  request: &RunVykarBackup,
  source_paths: Vec<String>,
) -> VykarBackupRepositoryResult {
  let advanced = request.advanced.clone();
  let hostname = request.hostname.clone();
  let snapshot_name = request.snapshot_name.clone();
  let source_label = request.source_label.clone();
  let cancellation = operation_cancellation_token(&request.run_id);
  let one_file_system =
    !request.filters.include_cross_filesystem_mounts;
  let result = tokio::task::spawn_blocking(move || {
    let cache = vykar_cache_dir(&hostname)?;
    let repository = VykarRepository::new(
      &repository,
      &hostname,
      &cache,
      &cache,
      &advanced,
    )?;
    repository.backup_cancellable_with_options(
      &snapshot_name,
      &source_label,
      &source_paths,
      Some(cancellation.as_ref()),
      one_file_system,
    )
  })
  .await;
  match result {
    Ok(Ok(result)) => VykarBackupRepositoryResult {
      complete: !result.partial,
      partial: result.partial,
      files: result.files,
      original_size: result.original_size,
      stored_size: result.stored_size,
      error: None,
    },
    Ok(Err(error)) => VykarBackupRepositoryResult {
      error: Some(format!("{error:#}")),
      ..Default::default()
    },
    Err(error) => VykarBackupRepositoryResult {
      error: Some(format!("Vykar worker failed: {error}")),
      ..Default::default()
    },
  }
}

#[derive(Serialize)]
struct KomodoBackupManifest<'a> {
  schema: &'static str,
  version: u32,
  run_id: &'a str,
  source_label: &'a str,
  hostname: &'a str,
  komodo_version: &'a str,
  paths: &'a [String],
  path_aliases: &'a BTreeMap<String, String>,
  target: &'a PeripheryBackupTarget,
  configuration_sha256: String,
  paths_sha256: String,
  path_aliases_sha256: String,
}

fn write_manifest(
  request: &RunVykarBackup,
  paths: &[String],
  directory: &Path,
) -> anyhow::Result<()> {
  let target = serde_json::to_vec(&request.target)
    .context("Failed to serialize backup source identity")?;
  let path_aliases = backup_manifest_path_aliases(request, paths)?;
  let manifest = KomodoBackupManifest {
    schema: "komodo.backup-manifest/v1",
    version: 1,
    run_id: &request.run_id,
    source_label: &request.source_label,
    hostname: &request.hostname,
    komodo_version: &request.komodo_version,
    paths,
    path_aliases: &path_aliases,
    target: &request.target,
    configuration_sha256: hex::encode(Sha256::digest(target)),
    paths_sha256: hex::encode(Sha256::digest(
      serde_json::to_vec(paths)
        .context("Failed to serialize backup source paths")?,
    )),
    path_aliases_sha256: hex::encode(Sha256::digest(
      serde_json::to_vec(&path_aliases)
        .context("Failed to serialize backup source path aliases")?,
    )),
  };
  let bytes = serde_json::to_vec_pretty(&manifest)
    .context("Failed to serialize backup manifest")?;
  let path = directory.join("komodo-backup-manifest.json");
  let mut file = OpenOptions::new()
    .create_new(true)
    .write(true)
    .open(&path)
    .with_context(|| {
      format!("Failed to create {}", path.display())
    })?;
  file.write_all(&bytes)?;
  file.sync_all()?;
  Ok(())
}

fn backup_manifest_path_aliases(
  request: &RunVykarBackup,
  paths: &[String],
) -> anyhow::Result<BTreeMap<String, String>> {
  let PeripheryBackupTarget::Stack { stack, .. } = &request.target
  else {
    return Ok(BTreeMap::new());
  };
  let run_directory = paths
    .first()
    .context("Stack backup has no run-directory source")?;
  compose_bind_path_aliases(stack, Path::new(run_directory))
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
  left == right || left.starts_with(right) || right.starts_with(left)
}

fn resolve_existing_ancestor(path: &Path) -> anyhow::Result<PathBuf> {
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
            "Restore destination has no resolvable ancestor: {}",
            path.display()
          )
        })?;
        missing.push(name.to_os_string());
        ancestor = ancestor.parent().with_context(|| {
          format!(
            "Restore destination has no resolvable ancestor: {}",
            path.display()
          )
        })?;
      }
      Err(error) => {
        return Err(error).with_context(|| {
          format!(
            "Failed to resolve restore destination ancestor: {}",
            path.display()
          )
        });
      }
    }
  }
}

fn validate_path_outside_internal_storage(
  path: &Path,
  internal_storage: &Path,
  label: &str,
) -> anyhow::Result<()> {
  let resolved_path = resolve_existing_ancestor(path)?;
  let resolved_internal =
    resolve_existing_ancestor(internal_storage)?;
  if paths_overlap(path, internal_storage)
    || paths_overlap(&resolved_path, &resolved_internal)
  {
    return Err(ExcludedBackupSource(format!(
      "{label} '{}' overlaps Periphery's internal backup storage '{}'",
      path.display(),
      internal_storage.display()
    )).into());
  }
  Ok(())
}

fn validate_resolved_restore_destinations(
  publish: &[RestorePublishPath],
) -> anyhow::Result<()> {
  validate_resolved_restore_destinations_against(
    publish,
    &periphery_config().stack_dir().join(".komodo-vykar"),
  )
}

fn validate_resolved_restore_destinations_against(
  publish: &[RestorePublishPath],
  internal_storage: &Path,
) -> anyhow::Result<()> {
  let destinations = publish
    .iter()
    .map(|item| {
      validate_restore_destination_ancestors(item)?;
      let destination = Path::new(&item.destination);
      validate_path_outside_internal_storage(
        destination,
        internal_storage,
        "Restore destination",
      )?;
      resolve_existing_ancestor(destination)
        .map(|resolved| (item.destination.as_str(), resolved))
    })
    .collect::<anyhow::Result<Vec<_>>>()?;
  for (index, (left_label, left)) in destinations.iter().enumerate() {
    for (right_label, right) in destinations.iter().skip(index + 1) {
      if paths_overlap(left, right) {
        return Err(anyhow!(
          "Restore destinations overlap after resolving filesystem aliases: '{left_label}' and '{right_label}'"
        ));
      }
    }
  }
  Ok(())
}

async fn validate_restore_destinations(
  publish: &[RestorePublishPath],
  protected_repository_paths: &[ProtectedRepositoryPath],
) -> anyhow::Result<()> {
  validate_resolved_restore_destinations(publish)?;
  let docker_guard = docker_client().load();
  let docker = docker_guard
    .as_ref()
    .as_ref()
    .context("Docker is unavailable")?;
  let containers = docker.list_containers().await?;
  let protected_repository_sources =
    resolve_protected_repository_sources(
      docker,
      &containers,
      protected_repository_paths,
    )
    .await?;
  for item in publish {
    validate_path_outside_protected_repositories(
      Path::new(&item.destination),
      &protected_repository_sources,
      "Restore destination",
    )?;
  }
  Ok(())
}

fn validate_restore_destination_ancestors(
  item: &RestorePublishPath,
) -> anyhow::Result<()> {
  let destination = Path::new(&item.destination);
  if !destination.is_absolute()
    || destination
      .components()
      .any(|part| matches!(part, std::path::Component::ParentDir))
  {
    return Err(anyhow!(
      "Restore destination is not an absolute normalized path"
    ));
  }
  if let Some(root) = item.destination_root.as_deref().map(Path::new)
    && (!root.is_absolute()
      || root
        .components()
        .any(|part| matches!(part, std::path::Component::ParentDir))
      || !destination.starts_with(root))
  {
    return Err(anyhow!(
      "Selected restore destination is outside its confirmed absolute root"
    ));
  }
  // A full mapped destination has no selection boundary. Both forms still
  // need every ancestor checked from the filesystem root, not just the leaf.
  let root = Path::new("/");
  let relative = destination.strip_prefix(root)?;
  let mut ancestor = root.to_path_buf();
  for part in relative.components() {
    match std::fs::symlink_metadata(&ancestor) {
      Ok(metadata)
        if metadata.file_type().is_symlink()
          || !metadata.is_dir() =>
      {
        return Err(anyhow!(
          "Restore cannot traverse symlink or non-directory ancestor '{}'",
          ancestor.display()
        ));
      }
      Ok(_) => {}
      Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
        return Ok(());
      }
      Err(error) => return Err(error.into()),
    }
    ancestor.push(part.as_os_str());
  }
  // The leaf itself may be a symlink: publication replaces that entry.
  Ok(())
}

fn insert_bind_backup_root(
  bind_paths: &mut BTreeSet<PathBuf>,
  run_directory: &Path,
  path: &Path,
) -> anyhow::Result<()> {
  let bind = validate_source_path(path)?;
  if bind == run_directory || bind.starts_with(run_directory) {
    // Vykar traverses mounts, so the Stack root already captures a bind below
    // it.
    return Ok(());
  }
  if run_directory.starts_with(&bind) {
    return Err(anyhow!(
      "Bind source '{}' contains the Stack run directory '{}'; overlapping backup roots cannot be restored atomically",
      bind.display(),
      run_directory.display()
    ));
  }
  if bind_paths.iter().any(|existing| bind.starts_with(existing)) {
    // An ancestor already captures this tree. Keeping both roots would make
    // the resulting full snapshot impossible to publish atomically.
    return Ok(());
  }
  bind_paths.retain(|existing| !existing.starts_with(&bind));
  bind_paths.insert(bind);
  Ok(())
}

fn compose_bind_paths(
  stack: &komodo_client::entities::stack::Stack,
  run_directory: &Path,
) -> anyhow::Result<BTreeSet<PathBuf>> {
  let Some(config) = stack.info.deployed_config.as_deref() else {
    return Ok(BTreeSet::new());
  };
  let config: BackupComposeConfig =
    serde_yaml_ng::from_str(config)
      .context("Failed to parse deployed Compose configuration")?;
  let mut paths = BTreeSet::new();
  for mount in config
    .services
    .into_values()
    .flat_map(|service| service.volumes)
  {
    let source = compose_bind_source(mount);
    let Some(source) = source else {
      continue;
    };
    let source = Path::new(&source);
    let source = if source.is_absolute() {
      source.to_path_buf()
    } else {
      run_directory.join(source)
    };
    insert_bind_backup_root(&mut paths, run_directory, &source)?;
  }
  Ok(paths)
}

fn compose_bind_source(mount: BackupComposeMount) -> Option<String> {
  match mount {
    BackupComposeMount::Long {
      mount_type, source, ..
    } => source.filter(|source| {
      mount_type.as_deref() == Some("bind")
        || mount_type.is_none() && Path::new(source).is_absolute()
    }),
    BackupComposeMount::Short(value) => {
      split_compose_short_mount(&value).and_then(|(source, _)| {
        (Path::new(source).is_absolute() || source.starts_with('.'))
          .then(|| source.to_string())
      })
    }
  }
}

fn compose_bind_path_aliases(
  stack: &komodo_client::entities::stack::Stack,
  _run_directory: &Path,
) -> anyhow::Result<BTreeMap<String, String>> {
  let Some(config) = stack.info.deployed_config.as_deref() else {
    return Ok(BTreeMap::new());
  };
  let config: BackupComposeConfig =
    serde_yaml_ng::from_str(config)
      .context("Failed to parse deployed Compose configuration")?;
  let mut aliases = BTreeMap::new();
  for mount in config
    .services
    .into_values()
    .flat_map(|service| service.volumes)
  {
    let Some(source) = compose_bind_source(mount) else {
      continue;
    };
    let source_path = Path::new(&source);
    if !source_path.is_absolute() {
      // Relative bind paths move with the recovered run directory and do not
      // need an absolute source rewrite.
      continue;
    }
    let canonical = validate_source_path(source_path)?;
    if canonical != source_path {
      aliases
        .insert(source, canonical.to_string_lossy().into_owned());
    }
  }
  Ok(aliases)
}

fn remap_absolute_bind_source(
  source: &str,
  mappings: &HashMap<String, String>,
  path_aliases: &HashMap<String, String>,
) -> Option<String> {
  let source = Path::new(
    path_aliases
      .get(source)
      .map(String::as_str)
      .unwrap_or(source),
  );
  if !source.is_absolute() {
    return None;
  }
  mappings
    .iter()
    .filter_map(|(from, to)| {
      let from = Path::new(from);
      source.strip_prefix(from).ok().map(|relative| {
        (
          from.components().count(),
          Path::new(to).join(relative).to_string_lossy().into_owned(),
        )
      })
    })
    .max_by_key(|(depth, _)| *depth)
    .map(|(_, mapped)| mapped)
}

fn split_compose_short_mount(value: &str) -> Option<(&str, &str)> {
  let mut braces = 0_u32;
  for (index, character) in value.char_indices() {
    match character {
      '{' => braces += 1,
      '}' => braces = braces.saturating_sub(1),
      ':' if braces == 0 => {
        return Some((&value[..index], &value[index + 1..]));
      }
      _ => {}
    }
  }
  None
}

fn compose_mount_target(mount: &BackupComposeMount) -> Option<&str> {
  match mount {
    BackupComposeMount::Long { target, .. } => target.as_deref(),
    BackupComposeMount::Short(value) => {
      split_compose_short_mount(value)
        .map(|(_, suffix)| suffix.split(':').next().unwrap_or(suffix))
    }
  }
}

/// Associate expressions with the authenticated, interpolated deployment by
/// service and mount target. Do not expand using the recovery host's env.
fn resolve_recovered_bind_expressions(
  document: &mut serde_yaml_ng::Value,
  deployed_config: Option<&str>,
  mappings: &HashMap<String, String>,
  aliases: &HashMap<String, String>,
) -> anyhow::Result<()> {
  use serde_yaml_ng::Value;
  let deployed = deployed_config
    .map(serde_yaml_ng::from_str::<BackupComposeConfig>)
    .transpose()
    .context(
      "Failed to parse snapshot's deployed Compose configuration",
    )?;
  let key = |value: &str| Value::String(value.into());
  let Some(services) =
    document.get_mut("services").and_then(Value::as_mapping_mut)
  else {
    return Ok(());
  };
  for (service_name, service) in services {
    let Some(volumes) =
      service.get_mut("volumes").and_then(Value::as_sequence_mut)
    else {
      continue;
    };
    for volume in volumes {
      let parsed: BackupComposeMount =
        serde_yaml_ng::from_value(volume.clone())?;
      let source = match &parsed {
        BackupComposeMount::Short(value) => {
          split_compose_short_mount(value).map(|(source, _)| source)
        }
        BackupComposeMount::Long {
          mount_type, source, ..
        } => {
          if mount_type.as_deref().is_some_and(|kind| kind != "bind")
          {
            continue;
          }
          source.as_deref()
        }
      };
      if !source.is_some_and(|source| source.contains('$')) {
        continue;
      }
      let target = compose_mount_target(&parsed)
        .context("Cannot identify an environment-expanded Compose mount target")?;
      let deployed_mount = deployed.as_ref()
        .and_then(|config| config.services.get(service_name.as_str()?))
        .and_then(|service| service.volumes.iter().find(|mount| compose_mount_target(mount) == Some(target)))
        .context("Cannot resolve an environment-expanded Compose mount from snapshot deployment metadata")?;
      let Some(expanded) =
        compose_bind_source(deployed_mount.clone())
      else {
        // The deployed expression names a Docker volume, not a bind source.
        continue;
      };
      if remap_absolute_bind_source(&expanded, mappings, aliases)
        .is_none()
      {
        // Intentionally excluded roots have no confirmed mapping.
        continue;
      }
      match volume {
        Value::String(short) => {
          let (_, suffix) = split_compose_short_mount(short).unwrap();
          *short = format!("{expanded}:{suffix}");
        }
        Value::Mapping(long) => {
          long.insert(key("source"), Value::String(expanded));
        }
        _ => unreachable!(),
      }
    }
  }
  Ok(())
}

fn rewrite_compose_bind_mappings(
  document: &mut serde_yaml_ng::Value,
  mappings: &HashMap<String, String>,
  path_aliases: &HashMap<String, String>,
) -> usize {
  use serde_yaml_ng::Value;

  let key = |value: &str| Value::String(value.into());
  let Some(services) = document
    .as_mapping_mut()
    .and_then(|root| root.get_mut(key("services")))
    .and_then(Value::as_mapping_mut)
  else {
    return 0;
  };
  let mut rewritten = 0;
  for service in services.values_mut() {
    let Some(volumes) = service
      .as_mapping_mut()
      .and_then(|service| service.get_mut(key("volumes")))
      .and_then(Value::as_sequence_mut)
    else {
      continue;
    };
    for volume in volumes {
      match volume {
        Value::String(short) => {
          let Some((source, suffix)) =
            split_compose_short_mount(short)
          else {
            continue;
          };
          if let Some(mapped) =
            remap_absolute_bind_source(source, mappings, path_aliases)
          {
            *short = format!("{mapped}:{suffix}");
            rewritten += 1;
          }
        }
        Value::Mapping(long) => {
          let mount_type = long
            .get(key("type"))
            .and_then(Value::as_str)
            .map(str::to_owned);
          let Some(source) = long
            .get_mut(key("source"))
            .and_then(|value| value.as_str())
            .map(str::to_owned)
          else {
            continue;
          };
          if mount_type.as_deref().is_some_and(|kind| kind != "bind")
          {
            continue;
          }
          if let Some(mapped) = remap_absolute_bind_source(
            &source,
            mappings,
            path_aliases,
          ) {
            long.insert(key("source"), Value::String(mapped));
            rewritten += 1;
          }
        }
        _ => {}
      }
    }
  }
  rewritten
}

/// Open only regular files reached through real directories in staging.
/// Use the same descriptor for reading and rewriting, never a truncating
/// pathname open that could follow a restored link into the host.
fn open_staged_compose_file(
  staging: &Path,
  path: &Path,
) -> anyhow::Result<std::fs::File> {
  let relative = path.strip_prefix(staging)?;
  let mut current = staging.to_path_buf();
  if !std::fs::symlink_metadata(&current)?.is_dir() {
    return Err(anyhow!(
      "Compose staging root must be a real directory"
    ));
  }
  let mut components = relative.components().peekable();
  while let Some(component) = components.next() {
    if !matches!(component, std::path::Component::Normal(_)) {
      return Err(anyhow!("Unsafe staged Compose path"));
    }
    current.push(component);
    let metadata = std::fs::symlink_metadata(&current)?;
    let valid = if components.peek().is_some() {
      metadata.is_dir()
    } else {
      metadata.is_file()
    };
    if !valid {
      return Err(anyhow!(
        "Recovered Compose paths must not contain symlinks or special files: {}",
        current.display()
      ));
    }
  }
  let file = OpenOptions::new()
    .read(true)
    .write(true)
    .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
    .open(path)?;
  if !file.metadata()?.is_file() {
    return Err(anyhow!(
      "Recovered Compose file must be a regular file"
    ));
  }
  Ok(file)
}

fn rewrite_recovered_stack_compose_files(
  request: &TransactionalVykarRestore,
  staging: &Path,
) -> anyhow::Result<()> {
  let PeripheryBackupTarget::Stack { stack, .. } = &request.target
  else {
    return Ok(());
  };
  if request.bind_path_mappings.is_empty() {
    return Ok(());
  }
  let run_directory = Path::new(&stack.config.run_directory);
  let run_root = request
    .publish
    .iter()
    .find(|item| Path::new(&item.destination) == run_directory)
    .context(
      "Recovered Stack publish plan has no run-directory root",
    )?;
  let staged_run_directory = staging.join(&run_root.snapshot_path);
  for compose_file in stack.compose_file_paths() {
    let relative = Path::new(compose_file);
    if relative.is_absolute()
      || relative.components().any(|component| {
        matches!(component, std::path::Component::ParentDir)
      })
    {
      return Err(anyhow!(
        "Recovered Stack Compose path is unsafe: {compose_file}"
      ));
    }
    let path = staged_run_directory.join(relative);
    let mut file = open_staged_compose_file(staging, &path)?;
    let mut text = String::new();
    file.read_to_string(&mut text).with_context(|| {
      format!(
        "Failed to read recovered Compose file {}",
        path.display()
      )
    })?;
    let mut document: serde_yaml_ng::Value =
      serde_yaml_ng::from_str(&text).with_context(|| {
        format!(
          "Failed to parse recovered Compose file {}",
          path.display()
        )
      })?;
    resolve_recovered_bind_expressions(
      &mut document,
      stack.info.deployed_config.as_deref(),
      &request.bind_path_mappings,
      &request.bind_path_aliases,
    )?;
    if rewrite_compose_bind_mappings(
      &mut document,
      &request.bind_path_mappings,
      &request.bind_path_aliases,
    ) == 0
    {
      continue;
    }
    let rewritten = serde_yaml_ng::to_string(&document)?;
    use std::io::Seek;
    file.rewind()?;
    file.set_len(0)?;
    file.write_all(rewritten.as_bytes())?;
    file.sync_all()?;
  }
  Ok(())
}

async fn affected_running_containers(
  docker: &crate::docker::DockerClient,
  containers: &[ContainerListItem],
  target: &PeripheryBackupTarget,
  paths: &BTreeSet<PathBuf>,
  protected_paths: &[ProtectedRepositoryPath],
) -> anyhow::Result<Vec<String>> {
  ensure_target_not_control_plane(
    containers,
    target,
    protected_paths,
  )?;
  // The worker mounts Docker's volume root to perform this operation. Its own
  // filesystem gate already excludes other mutations; stopping it would kill
  // the backup/restore before it can restart application containers.
  let own_id = komodo_backup::container::current_container_id();
  let mut affected = running_containers_for_target(
    containers,
    target,
    own_id.as_deref(),
    protected_paths,
  )
  .into_iter()
  .collect::<BTreeSet<_>>();
  for container in containers.iter().filter(|container| {
    container_is_quiesce_candidate(container, own_id.as_deref())
      && !is_core_container(container, protected_paths)
  }) {
    if affected.contains(&container.name) {
      continue;
    }
    let inspected = docker.inspect_container(&container.name).await?;
    if inspected.mounts.into_iter().any(|mount| {
      mount_affects_paths(
        mount.typ.as_deref(),
        mount.source.as_deref(),
        paths,
      )
    }) {
      affected.insert(container.name.clone());
    }
  }
  Ok(affected.into_iter().collect())
}

fn mount_type_affects_paths(mount_type: Option<&str>) -> bool {
  matches!(mount_type, Some("bind" | "volume"))
}

fn mount_affects_paths(
  mount_type: Option<&str>,
  source: Option<&str>,
  paths: &BTreeSet<PathBuf>,
) -> bool {
  if !mount_type_affects_paths(mount_type) {
    return false;
  }
  let Some(source) = source else {
    return false;
  };
  let source = PathBuf::from(source);
  let source = source.canonicalize().unwrap_or(source);
  paths.iter().any(|path| paths_overlap(&source, path))
}

async fn discover_source(
  target: &PeripheryBackupTarget,
  protected_repository_paths: &[ProtectedRepositoryPath],
  filters: &BackupSourceFilters,
) -> anyhow::Result<DiscoverBackupSourceResponse> {
  let docker_guard = docker_client().load();
  let docker = docker_guard
    .as_ref()
    .as_ref()
    .context("Docker is unavailable")?;
  let containers = docker.list_containers().await?;
  let protected_repository_sources =
    resolve_protected_repository_sources(
      docker,
      &containers,
      protected_repository_paths,
    )
    .await?;
  match target {
    PeripheryBackupTarget::Stack { stack, repo } => {
      if !stack.config.swarm_id.is_empty() {
        return Err(anyhow!(
          "Swarm stacks are not supported by backup v1"
        ));
      }
      let run_directory = validate_source_path(
        &crate::stack::write::resolved_run_directory(
          stack,
          repo.as_deref(),
        ),
      )?;
      let mut bind_paths = compose_bind_paths(stack, &run_directory)?;
      let project_name = stack.project_name(false);
      for container in containers.iter().filter(|container| {
        container.labels.get(COMPOSE_PROJECT_LABEL)
          == Some(&project_name)
      }) {
        let inspected =
          docker.inspect_container(&container.name).await?;
        for mount in inspected
          .mounts
          .into_iter()
          .filter(|mount| mount.typ.as_deref() == Some("bind"))
        {
          let source = mount
            .source
            .context("Bind mount did not report a source path")?;
          insert_bind_backup_root(
            &mut bind_paths,
            &run_directory,
            Path::new(&source),
          )?;
        }
      }
      let bind_paths = select_bind_backup_roots(
        bind_paths,
        &run_directory,
        filters,
      )?;
      let internal_storage =
        periphery_config().stack_dir().join(".komodo-vykar");
      validate_path_outside_internal_storage(
        &run_directory,
        &internal_storage,
        "Backup source",
      )?;
      validate_path_outside_protected_repositories(
        &run_directory,
        &protected_repository_sources,
        "Backup source",
      )?;
      for bind_path in &bind_paths {
        validate_path_outside_internal_storage(
          bind_path,
          &internal_storage,
          "Backup source",
        )?;
        validate_path_outside_protected_repositories(
          bind_path,
          &protected_repository_sources,
          "Backup source",
        )?;
      }
      let mut affected_paths = bind_paths.clone();
      affected_paths.insert(run_directory.clone());
      let running = affected_running_containers(
        docker,
        &containers,
        target,
        &affected_paths,
        protected_repository_paths,
      )
      .await?;
      let mut paths =
        vec![run_directory.to_string_lossy().into_owned()];
      paths.extend(
        bind_paths
          .into_iter()
          .map(|path| path.to_string_lossy().into_owned()),
      );
      Ok(DiscoverBackupSourceResponse {
        paths,
        running_containers: running,
      })
    }
    PeripheryBackupTarget::Volume { volume_name } => {
      if volume_name.trim().is_empty() {
        return Err(anyhow!("Volume name cannot be empty"));
      }
      let volume = docker.inspect_volume(volume_name).await?;
      if volume.driver != "local"
        || volume.scope != VolumeScopeEnum::Local
      {
        return Err(anyhow!(
          "Backup v1 supports only local named volumes; '{}' uses driver '{}' with scope {:?}",
          volume.name,
          volume.driver,
          volume.scope
        ));
      }
      if !filters.include_anonymous_volumes
        && is_anonymous_volume(&volume.name, &volume.labels)
      {
        return Err(anyhow!(
          "Anonymous Docker volume '{}' is excluded by backup settings",
          volume.name
        ));
      }
      let mountpoint =
        validate_source_path(Path::new(&volume.mountpoint))?;
      validate_path_outside_internal_storage(
        &mountpoint,
        &periphery_config().stack_dir().join(".komodo-vykar"),
        "Backup source",
      )?;
      validate_path_outside_protected_repositories(
        &mountpoint,
        &protected_repository_sources,
        "Backup source",
      )?;
      let running_containers = affected_running_containers(
        docker,
        &containers,
        target,
        &BTreeSet::from([mountpoint.clone()]),
        protected_repository_paths,
      )
      .await?;
      Ok(DiscoverBackupSourceResponse {
        paths: vec![mountpoint.to_string_lossy().into_owned()],
        running_containers,
      })
    }
  }
}

fn select_bind_backup_roots(
  paths: BTreeSet<PathBuf>,
  run_directory: &Path,
  filters: &BackupSourceFilters,
) -> anyhow::Result<BTreeSet<PathBuf>> {
  let include =
    VykarPatternMatcher::new(&filters.bind_mount_include_patterns)
      .context("Invalid bind-mount include patterns")?;
  let exclude =
    VykarPatternMatcher::new(&filters.bind_mount_exclude_patterns)
      .context("Invalid bind-mount exclude patterns")?;
  let run_device = std::fs::metadata(run_directory)
    .with_context(|| {
      format!(
        "Failed to inspect Stack run-directory filesystem: {}",
        run_directory.display()
      )
    })?
    .dev();
  let mut selected = BTreeSet::new();
  for path in paths {
    let metadata = std::fs::metadata(&path).with_context(|| {
      format!(
        "Failed to inspect bind-mount filesystem: {}",
        path.display()
      )
    })?;
    if !filters.bind_mount_include_patterns.is_empty()
      && !include.matches(&path, metadata.is_dir())
    {
      continue;
    }
    if exclude.matches(&path, metadata.is_dir()) {
      continue;
    }
    if !filters.include_cross_filesystem_mounts
      && metadata.dev() != run_device
    {
      continue;
    }
    selected.insert(path);
  }
  Ok(selected)
}

fn unfiltered_source_filters() -> BackupSourceFilters {
  BackupSourceFilters {
    include_cross_filesystem_mounts: true,
    include_anonymous_volumes: true,
    ..Default::default()
  }
}

async fn resolve_protected_repository_sources(
  docker: &crate::docker::DockerClient,
  containers: &[ContainerListItem],
  protected_repository_paths: &[ProtectedRepositoryPath],
) -> anyhow::Result<Vec<PathBuf>> {
  let mut sources = BTreeSet::new();
  let own_id = komodo_backup::container::current_container_id();
  for container in containers.iter().filter(|container| {
    is_core_container(container, protected_repository_paths)
      || (container_backup_is_skipped(container)
        && !own_id
          .as_deref()
          .is_some_and(|id| container_matches_id(container, id)))
  }) {
    let inspected = docker.inspect_container(&container.name).await?;
    for mount in inspected.mounts {
      if !mount_type_affects_paths(mount.typ.as_deref()) {
        continue;
      }
      let Some(destination) = mount.destination.map(PathBuf::from)
      else {
        continue;
      };
      let Some(source) = mount.source.map(PathBuf::from) else {
        continue;
      };
      if container_backup_is_skipped(container)
        && !is_core_container(container, protected_repository_paths)
      {
        // Refusing these sources (even for stopped containers) avoids either
        // stopping the database or taking an inconsistent live database copy.
        sources.insert(source.canonicalize().unwrap_or(source));
        continue;
      }
      for repository in
        protected_repository_paths.iter().filter(|repository| {
          container_matches_id(
            container,
            &repository.core_container_id,
          )
        })
      {
        let Some(mapped) = map_path_through_mount(
          Path::new(&repository.path),
          &destination,
          &source,
        ) else {
          continue;
        };
        sources.insert(mapped.canonicalize().unwrap_or(mapped));
      }
    }
  }
  // Host-side sources must also be protected through Periphery's own mounts
  // (the standard deployment shares /data with Core). Never borrow the
  // container-side namespace of an unrelated application or a remote Core.
  if !sources.is_empty() {
    let own_id = komodo_backup::container::current_container_id()
      .context("Cannot identify the Periphery Docker container for repository protection")?;
    let own = containers
      .iter()
      .find(|container| container_matches_id(container, &own_id))
      .context(
        "Periphery container is not visible in its Docker daemon",
      )?;
    let inspected = docker.inspect_container(&own.name).await?;
    let host_sources = sources.clone();
    for mount in inspected.mounts {
      let (Some(host), Some(local)) =
        (mount.source, mount.destination)
      else {
        continue;
      };
      let host = PathBuf::from(host);
      let host = host.canonicalize().unwrap_or(host);
      for repository in &host_sources {
        if let Some(alias) =
          map_path_through_mount(repository, &host, Path::new(&local))
        {
          sources.insert(alias.canonicalize().unwrap_or(alias));
        }
      }
    }
  }
  if let Some(own) = containers.iter().find(|container| {
    own_id
      .as_deref()
      .is_some_and(|id| container_matches_id(container, id))
  }) {
    // Core and Periphery may share the development container's namespace.
    // Protect its private files even when they are not Docker-mounted paths.
    sources.extend(
      protected_repository_paths
        .iter()
        .filter(|path| {
          container_matches_id(own, &path.core_container_id)
        })
        .map(|path| PathBuf::from(&path.path)),
    );
  }
  Ok(sources.into_iter().collect())
}

fn container_matches_id(
  container: &ContainerListItem,
  id: &str,
) -> bool {
  (id.len() == 12 || id.len() == 64)
    && id.bytes().all(|byte| {
      byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
    })
    && container.id.as_deref().is_some_and(|candidate| {
      candidate.len() == 64 && candidate.starts_with(id)
    })
}

fn map_path_through_mount(
  repository: &Path,
  mount_destination: &Path,
  mount_source: &Path,
) -> Option<PathBuf> {
  if let Ok(relative) = repository.strip_prefix(mount_destination) {
    // The mount contains the repository. Protect only the corresponding
    // subtree on the host so siblings in the same shared volume remain
    // eligible backup and restore roots.
    Some(mount_source.join(relative))
  } else if mount_destination.starts_with(repository) {
    // The entire mounted source is nested beneath the repository.
    Some(mount_source.to_path_buf())
  } else {
    None
  }
}

fn validate_path_outside_protected_repositories(
  path: &Path,
  protected_repository_sources: &[PathBuf],
  label: &str,
) -> anyhow::Result<()> {
  for repository in protected_repository_sources {
    let resolved_path = resolve_existing_ancestor(path)?;
    let resolved_repository = resolve_existing_ancestor(repository)?;
    if paths_overlap(path, repository)
      || paths_overlap(&resolved_path, &resolved_repository)
    {
      return Err(ExcludedBackupSource(format!(
        "{label} '{}' overlaps protected Core, repository, or skipped-container storage '{}'",
        path.display(),
        repository.display()
      )).into());
    }
  }
  Ok(())
}

async fn discover_running_containers(
  target: &PeripheryBackupTarget,
  publish: &[RestorePublishPath],
  protected_paths: &[ProtectedRepositoryPath],
) -> anyhow::Result<Vec<String>> {
  let docker_guard = docker_client().load();
  let docker = docker_guard
    .as_ref()
    .as_ref()
    .context("Docker is unavailable")?;
  let containers = docker.list_containers().await?;
  let paths = publish
    .iter()
    .map(|item| {
      resolve_existing_ancestor(Path::new(&item.destination))
    })
    .collect::<anyhow::Result<BTreeSet<_>>>()?;
  affected_running_containers(
    docker,
    &containers,
    target,
    &paths,
    protected_paths,
  )
  .await
}

fn is_core_container(
  container: &ContainerListItem,
  protected_paths: &[ProtectedRepositoryPath],
) -> bool {
  protected_paths.iter().any(|path| {
    container_matches_id(container, &path.core_container_id)
  })
}

fn container_backup_is_skipped(
  container: &ContainerListItem,
) -> bool {
  container
    .labels
    .get("komodo.skip")
    .is_some_and(|value| value != "false")
}

fn ensure_target_not_control_plane(
  containers: &[ContainerListItem],
  target: &PeripheryBackupTarget,
  protected_paths: &[ProtectedRepositoryPath],
) -> anyhow::Result<()> {
  for container in containers.iter().filter(|container| {
    is_core_container(container, protected_paths)
      || container_backup_is_skipped(container)
  }) {
    let consumes_target = match target {
      PeripheryBackupTarget::Stack { stack, .. } => {
        container.labels.get(COMPOSE_PROJECT_LABEL)
          == Some(&stack.project_name(false))
      }
      PeripheryBackupTarget::Volume { volume_name } => {
        container.volumes.contains(volume_name)
      }
    };
    if consumes_target {
      return Err(ExcludedBackupSource(format!("Backup/restore target belongs to protected container '{}'; use Core's logical backup and retain its private recovery material separately", container.name)).into());
    }
  }
  Ok(())
}

fn running_containers_for_target(
  containers: &[ContainerListItem],
  target: &PeripheryBackupTarget,
  own_id: Option<&str>,
  protected_paths: &[ProtectedRepositoryPath],
) -> Vec<String> {
  match target {
    PeripheryBackupTarget::Stack { stack, .. } => {
      let project_name = stack.project_name(false);
      containers
        .iter()
        .filter(|container| {
          container_is_quiesce_candidate(container, own_id)
            && !is_core_container(container, protected_paths)
            && container.labels.get(COMPOSE_PROJECT_LABEL)
              == Some(&project_name)
        })
        .map(|container| container.name.clone())
        .collect()
    }
    PeripheryBackupTarget::Volume { volume_name } => containers
      .iter()
      .filter(|container| {
        container_is_quiesce_candidate(container, own_id)
          && !is_core_container(container, protected_paths)
          && container.volumes.contains(volume_name)
      })
      .map(|container| container.name.clone())
      .collect(),
  }
}

fn container_is_quiesce_candidate(
  container: &ContainerListItem,
  own_id: Option<&str>,
) -> bool {
  container.state == ContainerStateStatusEnum::Running
    && !own_id.is_some_and(|id| container_matches_id(container, id))
    && !container_backup_is_skipped(container)
}

fn validate_source_path(path: &Path) -> anyhow::Result<PathBuf> {
  if !path.is_absolute() {
    return Err(anyhow!(
      "Backup source must be absolute: {}",
      path.display()
    ));
  }
  path.canonicalize().with_context(|| {
    format!("Backup source is unavailable: {}", path.display())
  })
}

async fn run_container_command(
  action: &str,
  container: &str,
) -> anyhow::Result<()> {
  let log = run_komodo_standard_command(
    &format!("Backup {action} container {container}"),
    &format!("docker {action} -- {}", escape(container.into())),
    CommandOptions::default(),
  )
  .await;
  if log.success {
    Ok(())
  } else {
    Err(anyhow!("{}", log.stderr))
  }
}

async fn create_restore_volume(
  volume_name: &str,
  journal_id: &str,
) -> anyhow::Result<()> {
  let label = format!("{RESTORE_PLAN_VOLUME_LABEL}={journal_id}");
  let log = run_komodo_standard_command(
    &format!("Backup create restore volume {volume_name}"),
    &format!(
      "docker volume create --label {} -- {}",
      escape(label.into()),
      escape(volume_name.into())
    ),
    CommandOptions::default(),
  )
  .await;
  if log.success {
    Ok(())
  } else {
    Err(anyhow!("{}", log.stderr))
  }
}

async fn remove_restore_volume(
  volume_name: &str,
) -> anyhow::Result<()> {
  let log = run_komodo_standard_command(
    &format!("Backup remove restore volume {volume_name}"),
    &format!("docker volume rm -- {}", escape(volume_name.into())),
    CommandOptions::default(),
  )
  .await;
  if log.success {
    Ok(())
  } else {
    Err(anyhow!("{}", log.stderr))
  }
}

async fn prepare_restore_volume(
  volume_name: &str,
  restore_plan_id: &str,
  journal_id: &str,
  create_if_missing: bool,
) -> anyhow::Result<Option<PathBuf>> {
  let docker_guard = docker_client().load();
  let docker = docker_guard
    .as_ref()
    .as_ref()
    .context("Docker is unavailable")?;
  let containers = docker.list_containers().await?;
  let exists = docker
    .list_volumes(&containers)
    .await?
    .into_iter()
    .any(|volume| volume.name == volume_name);
  if !create_if_missing {
    if exists {
      return Ok(None);
    }
    return Err(anyhow!(
      "Destination volume '{volume_name}' no longer exists; create a new restore preflight"
    ));
  }
  if exists {
    let volume = docker.inspect_volume(volume_name).await?;
    if volume
      .labels
      .get(RESTORE_PLAN_VOLUME_LABEL)
      .map(String::as_str)
      != Some(restore_plan_id)
    {
      return Err(anyhow!(
        "Destination volume '{volume_name}' now exists; create a new restore preflight and explicitly confirm overwrite"
      ));
    }
  }
  let journal = persist_restore_volume_journal(
    journal_id,
    volume_name,
    restore_plan_id,
  )?;
  if !exists {
    let created = async {
      create_restore_volume(volume_name, restore_plan_id).await?;
      let volume = docker.inspect_volume(volume_name).await?;
      if volume
        .labels
        .get(RESTORE_PLAN_VOLUME_LABEL)
        .map(String::as_str)
        != Some(restore_plan_id)
      {
        return Err(anyhow!(
          "Destination volume '{volume_name}' was created concurrently by another process; restore aborted"
        ));
      }
      Ok(())
    }
    .await;
    if let Err(error) = created {
      let cleanup =
        cleanup_owned_restore_volume_journal(&journal).await;
      return match cleanup {
        Ok(()) => Err(error),
        Err(cleanup) => Err(error.context(format!(
          "Created restore Volume cleanup failed: {cleanup:#}"
        ))),
      };
    }
  }
  Ok(Some(journal))
}

async fn restart_containers(
  containers: &[String],
) -> (Vec<String>, Vec<String>) {
  let mut restarted = Vec::new();
  let mut errors = Vec::new();
  for container in containers {
    match run_container_command("start", container).await {
      Ok(()) => restarted.push(container.clone()),
      Err(error) => errors.push(format!("{container}: {error:#}")),
    }
  }
  (restarted, errors)
}

fn vykar_cache_dir(hostname: &str) -> anyhow::Result<PathBuf> {
  let directory = periphery_config()
    .stack_dir()
    .join(".komodo-vykar")
    .join(hex::encode(Sha256::digest(hostname.as_bytes())));
  std::fs::create_dir_all(&directory).with_context(|| {
    format!("Failed to create Vykar cache at {}", directory.display())
  })?;
  Ok(directory)
}

fn resolve_volume_publish_destinations(
  publish: &mut [RestorePublishPath],
  volume_name: &str,
  mountpoint: &str,
  full_restore: bool,
) -> anyhow::Result<()> {
  let mountpoint = Path::new(mountpoint);
  let logical_root = Path::new("/var/lib/docker/volumes")
    .join(volume_name)
    .join("_data");
  for item in publish {
    let destination = if full_restore {
      mountpoint.to_path_buf()
    } else {
      let relative = Path::new(&item.destination)
        .strip_prefix(&logical_root)
        .with_context(|| {
          format!(
            "Selected Volume destination '{}' is outside logical root '{}'",
            item.destination,
            logical_root.display()
          )
        })?;
      mountpoint.join(relative)
    };
    item.destination = destination.to_string_lossy().into_owned();
    if !full_restore {
      item.destination_root =
        Some(mountpoint.to_string_lossy().into_owned());
    }
  }
  Ok(())
}

impl Resolve<Args> for TransactionalVykarRestore {
  async fn resolve(
    mut self,
    args: &Args,
  ) -> anyhow::Result<TransactionalVykarRestoreResponse> {
    let _operation = backup_operation_lock().lock().await;
    let _filesystem = protected_filesystem_guard()?;
    ensure_no_pending_recovery()?;
    let current_preview = PreflightVykarRestore {
      target: self.target.clone(),
      repository: self.repository.clone(),
      protected_repository_paths: self
        .protected_repository_paths
        .clone(),
      advanced: self.advanced.clone(),
      hostname: self.hostname.clone(),
      snapshot_name: self.snapshot_name.clone(),
      selected_paths: self.selected_paths.clone(),
      publish: self.publish.clone(),
    }
    .resolve(args)
    .await?;
    if !self.expected_preview.matches(&current_preview) {
      return Err(anyhow!(
        "Restore preview changed before the destination filesystem was locked; create and review a fresh preflight"
      ));
    }
    let (_cancellation, _cancellation_registration) =
      register_operation_cancellation(&self.journal_id);
    let owned_volume_journal =
      if let PeripheryBackupTarget::Volume { volume_name } =
        &self.target
      {
        let volume_restore_plan_id =
          if self.volume_restore_plan_id.is_empty() {
            &self.journal_id
          } else {
            &self.volume_restore_plan_id
          };
        prepare_restore_volume(
          volume_name,
          volume_restore_plan_id,
          &self.journal_id,
          self.create_volume_if_missing,
        )
        .await?
      } else {
        None
      };
    let preparation = async {
      if let PeripheryBackupTarget::Volume { volume_name } =
        &self.target
      {
        let mountpoint = discover_source(
          &self.target,
          &self.protected_repository_paths,
          &unfiltered_source_filters(),
        )
        .await?
        .paths
        .into_iter()
        .next()
        .context("Destination volume has no mountpoint")?;
        resolve_volume_publish_destinations(
          &mut self.publish,
          volume_name,
          &mountpoint,
          self.selected_paths.is_empty(),
        )?;
      }
      validate_restore_destinations(
        &self.publish,
        &self.protected_repository_paths,
      )
      .await?;
      let running_containers = discover_running_containers(
        &self.target,
        &self.publish,
        &self.protected_repository_paths,
      )
      .await?;
      // Persist the complete pre-restore running set before the first stop.
      // Startup recovery can then restart every affected container after
      // repairing the filesystem and Volume ownership journal.
      let container_journal = persist_container_quiesce_journal(
        &self.journal_id,
        &running_containers,
      )?;
      anyhow::Ok((running_containers, container_journal))
    }
    .await;
    let (running_containers, container_journal) = match preparation {
      Ok(prepared) => prepared,
      Err(error) => {
        if let Some(journal) = owned_volume_journal.as_deref()
          && let Err(cleanup) =
            cleanup_owned_restore_volume_journal(journal).await
        {
          return Err(error.context(format!(
            "Created restore Volume cleanup failed: {cleanup:#}"
          )));
        }
        return Err(error);
      }
    };
    let mut stopped_containers: Vec<String> = Vec::new();
    for container in &running_containers {
      if let Err(stop_error) =
        run_container_command("stop", container).await
      {
        let (restarted, restart_errors) =
          restart_quiesced_containers(
            container_journal.as_deref(),
            &stopped_containers,
          )
          .await?;
        let volume_cleanup_error =
          if let Some(journal) = owned_volume_journal.as_deref() {
            cleanup_owned_restore_volume_journal(journal).await.err()
          } else {
            None
          };
        return Ok(TransactionalVykarRestoreResponse {
          complete: false,
          rolled_back: volume_cleanup_error.is_none(),
          finalization_pending: false,
          containers_restarted: if restart_errors.is_empty()
            && volume_cleanup_error.is_none()
          {
            restarted
          } else {
            Vec::new()
          },
          critical_error: if volume_cleanup_error.is_some()
            || !restart_errors.is_empty()
          {
            Some(format!(
              "Restore quiesce failed ({stop_error:#}); created Volume cleanup: {}; container restart: {}",
              volume_cleanup_error
                .map(|error| format!("failed: {error:#}"))
                .unwrap_or_else(|| "complete".into()),
              if restart_errors.is_empty() {
                "complete".into()
              } else {
                restart_errors.join("; ")
              }
            ))
          } else {
            None
          },
        });
      }
      stopped_containers.push(container.clone());
    }

    let restore_result = transactional_restore(&self).await;
    let rolled_back = match restore_result {
      RestoreTransactionResult::Published {
        rolled_back,
        finalization_pending,
      } => {
        if rolled_back
          && let Some(journal) = owned_volume_journal.as_deref()
          && let Err(error) =
            cleanup_owned_restore_volume_journal(journal).await
        {
          return Ok(TransactionalVykarRestoreResponse {
            complete: false,
            rolled_back: false,
            finalization_pending: false,
            containers_restarted: Vec::new(),
            critical_error: Some(format!(
              "Restore rolled back but its created Volume could not be removed; affected containers remain stopped: {error:#}"
            )),
          });
        }
        if finalization_pending {
          return Ok(TransactionalVykarRestoreResponse {
            complete: true,
            rolled_back: false,
            finalization_pending: true,
            containers_restarted: Vec::new(),
            critical_error: None,
          });
        }
        rolled_back
      }
      RestoreTransactionResult::FailedBeforePublication(error) => {
        warn!(
          "Restore failed before publication; original data is unchanged: {error:#}"
        );
        let cleanup_error =
          if let Some(journal) = owned_volume_journal.as_deref() {
            cleanup_owned_restore_volume_journal(journal).await.err()
          } else {
            None
          };
        let (restarted, restart_errors) =
          restart_quiesced_containers(
            container_journal.as_deref(),
            &stopped_containers,
          )
          .await?;
        return Ok(TransactionalVykarRestoreResponse {
          complete: false,
          rolled_back: cleanup_error.is_none(),
          finalization_pending: false,
          containers_restarted: if restart_errors.is_empty()
            && cleanup_error.is_none()
          {
            restarted
          } else {
            Vec::new()
          },
          critical_error: if cleanup_error.is_some()
            || !restart_errors.is_empty()
          {
            Some(format!(
              "Restore failed before publication ({error:#}); created Volume cleanup: {}; container restart: {}",
              cleanup_error
                .map(|error| format!("failed: {error:#}"))
                .unwrap_or_else(|| "complete".into()),
              if restart_errors.is_empty() {
                "complete".into()
              } else {
                restart_errors.join("; ")
              }
            ))
          } else {
            None
          },
        });
      }
      RestoreTransactionResult::StagingCleanupFailed(error) => {
        return Ok(TransactionalVykarRestoreResponse {
          complete: false,
          rolled_back: false,
          finalization_pending: false,
          containers_restarted: Vec::new(),
          critical_error: Some(format!(
            "Restore failed before publication; original data is unchanged, but staging recovery is required and affected containers remain stopped: {error:#}"
          )),
        });
      }
      RestoreTransactionResult::Indeterminate(error) => {
        return Ok(TransactionalVykarRestoreResponse {
          complete: false,
          rolled_back: false,
          finalization_pending: false,
          containers_restarted: Vec::new(),
          critical_error: Some(format!(
            "Restore state is indeterminate; affected containers remain stopped: {error:#}"
          )),
        });
      }
    };
    let (restarted, restart_errors) = restart_quiesced_containers(
      container_journal.as_deref(),
      &stopped_containers,
    )
    .await?;
    if restart_errors.is_empty() {
      Ok(TransactionalVykarRestoreResponse {
        complete: !rolled_back,
        rolled_back,
        finalization_pending: false,
        containers_restarted: restarted,
        critical_error: None,
      })
    } else {
      for container in &restarted {
        let _ = run_container_command("stop", container).await;
      }
      Ok(TransactionalVykarRestoreResponse {
        complete: false,
        rolled_back,
        finalization_pending: false,
        containers_restarted: Vec::new(),
        critical_error: Some(format!(
          "Container state is indeterminate; keep affected containers stopped: {}",
          restart_errors.join("; ")
        )),
      })
    }
  }
}

impl Resolve<Args> for PreflightVykarRestore {
  async fn resolve(
    mut self,
    _: &Args,
  ) -> anyhow::Result<PreflightVykarRestoreResponse> {
    let deadline = Instant::now() + RESTORE_PREFLIGHT_TIMEOUT;
    restore_preflight_before_deadline(deadline, async move {
    let permit = preflight_slots().clone().try_acquire_owned()
      .context("Another restore preflight is still running on this Periphery; retry after it finishes")?;
    let discovered = match &self.target {
      PeripheryBackupTarget::Stack { .. } => {
        // A missing Stack destination can legitimately be planned as a
        // recovered Stack; execution recreates its mapped filesystem roots.
        discover_source(
          &self.target,
          &self.protected_repository_paths,
          &unfiltered_source_filters(),
        )
        .await
        .ok()
      }
      PeripheryBackupTarget::Volume { volume_name } => {
        let docker_guard = docker_client().load();
        let docker = docker_guard
          .as_ref()
          .as_ref()
          .context("Docker is unavailable")?;
        let containers = docker.list_containers().await?;
        let exists = docker
          .list_volumes(&containers)
          .await?
          .into_iter()
          .any(|volume| volume.name == *volume_name);
        if exists {
          // Once Docker confirms the Volume exists, an unsupported driver or
          // inspect failure is a real preflight error, not evidence that the
          // destination is absent.
          Some(
            discover_source(
              &self.target,
              &self.protected_repository_paths,
              &unfiltered_source_filters(),
            )
            .await?,
          )
        } else {
          None
        }
      }
    };
    let destination_exists = discovered.is_some();
    let missing_volume = match &self.target {
      PeripheryBackupTarget::Volume { volume_name }
        if !destination_exists =>
      {
        Some(volume_name.clone())
      }
      _ => None,
    };
    if let PeripheryBackupTarget::Volume { volume_name } =
      &self.target
      && let Some(mountpoint) =
        discovered.as_ref().and_then(|source| source.paths.first())
    {
      resolve_volume_publish_destinations(
        &mut self.publish,
        volume_name,
        mountpoint,
        self.selected_paths.is_empty(),
      )?;
    }
    // A new Volume has no inspectable mountpoint. Its preview names paths
    // inside that Volume, never guessed host paths. After creation, execution
    // validates the inspected mountpoint before stopping/publishing anything.
    let running_containers = if missing_volume.is_some() {
      Vec::new()
    } else {
      validate_restore_destinations(
        &self.publish,
        &self.protected_repository_paths,
      )
      .await?;
      discover_running_containers(
        &self.target,
        &self.publish,
        &self.protected_repository_paths,
      )
      .await?
    };
    let repository = self.repository.clone();
    let advanced = self.advanced.clone();
    let hostname = self.hostname.clone();
    let snapshot = self.snapshot_name.clone();
    let selected = self.selected_paths;
    let publish = self.publish;
    let worker = tokio::task::spawn_blocking(move || {
      // Keep the slot inside the worker if a caller disconnects or times out.
      // Slow backend reads must not spawn unlimited abandoned inventories.
      let _permit = permit;
      let cache = vykar_cache_dir(&hostname)?;
      let snapshot_paths = VykarRepository::new(
        &repository,
        &hostname,
        &cache,
        &cache,
        &advanced,
      )?
      .snapshot_paths(&snapshot, &selected, deadline)?;
      if let Some(volume_name) = missing_volume {
        return compare_missing_volume_paths(
          &snapshot_paths,
          &publish,
          &volume_name,
          deadline,
        );
      }
      compare_restore_paths(
        &snapshot_paths,
        &publish,
        &selected,
        deadline,
      )
    });
    let (created_paths, overwritten_paths, deleted_paths) = worker
      .await
      .context("Restore preflight worker failed")??;
    Ok(PreflightVykarRestoreResponse {
      destination_exists,
      created_paths,
      overwritten_paths,
      deleted_paths,
      containers_to_stop: running_containers,
    })
    }).await
  }
}

async fn restore_preflight_before_deadline<T>(
  deadline: Instant,
  preflight: impl std::future::Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
  tokio::time::timeout_at(
    tokio::time::Instant::from_std(deadline),
    preflight,
  )
  .await
  .context("Restore preflight exceeded 60 seconds; no restore changes were started")?
}

fn compare_missing_volume_paths(
  snapshot_paths: &[komodo_backup::SnapshotPath],
  publish: &[RestorePublishPath],
  volume_name: &str,
  deadline: Instant,
) -> anyhow::Result<(Vec<String>, Vec<String>, Vec<String>)> {
  let logical_root = Path::new("/var/lib/docker/volumes")
    .join(volume_name)
    .join("_data");
  let mut budget = RestorePreviewBudget::new(deadline);
  let mut created = Vec::new();
  for item in snapshot_paths {
    let Some((mapping, relative)) =
      map_snapshot_path(&item.path, publish)?
    else {
      continue;
    };
    let destination = Path::new(&mapping.destination).join(relative);
    let relative = destination.strip_prefix(&logical_root).context(
      "New Volume preview destination is outside its logical root",
    )?;
    if relative
      .components()
      .any(|part| !matches!(part, std::path::Component::Normal(_)))
    {
      return Err(anyhow!(
        "New Volume preview contains an unsafe relative path"
      ));
    }
    let display = PathBuf::from(format!(
      "volume://{volume_name}/{}",
      relative.to_string_lossy()
    ));
    budget.inventory.consume(&display.to_string_lossy())?;
    budget.push(&mut created, &display)?;
  }
  created.sort();
  created.dedup();
  Ok((created, Vec::new(), Vec::new()))
}

fn compare_restore_paths(
  snapshot_paths: &[komodo_backup::SnapshotPath],
  publish: &[RestorePublishPath],
  selected: &[String],
  deadline: Instant,
) -> anyhow::Result<(Vec<String>, Vec<String>, Vec<String>)> {
  let mut budget = RestorePreviewBudget::new(deadline);
  let mut expected = HashSet::<PathBuf>::new();
  let mut created = Vec::new();
  let mut overwritten = Vec::new();
  for item in snapshot_paths {
    if Instant::now() >= deadline {
      return Err(anyhow!(
        "Restore preflight exceeded its time limit"
      ));
    }
    let Some((mapping, relative)) =
      map_snapshot_path(&item.path, publish)?
    else {
      continue;
    };
    let destination = if relative.as_os_str().is_empty() {
      PathBuf::from(&mapping.destination)
    } else {
      Path::new(&mapping.destination).join(relative)
    };
    budget.inventory.consume(&destination.to_string_lossy())?;
    expected.insert(destination.clone());
    match restore_preview_metadata(
      Path::new(&mapping.destination),
      &destination,
    )? {
      None => {
        budget.push(&mut created, &destination)?;
      }
      Some(metadata) if !item.directory || !metadata.is_dir() => {
        budget.push(&mut overwritten, &destination)?;
      }
      Some(_) => {}
    }
  }

  let restore_roots = if selected.is_empty() {
    publish
      .iter()
      .map(|mapping| PathBuf::from(&mapping.destination))
      .collect::<Vec<_>>()
  } else {
    selected
      .iter()
      .map(|selection| {
        map_snapshot_path(selection.trim_matches('/'), publish)
      })
      .collect::<anyhow::Result<Vec<_>>>()?
      .into_iter()
      .flatten()
      .map(|(mapping, relative)| {
        Path::new(&mapping.destination).join(relative)
      })
      .collect()
  };
  let mut deleted = Vec::new();
  for root in restore_roots {
    collect_unexpected_paths(
      &root,
      &expected,
      &mut deleted,
      &mut budget,
    )?;
  }
  created.sort();
  created.dedup();
  overwritten.sort();
  overwritten.dedup();
  deleted.sort();
  deleted.dedup();
  Ok((created, overwritten, deleted))
}

/// Publication replaces symlinks instead of following them. Descendants of
/// a replaced symlink (or file) are therefore created, regardless of what
/// exists beyond that link in the host filesystem.
fn restore_preview_metadata(
  root: &Path,
  destination: &Path,
) -> anyhow::Result<Option<std::fs::Metadata>> {
  let mut path = root.to_path_buf();
  let mut components = destination.strip_prefix(root)?.components();
  loop {
    let metadata = match std::fs::symlink_metadata(&path) {
      Ok(metadata) => metadata,
      Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
        return Ok(None);
      }
      Err(error) => return Err(error.into()),
    };
    let Some(component) = components.next() else {
      return Ok(Some(metadata));
    };
    if !metadata.is_dir() {
      return Ok(None);
    }
    path.push(component.as_os_str());
  }
}

fn map_snapshot_path<'a>(
  snapshot_path: &str,
  publish: &'a [RestorePublishPath],
) -> anyhow::Result<Option<(&'a RestorePublishPath, PathBuf)>> {
  let path = Path::new(snapshot_path);
  if path.is_absolute()
    || path.components().any(|component| {
      matches!(component, std::path::Component::ParentDir)
    })
  {
    return Err(anyhow!("Unsafe snapshot path in restore preflight"));
  }
  let best = publish
    .iter()
    .filter_map(|mapping| {
      let root = Path::new(mapping.snapshot_path.trim_matches('/'));
      path.strip_prefix(root).ok().map(|relative| {
        (mapping, relative.to_path_buf(), root.components().count())
      })
    })
    .max_by_key(|(_, _, depth)| *depth);
  Ok(best.map(|(mapping, relative, _)| (mapping, relative)))
}

fn collect_unexpected_paths(
  root: &Path,
  expected: &HashSet<PathBuf>,
  deleted: &mut Vec<String>,
  budget: &mut RestorePreviewBudget,
) -> anyhow::Result<()> {
  budget.inventory.consume(&root.to_string_lossy())?;
  let metadata = match std::fs::symlink_metadata(root) {
    Ok(metadata) => metadata,
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
      return Ok(());
    }
    Err(error) => return Err(error.into()),
  };
  if !expected.contains(root) {
    budget.push(deleted, root)?;
  }
  if !metadata.is_dir() {
    return Ok(());
  }
  // A bounded iterator stack avoids recursive stack overflow and does not
  // collect a wide directory's entire contents before checking the budget.
  let mut directories = vec![std::fs::read_dir(root)?];
  while let Some(directory) = directories.last_mut() {
    let Some(entry) = directory.next() else {
      directories.pop();
      continue;
    };
    let entry = entry?;
    let path = entry.path();
    budget.inventory.consume(&path.to_string_lossy())?;
    if !expected.contains(&path) {
      budget.push(deleted, &path)?;
    }
    if entry.file_type()?.is_dir() {
      if directories.len() >= MAX_RESTORE_PREVIEW_DEPTH {
        return Err(anyhow!(
          "Restore preflight destination exceeds 128 directory levels"
        ));
      }
      directories.push(std::fs::read_dir(path)?);
    }
  }
  Ok(())
}

enum RestoreTransactionResult {
  Published {
    rolled_back: bool,
    finalization_pending: bool,
  },
  FailedBeforePublication(anyhow::Error),
  StagingCleanupFailed(anyhow::Error),
  Indeterminate(anyhow::Error),
}

fn finish_restore_before_publication(
  error: Option<anyhow::Error>,
  cleanup: anyhow::Result<()>,
) -> RestoreTransactionResult {
  if let Err(cleanup) = cleanup {
    let reason = error
      .map(|error| format!("Restore failed: {error:#}"))
      .unwrap_or_else(|| "Restore was cancelled".into());
    return RestoreTransactionResult::StagingCleanupFailed(
      cleanup
        .context(format!("{reason}; restore staging cleanup failed")),
    );
  }
  match error {
    Some(error) => {
      RestoreTransactionResult::FailedBeforePublication(error)
    }
    None => RestoreTransactionResult::Published {
      rolled_back: true,
      finalization_pending: false,
    },
  }
}

async fn transactional_restore(
  request: &TransactionalVykarRestore,
) -> RestoreTransactionResult {
  if request.publish.is_empty() {
    return RestoreTransactionResult::FailedBeforePublication(
      anyhow!("Restore publish plan is empty"),
    );
  }
  if operation_cancelled(&request.journal_id) {
    return RestoreTransactionResult::Published {
      rolled_back: true,
      finalization_pending: false,
    };
  }
  if let Err(error) = validate_restore_destinations(
    &request.publish,
    &request.protected_repository_paths,
  )
  .await
  {
    return RestoreTransactionResult::FailedBeforePublication(error);
  }
  let first_destination =
    PathBuf::from(&request.publish[0].destination);
  let Some(parent) = first_destination.parent() else {
    return RestoreTransactionResult::FailedBeforePublication(
      anyhow!("Restore destination has no parent"),
    );
  };
  let parent = parent.to_path_buf();
  let staging =
    parent.join(format!(".komodo-restore-{}", request.journal_id));
  if path_lexists(&staging) {
    return RestoreTransactionResult::FailedBeforePublication(
      anyhow!("Restore staging path already exists"),
    );
  }
  let staging_journal = match persist_restore_staging_journal(
    &request.journal_id,
    std::slice::from_ref(&staging),
  ) {
    Ok(path) => path,
    Err(error) => {
      return RestoreTransactionResult::FailedBeforePublication(
        error,
      );
    }
  };

  let repository = request.repository.clone();
  let advanced = request.advanced.clone();
  let hostname = request.hostname.clone();
  let snapshot = request.snapshot_name.clone();
  let selected = request.selected_paths.clone();
  let restore_staging = staging.clone();
  let restore_result = tokio::task::spawn_blocking(move || {
    let cache = vykar_cache_dir(&hostname)?;
    let repository = VykarRepository::new(
      &repository,
      &hostname,
      &cache,
      &cache,
      &advanced,
    )?;
    repository.restore(&snapshot, &restore_staging, &selected)
  })
  .await;
  match restore_result {
    Ok(Ok(())) => {}
    Ok(Err(error)) => {
      return finish_restore_before_publication(
        Some(error),
        cleanup_restore_staging_journal(&staging_journal),
      );
    }
    Err(error) => {
      return finish_restore_before_publication(
        Some(
          anyhow::Error::new(error)
            .context("Vykar restore worker failed"),
        ),
        cleanup_restore_staging_journal(&staging_journal),
      );
    }
  }

  if let Err(error) =
    rewrite_recovered_stack_compose_files(request, &staging)
  {
    return finish_restore_before_publication(
      Some(error),
      cleanup_restore_staging_journal(&staging_journal),
    );
  }

  if operation_cancelled(&request.journal_id) {
    return finish_restore_before_publication(
      None,
      cleanup_restore_staging_journal(&staging_journal),
    );
  }

  let publish = request.publish.clone();
  let journal_id = request.journal_id.clone();
  let publication_started = Arc::new(AtomicBool::new(false));
  let worker_started = publication_started.clone();
  let publish_staging = staging.clone();
  let publication_staging_journal = staging_journal.clone();
  let defer_finalize = request.defer_finalize;
  let result = tokio::task::spawn_blocking(move || {
    publish_restore(
      &publish_staging,
      &publish,
      &journal_id,
      &worker_started,
      Some(&publication_staging_journal),
      defer_finalize,
    )
  })
  .await;
  match result {
    Ok(Ok(rolled_back)) => RestoreTransactionResult::Published {
      rolled_back,
      finalization_pending: request.defer_finalize && !rolled_back,
    },
    Ok(Err(error)) => {
      if publication_started.load(Ordering::SeqCst) {
        RestoreTransactionResult::Indeterminate(error)
      } else {
        finish_restore_before_publication(
          Some(error),
          cleanup_restore_staging_journal(&staging_journal),
        )
      }
    }
    Err(error) => {
      let error = anyhow::Error::new(error)
        .context("Restore publish worker failed");
      if publication_started.load(Ordering::SeqCst) {
        RestoreTransactionResult::Indeterminate(error)
      } else {
        finish_restore_before_publication(
          Some(error),
          cleanup_restore_staging_journal(&staging_journal),
        )
      }
    }
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RestoreJournalEntry {
  source: PathBuf,
  destination: PathBuf,
  rollback: PathBuf,
  /// `None` denotes a legacy journal whose original-destination state is
  /// ambiguous and must be recovered conservatively.
  #[serde(default)]
  original_existed: Option<bool>,
  published: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RestoreJournal {
  staging: PathBuf,
  entries: Vec<RestoreJournalEntry>,
  #[serde(default)]
  committed: bool,
  /// Filesystem commit/rollback completed, but the journal remains durable
  /// until every quiesced container has restarted. This makes finalization
  /// idempotent across transient Docker failures.
  #[serde(default)]
  finalized: bool,
  /// Core must decide deferred recovered-Stack publications. Periphery never
  /// rolls an undecided deferred journal back during startup.
  #[serde(default)]
  deferred: bool,
  /// Filesystem finalization and container recovery both completed. Deferred
  /// journals retain this receipt until Core acknowledges its durable state.
  #[serde(default)]
  completed: bool,
  /// A Volume created specifically for this restore. The same durable
  /// journal owns both filesystem rollback and removal of the side effect
  /// until publication is committed.
  #[serde(default)]
  owned_volume: Option<RestoreOwnedVolume>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RestoreOwnedVolume {
  volume_name: String,
  restore_plan_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RestoreStagingJournal {
  paths: Vec<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ContainerQuiesceJournal {
  containers: Vec<String>,
}

#[derive(Default)]
struct RemovePathsOnDrop(Vec<PathBuf>);

impl Drop for RemovePathsOnDrop {
  fn drop(&mut self) {
    for path in self.0.iter().rev() {
      let _ = remove_path(path);
    }
  }
}

fn restore_journal_dir() -> anyhow::Result<PathBuf> {
  let directory = periphery_config()
    .stack_dir()
    .join(".komodo-vykar")
    .join("restore-journals");
  std::fs::create_dir_all(&directory)?;
  Ok(directory)
}

/// Check durable journals under the operation lock before any new side effect.
/// Even a completed deferred receipt remains owned until Core acknowledges it.
/// Finalize and startup recovery deliberately bypass this gate to repair state.
pub(crate) fn ensure_no_pending_recovery() -> anyhow::Result<()> {
  crate::stack::delete::ensure_no_pending_deletions()?;
  ensure_recovery_directories_empty(&[
    restore_journal_dir()?,
    container_quiesce_journal_dir()?,
    restore_staging_journal_dir()?,
  ])
}

fn ensure_recovery_directories_empty(
  directories: &[PathBuf],
) -> anyhow::Result<()> {
  for directory in directories {
    for entry in std::fs::read_dir(directory)? {
      let path = entry?.path();
      if path.extension().and_then(|value| value.to_str())
        == Some("json")
      {
        return Err(anyhow!(
          "Backup/restore blocked by unresolved recovery journal '{}'; finish Core reconciliation or restart Periphery to recover it before retrying",
          path.display()
        ));
      }
    }
  }
  Ok(())
}

/// Protect the node's filesystem for real worker/process lifetimes, including
/// terminals and background mutations that outlive their request connections.
pub(crate) fn filesystem_barrier()
-> &'static Arc<tokio::sync::RwLock<()>> {
  static BARRIER: OnceLock<Arc<tokio::sync::RwLock<()>>> =
    OnceLock::new();
  BARRIER.get_or_init(|| Arc::new(tokio::sync::RwLock::new(())))
}

/// Mutating jobs and recovery actions retain this lease for their real
/// filesystem lifetime, independently of the request that launched them.
pub(crate) fn filesystem_mutation_guard()
-> anyhow::Result<tokio::sync::OwnedRwLockReadGuard<()>> {
  filesystem_barrier().clone().try_read_owned().context(
    "Filesystem operation blocked by a backup or restore on this Server; retry after it finishes",
  )
}

fn protected_filesystem_guard()
-> anyhow::Result<tokio::sync::OwnedRwLockWriteGuard<()>> {
  filesystem_barrier().clone().try_write_owned().context(
    "Backup/restore blocked by a running terminal, file operation, or Stack deletion on this Server; finish the operation or exit/delete its terminals, then retry",
  )
}

fn restore_journal_path(journal_id: &str) -> anyhow::Result<PathBuf> {
  Ok(restore_journal_dir()?.join(format!("{journal_id}.json")))
}

fn persist_restore_volume_journal(
  journal_id: &str,
  volume_name: &str,
  restore_plan_id: &str,
) -> anyhow::Result<PathBuf> {
  let path = restore_journal_path(journal_id)?;
  if path_lexists(&path) {
    return Err(anyhow!(
      "A restore journal already exists for operation '{journal_id}'"
    ));
  }
  persist_journal(
    &path,
    &RestoreJournal {
      staging: PathBuf::new(),
      entries: Vec::new(),
      committed: false,
      finalized: false,
      deferred: false,
      completed: false,
      owned_volume: Some(RestoreOwnedVolume {
        volume_name: volume_name.to_string(),
        restore_plan_id: restore_plan_id.to_string(),
      }),
    },
  )?;
  Ok(path)
}

async fn remove_owned_restore_volume(
  owned: &RestoreOwnedVolume,
) -> anyhow::Result<()> {
  let docker_guard = docker_client().load();
  let docker = docker_guard
    .as_ref()
    .as_ref()
    .context("Docker is unavailable")?;
  let containers = docker.list_containers().await?;
  let exists = docker
    .list_volumes(&containers)
    .await?
    .into_iter()
    .any(|volume| volume.name == owned.volume_name);
  if !exists {
    return Ok(());
  }
  let volume = docker.inspect_volume(&owned.volume_name).await?;
  if volume
    .labels
    .get(RESTORE_PLAN_VOLUME_LABEL)
    .map(String::as_str)
    != Some(owned.restore_plan_id.as_str())
  {
    warn!(
      "Restore journal no longer owns Volume '{}'; leaving it untouched",
      owned.volume_name
    );
    return Ok(());
  }
  remove_restore_volume(&owned.volume_name).await
}

async fn cleanup_owned_restore_volume_journal(
  path: &Path,
) -> anyhow::Result<()> {
  let bytes = std::fs::read(path).with_context(|| {
    format!("Failed to read restore journal {}", path.display())
  })?;
  let journal: RestoreJournal = serde_json::from_slice(&bytes)
    .with_context(|| {
      format!("Failed to decode restore journal {}", path.display())
    })?;
  if let Some(owned) = &journal.owned_volume {
    remove_owned_restore_volume(owned).await?;
  }
  remove_path(path)?;
  fsync_parent(path)
}

fn restore_staging_journal_dir() -> anyhow::Result<PathBuf> {
  let directory = periphery_config()
    .stack_dir()
    .join(".komodo-vykar")
    .join("restore-staging-journals");
  std::fs::create_dir_all(&directory)?;
  Ok(directory)
}

fn persist_restore_staging_journal(
  journal_id: &str,
  paths: &[PathBuf],
) -> anyhow::Result<PathBuf> {
  let path =
    restore_staging_journal_dir()?.join(format!("{journal_id}.json"));
  persist_journal(
    &path,
    &RestoreStagingJournal {
      paths: paths.to_vec(),
    },
  )?;
  Ok(path)
}

fn cleanup_restore_staging_journal(
  path: &Path,
) -> anyhow::Result<()> {
  let bytes = std::fs::read(path).with_context(|| {
    format!(
      "Failed to read restore staging journal {}",
      path.display()
    )
  })?;
  let journal: RestoreStagingJournal = serde_json::from_slice(&bytes)
    .with_context(|| {
      format!(
        "Failed to decode restore staging journal {}",
        path.display()
      )
    })?;
  for owned in journal.paths.iter().rev() {
    remove_path(owned)?;
    fsync_parent(owned)?;
  }
  remove_path(path)?;
  fsync_parent(path)
}

fn container_quiesce_journal_dir() -> anyhow::Result<PathBuf> {
  let directory = periphery_config()
    .stack_dir()
    .join(".komodo-vykar")
    .join("container-quiesce-journals");
  std::fs::create_dir_all(&directory)?;
  Ok(directory)
}

fn persist_container_quiesce_journal(
  journal_id: &str,
  containers: &[String],
) -> anyhow::Result<Option<PathBuf>> {
  let path = container_quiesce_journal_dir()?
    .join(format!("{journal_id}.json"));
  let existing = if path_lexists(&path) {
    read_container_quiesce_journal(&path)?.containers
  } else {
    Vec::new()
  };
  let containers =
    merge_container_quiesce_sets(&existing, containers);
  if containers.is_empty() {
    return Ok(None);
  }
  persist_journal(&path, &ContainerQuiesceJournal { containers })?;
  Ok(Some(path))
}

fn merge_container_quiesce_sets(
  existing: &[String],
  current: &[String],
) -> Vec<String> {
  existing
    .iter()
    .chain(current)
    .cloned()
    .collect::<BTreeSet<_>>()
    .into_iter()
    .collect()
}

fn read_container_quiesce_journal(
  path: &Path,
) -> anyhow::Result<ContainerQuiesceJournal> {
  serde_json::from_slice(&std::fs::read(path).with_context(|| {
    format!(
      "Failed to read container quiesce journal {}",
      path.display()
    )
  })?)
  .with_context(|| {
    format!(
      "Failed to decode container quiesce journal {}",
      path.display()
    )
  })
}

fn remove_container_quiesce_journal(
  path: Option<&Path>,
) -> anyhow::Result<()> {
  let Some(path) = path else {
    return Ok(());
  };
  remove_path(path)?;
  fsync_parent(path)
}

async fn restart_container_quiesce_journal(
  path: &Path,
) -> anyhow::Result<(Vec<String>, Vec<String>)> {
  if !path_lexists(path) {
    return Ok(Default::default());
  }
  let journal = read_container_quiesce_journal(path)?;
  let result = restart_containers(&journal.containers).await;
  if result.1.is_empty() {
    remove_container_quiesce_journal(Some(path))?;
  }
  Ok(result)
}

async fn restart_quiesced_containers(
  journal: Option<&Path>,
  stopped_this_attempt: &[String],
) -> anyhow::Result<(Vec<String>, Vec<String>)> {
  if let Some(journal) = journal {
    restart_container_quiesce_journal(journal).await
  } else {
    Ok(restart_containers(stopped_this_attempt).await)
  }
}

/// Recover publications with a durable decision, then restart containers
/// quiesced by an interrupted backup or restore. Undecided deferred
/// recovered-Stack publications remain intact for Core reconciliation. This
/// runs before Periphery accepts requests.
pub(crate) async fn recover_restore_journals() -> anyhow::Result<()> {
  let manifest_staging = backup_manifest_staging_dir();
  remove_path(&manifest_staging)?;
  std::fs::create_dir_all(&manifest_staging)?;
  let directory = restore_journal_dir()?;
  let mut deferred_journal_ids = HashSet::new();
  for entry in std::fs::read_dir(&directory)? {
    let path = entry?.path();
    if path.extension().and_then(|value| value.to_str())
      != Some("json")
    {
      continue;
    }
    let bytes = std::fs::read(&path).with_context(|| {
      format!("Failed to read restore journal {}", path.display())
    })?;
    let mut journal: RestoreJournal = serde_json::from_slice(&bytes)
      .with_context(|| {
        format!("Failed to decode restore journal {}", path.display())
      })?;
    let journal_id = path
      .file_stem()
      .and_then(|value| value.to_str())
      .context("Restore journal has an invalid file name")?;
    if journal.deferred {
      deferred_journal_ids.insert(journal_id.to_string());
      // An uncommitted, unfinalized deferred journal belongs to a recovered
      // Stack saga. Only Core can prove whether its resource insert happened,
      // so startup must leave both publication and containers untouched.
      if !journal.committed && !journal.finalized {
        continue;
      }
      if !journal.finalized {
        for entry in &journal.entries {
          remove_path(&entry.rollback)?;
          fsync_parent(&entry.destination)?;
          remove_path(&entry.source)?;
          fsync_parent(&entry.source)?;
        }
        if !journal.staging.as_os_str().is_empty() {
          remove_path(&journal.staging)?;
          fsync_parent(&journal.staging)?;
        }
        journal.finalized = true;
        persist_journal(&path, &journal)?;
      }
      if !journal.completed {
        let container_path = container_quiesce_journal_dir()?
          .join(format!("{journal_id}.json"));
        let (_, errors) =
          restart_container_quiesce_journal(&container_path).await?;
        if !errors.is_empty() {
          return Err(anyhow!(
            "Failed to recover containers from finalized deferred restore {}: {}",
            path.display(),
            errors.join("; ")
          ));
        }
        journal.completed = true;
        persist_journal(&path, &journal)?;
      }
      // Keep the completed receipt until Core durably records and
      // acknowledges the matching recovered Stack outcome.
      continue;
    }
    if journal.committed {
      for entry in &journal.entries {
        remove_path(&entry.rollback)?;
        fsync_parent(&entry.destination)?;
      }
    } else {
      rollback_published(&mut journal, &path)?;
    }
    for entry in &journal.entries {
      remove_path(&entry.source)?;
      fsync_parent(&entry.source)?;
    }
    if !journal.staging.as_os_str().is_empty() {
      remove_path(&journal.staging)?;
      fsync_parent(&journal.staging)?;
    }
    if !journal.committed
      && let Some(owned) = &journal.owned_volume
    {
      remove_owned_restore_volume(owned).await?;
    }
    remove_path(&path)?;
    fsync_parent(&path)?;
    warn!("Recovered interrupted restore journal {}", path.display());
  }
  let directory = restore_staging_journal_dir()?;
  for entry in std::fs::read_dir(&directory)? {
    let path = entry?.path();
    if path.extension().and_then(|value| value.to_str())
      != Some("json")
    {
      continue;
    }
    cleanup_restore_staging_journal(&path)?;
    warn!(
      "Removed staging from interrupted restore journal {}",
      path.display()
    );
  }
  let directory = container_quiesce_journal_dir()?;
  for entry in std::fs::read_dir(&directory)? {
    let path = entry?.path();
    if path.extension().and_then(|value| value.to_str())
      != Some("json")
    {
      continue;
    }
    if path
      .file_stem()
      .and_then(|value| value.to_str())
      .is_some_and(|id| deferred_journal_ids.contains(id))
    {
      continue;
    }
    let (_, errors) =
      restart_container_quiesce_journal(&path).await?;
    if !errors.is_empty() {
      return Err(anyhow!(
        "Failed to recover containers from interrupted backup/restore {}: {}",
        path.display(),
        errors.join("; ")
      ));
    }
    warn!(
      "Restarted containers from interrupted backup/restore journal {}",
      path.display()
    );
  }
  Ok(())
}

fn publish_restore(
  staging: &Path,
  publish: &[RestorePublishPath],
  journal_id: &str,
  publication_started: &AtomicBool,
  staging_journal_path: Option<&Path>,
  defer_finalize: bool,
) -> anyhow::Result<bool> {
  let journal_directory = restore_journal_dir()?;
  publish_restore_in(
    staging,
    publish,
    journal_id,
    publication_started,
    &journal_directory,
    staging_journal_path,
    defer_finalize,
  )
}

fn restore_rollback_path(
  destination: &Path,
  journal_id: &str,
) -> anyhow::Result<PathBuf> {
  let parent = destination
    .parent()
    .context("Restore destination has no parent")?;
  let mut name = destination
    .file_name()
    .context("Restore destination has no file name")?
    .to_os_string();
  name.push(format!(".komodo-rollback-{journal_id}"));
  Ok(parent.join(name))
}

fn publish_restore_in(
  staging: &Path,
  publish: &[RestorePublishPath],
  journal_id: &str,
  publication_started: &AtomicBool,
  journal_directory: &Path,
  staging_journal_path: Option<&Path>,
  defer_finalize: bool,
) -> anyhow::Result<bool> {
  validate_resolved_restore_destinations(publish)?;
  let mut entries = Vec::new();
  let mut rollback_paths = HashSet::new();
  let mut preparation_cleanup = RemovePathsOnDrop::default();
  let mut staging_ownership = RestoreStagingJournal {
    paths: vec![staging.to_path_buf()],
  };
  for (index, item) in publish.iter().enumerate() {
    let relative = Path::new(&item.snapshot_path);
    if relative.is_absolute()
      || relative.components().any(|component| {
        matches!(component, std::path::Component::ParentDir)
      })
    {
      return Err(anyhow!("Unsafe snapshot publish path"));
    }
    let destination = PathBuf::from(&item.destination);
    if !destination.is_absolute() {
      return Err(anyhow!("Restore destination must be absolute"));
    }
    let destination_parent = destination
      .parent()
      .context("Restore destination has no parent")?;
    let original_existed = path_lexists(&destination);
    let rollback = restore_rollback_path(&destination, journal_id)?;
    if !rollback_paths.insert(rollback.clone()) {
      return Err(anyhow!(
        "Restore destinations produce the same rollback path: {}",
        rollback.display()
      ));
    }
    if path_lexists(&rollback) {
      return Err(anyhow!(
        "Rollback path already exists: {}",
        rollback.display()
      ));
    }
    let restored_source = staging.join(relative);
    if !path_lexists(&restored_source) {
      return Err(anyhow!(
        "Restored snapshot path is missing: {}",
        item.snapshot_path
      ));
    }
    std::fs::create_dir_all(destination_parent)?;
    let source = destination_parent
      .join(format!(".komodo-restore-{journal_id}-{index}"));
    if path_lexists(&source) {
      return Err(anyhow!(
        "Same-filesystem restore staging path already exists: {}",
        source.display()
      ));
    }
    if let Some(staging_journal_path) = staging_journal_path {
      staging_ownership.paths.push(source.clone());
      persist_journal(staging_journal_path, &staging_ownership)?;
    }
    preparation_cleanup.0.push(source.clone());
    let copy = std::process::Command::new("cp")
      .arg("-a")
      .arg("--")
      .arg(&restored_source)
      .arg(&source)
      .output()
      .context("Failed to start metadata-preserving restore copy")?;
    if !copy.status.success() {
      return Err(anyhow!(
        "Failed to stage restore on destination filesystem: {}",
        String::from_utf8_lossy(&copy.stderr)
      ));
    }
    if tree_digest(&restored_source)? != tree_digest(&source)? {
      return Err(anyhow!(
        "Same-filesystem restore staging verification failed"
      ));
    }
    sync_tree(&source)?;
    entries.push(RestoreJournalEntry {
      source,
      destination,
      rollback,
      original_existed: Some(original_existed),
      published: false,
    });
  }

  if entries
    .iter()
    .any(|entry| !destination_existence_matches(entry))
  {
    return Err(anyhow!(
      "Restore destination existence changed during publication preparation"
    ));
  }

  std::fs::create_dir_all(journal_directory)?;
  let journal_path =
    journal_directory.join(format!("{journal_id}.json"));
  let owned_volume = if path_lexists(&journal_path) {
    let existing: RestoreJournal =
      serde_json::from_slice(&std::fs::read(&journal_path)?)
        .with_context(|| {
          format!(
            "Failed to decode pre-publication restore journal {}",
            journal_path.display()
          )
        })?;
    if existing.committed
      || !existing.entries.is_empty()
      || existing.owned_volume.is_none()
    {
      return Err(anyhow!(
        "Restore journal already contains publication state"
      ));
    }
    existing.owned_volume
  } else {
    None
  };
  let mut journal = RestoreJournal {
    staging: staging.to_path_buf(),
    entries,
    committed: false,
    finalized: false,
    deferred: defer_finalize,
    completed: false,
    owned_volume,
  };
  persist_journal(&journal_path, &journal)?;
  // The durable journal owns cleanup from this point onward.
  preparation_cleanup.0.clear();
  publication_started.store(true, Ordering::SeqCst);
  if let Some(staging_journal_path) = staging_journal_path {
    remove_path(staging_journal_path)?;
    fsync_parent(staging_journal_path)?;
  }

  for index in 0..journal.entries.len() {
    if !destination_existence_matches(&journal.entries[index]) {
      rollback_published(&mut journal, &journal_path)?;
      cleanup_rolled_back_restore(&journal, &journal_path)?;
      return Ok(true);
    }
    if path_lexists(&journal.entries[index].destination) {
      if let Err(error) = std::fs::rename(
        &journal.entries[index].destination,
        &journal.entries[index].rollback,
      ) {
        rollback_published(&mut journal, &journal_path)?;
        warn!(
          "Restore rollback preparation failed and earlier publications were rolled back: {error:#}"
        );
        cleanup_rolled_back_restore(&journal, &journal_path)?;
        return Ok(true);
      }
      // Make destination -> rollback durable before the journal claims this
      // entry was published. Recovery must never remove original data after a
      // power loss that discarded the rename.
      fsync_parent(&journal.entries[index].destination)?;
    }
    // Persist publication intent before source -> destination. On recovery,
    // this distinguishes a newly-created destination (which has no rollback
    // path) from an entry that was never reached.
    journal.entries[index].published = true;
    persist_journal(&journal_path, &journal)?;
    if let Err(error) = std::fs::rename(
      &journal.entries[index].source,
      &journal.entries[index].destination,
    ) {
      rollback_published(&mut journal, &journal_path)?;
      warn!("Restore publish failed and was rolled back: {error:#}");
      cleanup_rolled_back_restore(&journal, &journal_path)?;
      return Ok(true);
    }
    fsync_parent(&journal.entries[index].destination)?;
  }

  if defer_finalize {
    // Core creates the recovered Stack only after publication. Preserve the
    // uncommitted durable journal and rollback trees until it explicitly
    // confirms that the database insert succeeded.
    return Ok(false);
  }

  journal.committed = true;
  persist_journal(&journal_path, &journal)?;
  for entry in &journal.entries {
    if path_lexists(&entry.rollback) {
      remove_path(&entry.rollback)?;
    }
    fsync_parent(&entry.destination)?;
  }
  remove_path(staging)?;
  fsync_parent(staging)?;
  std::fs::remove_file(&journal_path)?;
  fsync_parent(&journal_path)?;
  Ok(false)
}

fn destination_existence_matches(
  entry: &RestoreJournalEntry,
) -> bool {
  entry.original_existed == Some(path_lexists(&entry.destination))
}

fn cleanup_rolled_back_restore(
  journal: &RestoreJournal,
  journal_path: &Path,
) -> anyhow::Result<()> {
  for entry in &journal.entries {
    remove_path(&entry.source)?;
    fsync_parent(&entry.source)?;
  }
  if !journal.staging.as_os_str().is_empty() {
    remove_path(&journal.staging)?;
    fsync_parent(&journal.staging)?;
  }
  if journal.owned_volume.is_some() {
    // Keep only the durable Volume ownership record. The async caller removes
    // that side effect after the synchronous filesystem rollback completes;
    // startup recovery performs the same action after a crash.
    persist_journal(
      journal_path,
      &RestoreJournal {
        staging: PathBuf::new(),
        entries: Vec::new(),
        committed: false,
        finalized: false,
        deferred: journal.deferred,
        completed: false,
        owned_volume: journal.owned_volume.clone(),
      },
    )
  } else {
    remove_path(journal_path)?;
    fsync_parent(journal_path)
  }
}

fn path_lexists(path: &Path) -> bool {
  std::fs::symlink_metadata(path).is_ok()
}

fn remove_path(path: &Path) -> anyhow::Result<()> {
  let Ok(metadata) = std::fs::symlink_metadata(path) else {
    return Ok(());
  };
  if metadata.file_type().is_dir() {
    std::fs::remove_dir_all(path)?;
  } else {
    std::fs::remove_file(path)?;
  }
  Ok(())
}

fn tree_digest(root: &Path) -> anyhow::Result<Vec<u8>> {
  fn update(
    path: &Path,
    relative: &Path,
    digest: &mut Sha256,
  ) -> anyhow::Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    digest.update(relative.to_string_lossy().as_bytes());
    digest.update(metadata.permissions().mode().to_le_bytes());
    digest.update(metadata.len().to_le_bytes());
    digest.update(metadata.uid().to_le_bytes());
    digest.update(metadata.gid().to_le_bytes());
    digest.update(metadata.mtime().to_le_bytes());
    digest.update(metadata.mtime_nsec().to_le_bytes());
    let mut attribute_names = xattr::list(path)?.collect::<Vec<_>>();
    attribute_names.sort();
    for name in attribute_names {
      digest.update(name.as_encoded_bytes());
      if let Some(value) = xattr::get(path, &name)? {
        digest.update(value);
      }
    }
    if metadata.file_type().is_symlink() {
      digest.update(b"symlink");
      digest.update(
        std::fs::read_link(path)?.to_string_lossy().as_bytes(),
      );
    } else if metadata.is_dir() {
      digest.update(b"directory");
      let mut entries =
        std::fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
      entries.sort_by_key(|entry| entry.file_name());
      for entry in entries {
        update(
          &entry.path(),
          &relative.join(entry.file_name()),
          digest,
        )?;
      }
    } else if metadata.is_file() {
      digest.update(b"file");
      let mut file = std::fs::File::open(path)?;
      let mut buffer = [0_u8; 1024 * 128];
      loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
          break;
        }
        digest.update(&buffer[..read]);
      }
    }
    Ok(())
  }
  let mut digest = Sha256::new();
  update(root, Path::new(""), &mut digest)?;
  Ok(digest.finalize().to_vec())
}

fn sync_tree(root: &Path) -> anyhow::Result<()> {
  let metadata = std::fs::symlink_metadata(root)?;
  if metadata.file_type().is_symlink() {
    return Ok(());
  }
  if metadata.is_dir() {
    for entry in std::fs::read_dir(root)? {
      sync_tree(&entry?.path())?;
    }
    std::fs::File::open(root)?.sync_all()?;
  } else if metadata.is_file() {
    std::fs::File::open(root)?.sync_all()?;
  }
  Ok(())
}

fn rollback_published(
  restore: &mut RestoreJournal,
  journal_path: &Path,
) -> anyhow::Result<()> {
  for index in (0..restore.entries.len()).rev() {
    let entry = &restore.entries[index];
    let published = entry.published;
    let rollback = entry.rollback.clone();
    let destination = entry.destination.clone();
    let rollback_exists = path_lexists(&rollback);
    match entry.original_existed {
      Some(true) => {
        // If rollback still exists it is the authoritative original. If it no
        // longer exists, destination is either the untouched original or the
        // already-restored original from a crash after the durable rename.
        if rollback_exists {
          if path_lexists(&destination) {
            remove_path(&destination)?;
          }
          std::fs::rename(&rollback, &destination)?;
          fsync_parent(&destination)?;
        }
      }
      Some(false) => {
        if published && path_lexists(&destination) {
          remove_path(&destination)?;
          fsync_parent(&destination)?;
        }
      }
      None => {
        // A rollback path proves that a legacy entry had an original. Without
        // it, `published = true` is ambiguous, so fail closed rather than risk
        // deleting a restored original.
        if rollback_exists {
          if path_lexists(&destination) {
            remove_path(&destination)?;
          }
          std::fs::rename(&rollback, &destination)?;
          fsync_parent(&destination)?;
        } else if published {
          return Err(anyhow!(
            "Legacy restore journal is ambiguous for destination {}",
            destination.display()
          ));
        }
      }
    }
    restore.entries[index].published = false;
    persist_journal(journal_path, restore)?;
  }
  Ok(())
}

fn persist_journal<T: Serialize>(
  path: &Path,
  journal: &T,
) -> anyhow::Result<()> {
  let temporary = path.with_extension("tmp");
  let bytes = serde_json::to_vec(journal)?;
  let mut file = OpenOptions::new()
    .create(true)
    .truncate(true)
    .write(true)
    .open(&temporary)?;
  file.write_all(&bytes)?;
  file.sync_all()?;
  std::fs::rename(&temporary, path)?;
  fsync_parent(path)
}

fn fsync_parent(path: &Path) -> anyhow::Result<()> {
  let parent = path.parent().context("Path has no parent")?;
  std::fs::File::open(parent)?.sync_all()?;
  Ok(())
}

async fn finalize_restore_publication(
  journal_id: &str,
  commit: bool,
  acknowledge: bool,
) -> anyhow::Result<FinalizeVykarRestoreResponse> {
  let journal_path =
    restore_journal_dir()?.join(format!("{journal_id}.json"));
  let bytes = match std::fs::read(&journal_path) {
    Ok(bytes) => bytes,
    Err(error)
      if acknowledge
        && error.kind() == std::io::ErrorKind::NotFound =>
    {
      return Ok(FinalizeVykarRestoreResponse {
        complete: true,
        rolled_back: !commit,
        ..Default::default()
      });
    }
    Err(error) => {
      return Err(error).with_context(|| {
        format!(
          "Pending restore publication does not exist: {}",
          journal_path.display()
        )
      });
    }
  };
  let mut journal: RestoreJournal = serde_json::from_slice(&bytes)
    .with_context(|| {
      format!(
        "Failed to decode pending restore publication {}",
        journal_path.display()
      )
    })?;
  if journal.finalized {
    if journal.committed != commit {
      return Err(anyhow!(
        "Restore was already finalized with the opposite decision"
      ));
    }
  } else if commit {
    // Make the decision durable before discarding rollback data. Startup
    // recovery will finish a committed cleanup after a power loss.
    journal.committed = true;
    persist_journal(&journal_path, &journal)?;
    for entry in &journal.entries {
      remove_path(&entry.rollback)?;
      fsync_parent(&entry.destination)?;
      remove_path(&entry.source)?;
      fsync_parent(&entry.source)?;
    }
    if !journal.staging.as_os_str().is_empty() {
      remove_path(&journal.staging)?;
      fsync_parent(&journal.staging)?;
    }
    journal.finalized = true;
    persist_journal(&journal_path, &journal)?;
  } else {
    if journal.committed {
      return Err(anyhow!(
        "Restore commit is already durable and cannot be rolled back"
      ));
    }
    rollback_published(&mut journal, &journal_path)?;
    for entry in &journal.entries {
      remove_path(&entry.source)?;
      fsync_parent(&entry.source)?;
    }
    if !journal.staging.as_os_str().is_empty() {
      remove_path(&journal.staging)?;
      fsync_parent(&journal.staging)?;
    }
    if let Some(owned) = &journal.owned_volume {
      remove_owned_restore_volume(owned).await?;
    }
    journal.finalized = true;
    persist_journal(&journal_path, &journal)?;
  }

  if journal.completed {
    if acknowledge || !journal.deferred {
      remove_path(&journal_path)?;
      fsync_parent(&journal_path)?;
    }
    return Ok(FinalizeVykarRestoreResponse {
      complete: true,
      rolled_back: !commit,
      ..Default::default()
    });
  }

  let container_journal_path = container_quiesce_journal_dir()?
    .join(format!("{journal_id}.json"));
  let (restarted, restart_errors) =
    restart_container_quiesce_journal(&container_journal_path)
      .await?;
  if !restart_errors.is_empty() {
    return Ok(FinalizeVykarRestoreResponse {
      complete: false,
      rolled_back: !commit,
      containers_restarted: Vec::new(),
      critical_error: Some(format!(
        "Restore was finalized but affected containers could not all be restarted: {}",
        restart_errors.join("; ")
      )),
    });
  }
  journal.completed = true;
  persist_journal(&journal_path, &journal)?;
  // Deferred recovered-Stack publications retain a durable receipt until
  // Core records the outcome. Other callers preserve the prior cleanup
  // behavior, and acknowledgement makes receipt removal idempotent.
  if acknowledge || !journal.deferred {
    remove_path(&journal_path)?;
    fsync_parent(&journal_path)?;
  }
  Ok(FinalizeVykarRestoreResponse {
    complete: true,
    rolled_back: !commit,
    containers_restarted: restarted,
    critical_error: None,
  })
}

impl Resolve<Args> for FinalizeVykarRestore {
  async fn resolve(
    self,
    _: &Args,
  ) -> anyhow::Result<FinalizeVykarRestoreResponse> {
    let _operation = backup_operation_lock().lock().await;
    let _filesystem = protected_filesystem_guard()?;
    finalize_restore_publication(
      &self.journal_id,
      self.commit,
      self.acknowledge,
    )
    .await
  }
}

impl Resolve<Args> for CancelVykarOperation {
  async fn resolve(
    self,
    _: &Args,
  ) -> anyhow::Result<CancelVykarOperationResponse> {
    Ok(CancelVykarOperationResponse {
      cancelled: request_operation_cancellation(&self.operation_id),
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn pending_journals_gate_new_work_until_recovery_removes_them() {
    let directory = tempfile::tempdir().unwrap();
    let directories = vec![directory.path().to_path_buf()];
    assert!(ensure_recovery_directories_empty(&directories).is_ok());
    for contents in
      ["indeterminate", r#"{"completed":true,"deferred":true}"#]
    {
      let journal = directory.path().join("earlier-operation.json");
      std::fs::write(&journal, contents).unwrap();
      // No in-memory latch or current operation ID: a new process sees the
      // same gate, and even an unreadable journal fails closed.
      assert!(
        ensure_recovery_directories_empty(&directories).is_err()
      );
      std::fs::remove_file(journal).unwrap();
      assert!(
        ensure_recovery_directories_empty(&directories).is_ok()
      );
    }
    assert!(
      ensure_recovery_directories_empty(&[directory
        .path()
        .join("missing")])
      .is_err()
    );
  }

  #[test]
  fn terminal_and_protected_filesystem_leases_exclude_each_other() {
    let barrier = Arc::new(tokio::sync::RwLock::new(()));
    let terminal = barrier.clone().try_read_owned().unwrap();
    assert!(barrier.clone().try_write_owned().is_err());
    drop(terminal);
    let backup = barrier.clone().try_write_owned().unwrap();
    assert!(barrier.clone().try_read_owned().is_err());
    drop(backup);
    assert!(barrier.try_read_owned().is_ok());
  }

  #[test]
  fn locked_preview_rejects_changed_paths_and_containers() {
    let preview = PreflightVykarRestoreResponse {
      destination_exists: true,
      created_paths: vec!["b".into(), "a".into()],
      containers_to_stop: vec!["application".into()],
      ..Default::default()
    };
    let mut reordered = preview.clone();
    reordered.created_paths.reverse();
    assert!(preview.matches(&reordered));
    reordered.deleted_paths.push("new-file".into());
    assert!(!preview.matches(&reordered));
    let mut changed = preview.clone();
    changed.containers_to_stop.clear();
    assert!(!preview.matches(&changed));
    changed = preview.clone();
    changed.destination_exists = false;
    assert!(!preview.matches(&changed));
  }

  #[test]
  fn cancellation_registration_shares_and_cleans_up_token() {
    let id = "cancellable-backup-test";
    let (worker, registration) = register_operation_cancellation(id);
    assert!(request_operation_cancellation(id));
    assert!(worker.load(Ordering::SeqCst));
    drop(registration);
    assert!(!operation_cancelled(id));
  }

  #[test]
  fn cancellation_before_registration_is_bounded_and_consumed() {
    let id = "early-cancellable-backup-test";
    assert!(request_operation_cancellation(id));
    {
      let registry = cancellation_registry().lock().unwrap();
      assert!(!registry.active.contains_key(id));
      assert!(registry.pending.contains_key(id));
      assert!(registry.pending.len() <= MAX_PENDING_CANCELLATIONS);
    }
    let (worker, registration) = register_operation_cancellation(id);
    assert!(worker.load(Ordering::SeqCst));
    assert!(
      !cancellation_registry()
        .lock()
        .unwrap()
        .pending
        .contains_key(id)
    );
    drop(registration);
  }

  #[test]
  fn backup_sources_cannot_capture_internal_backup_storage() {
    let root = tempfile::tempdir().unwrap();
    let internal = root.path().join(".komodo-vykar");
    let stack = root.path().join("stack");
    std::fs::create_dir_all(&stack).unwrap();

    assert!(
      validate_path_outside_internal_storage(
        root.path(),
        &internal,
        "Backup source",
      )
      .is_err()
    );
    validate_path_outside_internal_storage(
      &stack,
      &internal,
      "Backup source",
    )
    .unwrap();
  }

  #[test]
  fn backup_sources_cannot_capture_core_repository_mounts() {
    let root = tempfile::tempdir().unwrap();
    let repository = root.path().join("core-repository");
    let application = root.path().join("application");
    std::fs::create_dir_all(&repository).unwrap();
    std::fs::create_dir_all(&application).unwrap();

    assert!(
      validate_path_outside_protected_repositories(
        root.path(),
        std::slice::from_ref(&repository),
        "Backup source",
      )
      .is_err()
    );
    assert!(
      validate_path_outside_protected_repositories(
        &repository.join("packs"),
        std::slice::from_ref(&repository),
        "Backup source",
      )
      .is_err()
    );
    validate_path_outside_protected_repositories(
      &application,
      &[repository],
      "Backup source",
    )
    .unwrap();
  }

  #[test]
  fn restore_destinations_cannot_replace_core_repository_mounts() {
    let root = tempfile::tempdir().unwrap();
    let repository = root.path().join("core-repository");
    let alias = root.path().join("repository-alias");
    std::fs::create_dir_all(&repository).unwrap();
    std::os::unix::fs::symlink(&repository, &alias).unwrap();

    assert!(
      validate_path_outside_protected_repositories(
        &alias.join("packs"),
        std::slice::from_ref(&repository),
        "Restore destination",
      )
      .is_err()
    );
  }

  #[test]
  fn protected_repository_mapping_preserves_the_mount_subpath() {
    assert_eq!(
      map_path_through_mount(
        Path::new("/data/backups/vykar"),
        Path::new("/data"),
        Path::new("/var/lib/docker/volumes/komodo_data/_data"),
      ),
      Some(PathBuf::from(
        "/var/lib/docker/volumes/komodo_data/_data/backups/vykar"
      ))
    );
    assert_eq!(
      map_path_through_mount(
        Path::new("/data/backups"),
        Path::new("/data/backups/vykar"),
        Path::new("/repository-volume"),
      ),
      Some(PathBuf::from("/repository-volume"))
    );
    let mapped = map_path_through_mount(
      Path::new("/data/backups/vykar"),
      Path::new("/data"),
      Path::new("/host/data"),
    )
    .unwrap();
    assert!(!paths_overlap(Path::new("/host/data/stacks"), &mapped));
  }

  #[test]
  fn private_core_mounts_and_worker_aliases_are_safety_exclusions() {
    let root = tempfile::tempdir().unwrap();
    let private = root.path().join("core-secrets");
    let alias = root.path().join("worker-alias");
    std::fs::create_dir_all(&private).unwrap();
    std::os::unix::fs::symlink(&private, &alias).unwrap();
    let mapped = map_path_through_mount(
      Path::new("/core-secrets"),
      Path::new("/core-secrets"),
      &private,
    )
    .unwrap();
    for path in [
      private.as_path(),
      alias.as_path(),
      &alias.join("backup.key"),
      root.path(),
    ] {
      for operation in ["Backup source", "Restore destination"] {
        let error = validate_path_outside_protected_repositories(
          path,
          std::slice::from_ref(&mapped),
          operation,
        )
        .unwrap_err();
        assert!(error.is::<ExcludedBackupSource>());
      }
    }
    validate_path_outside_protected_repositories(
      &root.path().join("application"),
      &[mapped],
      "Backup source",
    )
    .unwrap();
  }

  #[test]
  fn core_and_skipped_database_targets_are_refused_even_when_stopped()
  {
    let core_id = "a".repeat(64);
    let protected = vec![ProtectedRepositoryPath {
      path: "/core-secrets".into(),
      core_container_id: core_id.clone(),
    }];
    let stack = komodo_client::entities::stack::Stack {
      name: "control-plane".into(),
      ..Default::default()
    };
    let core = ContainerListItem {
      id: Some(core_id),
      name: "custom-core-name".into(),
      state: ContainerStateStatusEnum::Exited,
      volumes: vec!["private-secrets".into()],
      labels: HashMap::from([(
        COMPOSE_PROJECT_LABEL.into(),
        stack.project_name(false),
      )]),
      ..Default::default()
    };
    let database = ContainerListItem {
      name: "database".into(),
      state: ContainerStateStatusEnum::Exited,
      volumes: vec!["database-data".into()],
      labels: HashMap::from([("komodo.skip".into(), "true".into())]),
      ..Default::default()
    };
    let containers = vec![core, database];
    for volume_name in ["private-secrets", "database-data"] {
      let error = ensure_target_not_control_plane(
        &containers,
        &PeripheryBackupTarget::Volume {
          volume_name: volume_name.into(),
        },
        &protected,
      )
      .unwrap_err();
      assert!(error.is::<ExcludedBackupSource>());
    }
    assert!(
      ensure_target_not_control_plane(
        &containers,
        &PeripheryBackupTarget::Stack {
          stack: Box::new(stack),
          repo: None
        },
        &protected
      )
      .is_err()
    );
    ensure_target_not_control_plane(
      &containers,
      &PeripheryBackupTarget::Volume {
        volume_name: "application-data".into(),
      },
      &protected,
    )
    .unwrap();
  }

  #[test]
  fn quiescing_keeps_core_and_skip_label_containers_running() {
    let protected = vec![ProtectedRepositoryPath {
      path: "/core-secrets".into(),
      core_container_id: "a".repeat(64),
    }];
    let core = ContainerListItem {
      id: Some("a".repeat(64)),
      name: "core".into(),
      state: ContainerStateStatusEnum::Running,
      volumes: vec!["shared".into()],
      ..Default::default()
    };
    let database = ContainerListItem {
      id: Some("b".repeat(64)),
      name: "database".into(),
      labels: HashMap::from([("komodo.skip".into(), "true".into())]),
      ..core.clone()
    };
    let application = ContainerListItem {
      id: Some("c".repeat(64)),
      name: "application".into(),
      labels: HashMap::from([("komodo.skip".into(), "false".into())]),
      ..core.clone()
    };
    assert_eq!(
      running_containers_for_target(
        &[core, database, application],
        &PeripheryBackupTarget::Volume {
          volume_name: "shared".into()
        },
        Some(&"d".repeat(64)),
        &protected
      ),
      vec!["application"]
    );
  }

  #[test]
  fn repository_aliases_require_the_actual_core_container_identity() {
    let core = ContainerListItem {
      id: Some("a".repeat(64)),
      ..Default::default()
    };
    let mut unrelated = core.clone();
    unrelated.id = Some("b".repeat(64));
    unrelated.name = "komodo".into();
    assert!(container_matches_id(&core, &"a".repeat(64)));
    assert!(container_matches_id(&core, &"a".repeat(12)));
    assert!(!container_matches_id(&unrelated, &"a".repeat(64)));
    assert!(!container_matches_id(&unrelated, "komodo"));
    assert!(!container_matches_id(&core, ""));
    // Translate host protection into Periphery's namespace, not /data in
    // every application container on that Docker daemon.
    assert_eq!(
      map_path_through_mount(
        Path::new("/host/core/backups/vykar"),
        Path::new("/host/core"),
        Path::new("/periphery/data"),
      ),
      Some(PathBuf::from("/periphery/data/backups/vykar"))
    );
    assert_eq!(
      map_path_through_mount(
        Path::new("/host/core/backups/vykar"),
        Path::new("/srv/app"),
        Path::new("/data"),
      ),
      None
    );
  }

  #[test]
  fn bind_root_filters_use_vykar_path_rules() {
    let root = tempfile::tempdir().unwrap();
    let run = root.path().join("run");
    let included = root.path().join("binds/application");
    let excluded = root.path().join("binds/cache");
    let outside = root.path().join("other/data");
    for path in [&run, &included, &excluded, &outside] {
      std::fs::create_dir_all(path).unwrap();
    }
    let root_pattern =
      root.path().to_string_lossy().replace('\\', "/");
    let selected = select_bind_backup_roots(
      [included.clone(), excluded, outside].into_iter().collect(),
      &run,
      &BackupSourceFilters {
        bind_mount_include_patterns: vec![format!(
          "{root_pattern}/binds/**"
        )],
        bind_mount_exclude_patterns: vec!["**/cache".into()],
        ..Default::default()
      },
    )
    .unwrap();
    assert_eq!(selected, [included].into_iter().collect());
  }

  fn container(
    name: &str,
    state: ContainerStateStatusEnum,
    project: Option<&str>,
    volumes: &[&str],
  ) -> ContainerListItem {
    let mut labels = std::collections::HashMap::new();
    if let Some(project) = project {
      labels.insert(COMPOSE_PROJECT_LABEL.into(), project.into());
    }
    ContainerListItem {
      name: name.into(),
      state,
      volumes: volumes
        .iter()
        .map(|volume| (*volume).into())
        .collect(),
      labels,
      ..Default::default()
    }
  }

  #[test]
  fn stack_restore_stops_the_whole_running_deployed_project() {
    let stack = komodo_client::entities::stack::Stack {
      name: "configured-stack-name".into(),
      config: komodo_client::entities::stack::StackConfig {
        project_name: "new-project-name".into(),
        ..Default::default()
      },
      info: komodo_client::entities::stack::StackInfo {
        deployed_project_name: Some("deployed-project-name".into()),
        ..Default::default()
      },
      ..Default::default()
    };
    let target = PeripheryBackupTarget::Stack {
      stack: Box::new(stack),
      repo: None,
    };
    let containers = vec![
      container(
        "web",
        ContainerStateStatusEnum::Running,
        Some("deployed-project-name"),
        &[],
      ),
      container(
        "worker",
        ContainerStateStatusEnum::Running,
        Some("deployed-project-name"),
        &[],
      ),
      container(
        "already-stopped",
        ContainerStateStatusEnum::Exited,
        Some("deployed-project-name"),
        &[],
      ),
      container(
        "unrelated",
        ContainerStateStatusEnum::Running,
        Some("other-project"),
        &[],
      ),
    ];

    assert_eq!(
      running_containers_for_target(&containers, &target, None, &[]),
      ["web", "worker"]
    );
  }

  #[test]
  fn volume_restore_stops_every_running_container_with_access() {
    let target = PeripheryBackupTarget::Volume {
      volume_name: "shared-data".into(),
    };
    let containers = vec![
      container(
        "stack-a-web",
        ContainerStateStatusEnum::Running,
        Some("stack-a"),
        &["shared-data"],
      ),
      container(
        "stack-b-worker",
        ContainerStateStatusEnum::Running,
        Some("stack-b"),
        &["shared-data", "other-data"],
      ),
      container(
        "already-stopped",
        ContainerStateStatusEnum::Exited,
        Some("stack-c"),
        &["shared-data"],
      ),
      container(
        "unrelated",
        ContainerStateStatusEnum::Running,
        None,
        &["other-data"],
      ),
    ];

    assert_eq!(
      running_containers_for_target(&containers, &target, None, &[]),
      ["stack-a-web", "stack-b-worker"]
    );
  }

  #[test]
  fn source_validation_rejects_relative_paths() {
    assert!(
      validate_source_path(Path::new("relative/path")).is_err()
    );
  }

  #[test]
  fn restore_destinations_resolve_symlinked_existing_ancestors() {
    let root = tempfile::tempdir().unwrap();
    let real = root.path().join("real");
    let alias = root.path().join("alias");
    std::fs::create_dir(&real).unwrap();
    std::os::unix::fs::symlink(&real, &alias).unwrap();
    assert_eq!(
      resolve_existing_ancestor(&alias.join("new/child")).unwrap(),
      real.canonicalize().unwrap().join("new/child")
    );
  }

  #[test]
  fn restore_destinations_reject_overlap_through_symlinks() {
    let root = tempfile::tempdir().unwrap();
    let real = root.path().join("real");
    let alias = root.path().join("alias");
    std::fs::create_dir(&real).unwrap();
    std::os::unix::fs::symlink(&real, &alias).unwrap();
    let publish = vec![
      RestorePublishPath {
        destination_root: None,
        snapshot_path: "source/one".into(),
        destination: real.join("app").to_string_lossy().into_owned(),
      },
      RestorePublishPath {
        destination_root: None,
        snapshot_path: "source/two".into(),
        destination: alias
          .join("app/data")
          .to_string_lossy()
          .into_owned(),
      },
    ];
    assert!(
      validate_resolved_restore_destinations(&publish).is_err()
    );
  }

  #[test]
  fn restore_destinations_cannot_replace_internal_backup_storage() {
    let root = tempfile::tempdir().unwrap();
    let internal = root.path().join(".komodo-vykar");
    let publish = vec![RestorePublishPath {
      destination_root: None,
      snapshot_path: "source/root".into(),
      destination: root.path().to_string_lossy().into_owned(),
    }];
    assert!(
      validate_resolved_restore_destinations_against(
        &publish, &internal,
      )
      .is_err()
    );
  }

  #[test]
  fn selected_restore_rejects_symlink_ancestors_but_can_replace_the_leaf()
   {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let link = root.path().join("link");
    std::os::unix::fs::symlink(outside.path(), &link).unwrap();
    let mut item = RestorePublishPath {
      destination_root: Some(
        root.path().to_string_lossy().into_owned(),
      ),
      snapshot_path: "source/link/config".into(),
      destination: link.join("config").to_string_lossy().into_owned(),
    };
    assert!(validate_restore_destination_ancestors(&item).is_err());
    item.destination = link.to_string_lossy().into_owned();
    validate_restore_destination_ancestors(&item).unwrap();
    item.destination_root = Some(link.to_string_lossy().into_owned());
    item.destination =
      link.join("config").to_string_lossy().into_owned();
    assert!(validate_restore_destination_ancestors(&item).is_err());
    item.destination_root =
      Some(root.path().to_string_lossy().into_owned());
    item.destination = root
      .path()
      .join("missing/child")
      .to_string_lossy()
      .into_owned();
    validate_restore_destination_ancestors(&item).unwrap();
    item.destination =
      outside.path().join("config").to_string_lossy().into_owned();
    assert!(validate_restore_destination_ancestors(&item).is_err());
  }

  #[test]
  fn full_restore_rejects_linked_parents_but_can_replace_the_leaf() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let link = root.path().join("link");
    std::os::unix::fs::symlink(outside.path(), &link).unwrap();
    let mut item = RestorePublishPath {
      destination_root: None,
      snapshot_path: "source/config".into(),
      destination: link.join("config").to_string_lossy().into_owned(),
    };
    assert!(validate_restore_destination_ancestors(&item).is_err());
    item.destination = link.to_string_lossy().into_owned();
    validate_restore_destination_ancestors(&item).unwrap();
    item.destination = root
      .path()
      .join("missing/child")
      .to_string_lossy()
      .into_owned();
    validate_restore_destination_ancestors(&item).unwrap();
    let file = root.path().join("file");
    std::fs::write(&file, "not a directory").unwrap();
    item.destination =
      file.join("child").to_string_lossy().into_owned();
    assert!(validate_restore_destination_ancestors(&item).is_err());
    item.destination = "relative/path".into();
    assert!(validate_restore_destination_ancestors(&item).is_err());
  }

  #[test]
  fn selected_restore_also_checks_ancestors_above_its_root() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let link = root.path().join("link");
    std::fs::create_dir(outside.path().join("nested")).unwrap();
    std::os::unix::fs::symlink(outside.path(), &link).unwrap();
    let item = RestorePublishPath {
      destination_root: Some(
        link.join("nested").to_string_lossy().into_owned(),
      ),
      snapshot_path: "source/config".into(),
      destination: link
        .join("nested/config")
        .to_string_lossy()
        .into_owned(),
    };
    assert!(validate_restore_destination_ancestors(&item).is_err());
  }

  #[test]
  fn selected_volume_destinations_use_the_inspected_mountpoint() {
    let mut publish = vec![RestorePublishPath {
      destination_root: None,
      snapshot_path: "source/_data/config/app.toml".into(),
      destination:
        "/var/lib/docker/volumes/app-data/_data/config/app.toml"
          .into(),
    }];
    resolve_volume_publish_destinations(
      &mut publish,
      "app-data",
      "/custom/docker/volumes/app-data/data",
      false,
    )
    .unwrap();
    assert_eq!(
      publish[0].destination,
      "/custom/docker/volumes/app-data/data/config/app.toml"
    );
    assert_eq!(
      publish[0].destination_root.as_deref(),
      Some("/custom/docker/volumes/app-data/data")
    );
  }

  #[test]
  fn deployed_compose_config_discovers_bind_roots_without_containers()
  {
    let run_directory = tempfile::tempdir().unwrap();
    let bind_directory = tempfile::tempdir().unwrap();
    let mut stack = komodo_client::entities::stack::Stack::default();
    stack.info.deployed_config = Some(format!(
      "services:\n  app:\n    volumes:\n      - type: bind\n        source: '{}'\n        target: /data\n",
      bind_directory.path().display()
    ));
    let paths =
      compose_bind_paths(&stack, run_directory.path()).unwrap();
    assert_eq!(
      paths,
      [bind_directory.path().canonicalize().unwrap()]
        .into_iter()
        .collect()
    );
  }

  #[test]
  fn recovered_compose_rewrites_long_and_short_absolute_binds() {
    let mut document: serde_yaml_ng::Value = serde_yaml_ng::from_str(
      "services:\n  app:\n    volumes:\n      - type: bind\n        source: /srv/old/data\n        target: /data\n      - /srv/old/cache:/cache:ro\n      - named-data:/named\n",
    )
    .unwrap();
    let mappings = HashMap::from([(
      "/srv/old".to_string(),
      "/srv/recovered".to_string(),
    )]);
    assert_eq!(
      rewrite_compose_bind_mappings(
        &mut document,
        &mappings,
        &HashMap::new(),
      ),
      2
    );
    let rewritten = serde_yaml_ng::to_string(&document).unwrap();
    assert!(rewritten.contains("/srv/recovered/data"));
    assert!(rewritten.contains("/srv/recovered/cache:/cache:ro"));
    assert!(rewritten.contains("named-data:/named"));
    assert!(!rewritten.contains("/srv/old"));
  }

  #[test]
  fn recovered_compose_rewrites_a_recorded_symlink_alias() {
    let mut document: serde_yaml_ng::Value = serde_yaml_ng::from_str(
      "services:\n  app:\n    volumes:\n      - /srv/link/cache:/cache:ro\n",
    )
    .unwrap();
    let mappings = HashMap::from([(
      "/srv/real".to_string(),
      "/srv/recovered".to_string(),
    )]);
    let aliases = HashMap::from([(
      "/srv/link/cache".to_string(),
      "/srv/real/cache".to_string(),
    )]);
    assert_eq!(
      rewrite_compose_bind_mappings(
        &mut document,
        &mappings,
        &aliases,
      ),
      1
    );
    let rewritten = serde_yaml_ng::to_string(&document).unwrap();
    assert!(rewritten.contains("/srv/recovered/cache:/cache:ro"));
    assert!(!rewritten.contains("/srv/link"));
  }

  #[test]
  fn recovered_compose_resolves_bind_expressions_from_snapshot_metadata()
   {
    let mut document: serde_yaml_ng::Value = serde_yaml_ng::from_str(
      "services:\n  app:\n    volumes:\n      - ${DATA_ROOT:-/fallback}/cache:/cache:ro\n      - type: bind\n        source: ${DATA_ROOT}/data\n        target: /data\n      - ${VOLUME_NAME}:/named\n",
    ).unwrap();
    let deployed = "services:\n  app:\n    volumes:\n      - type: bind\n        source: /srv/link/cache\n        target: /cache\n      - type: bind\n        source: /srv/old/data\n        target: /data\n      - type: volume\n        source: named-data\n        target: /named\n";
    let mappings =
      HashMap::from([("/srv/old".into(), "/srv/recovered".into())]);
    let aliases = HashMap::from([(
      "/srv/link/cache".into(),
      "/srv/old/cache".into(),
    )]);
    resolve_recovered_bind_expressions(
      &mut document,
      Some(deployed),
      &mappings,
      &aliases,
    )
    .unwrap();
    assert_eq!(
      rewrite_compose_bind_mappings(
        &mut document,
        &mappings,
        &aliases
      ),
      2
    );
    let rewritten = serde_yaml_ng::to_string(&document).unwrap();
    assert!(rewritten.contains("/srv/recovered/cache:/cache:ro"));
    assert!(rewritten.contains("/srv/recovered/data"));
    assert!(rewritten.contains("${VOLUME_NAME}:/named"));
    assert!(!rewritten.contains("${DATA_ROOT"));
  }

  #[test]
  fn recovered_bind_expressions_fail_closed_without_deployment_metadata()
   {
    let mut document: serde_yaml_ng::Value = serde_yaml_ng::from_str(
      "services:\n  app:\n    volumes:\n      - ${DATA_ROOT}/cache:/cache\n",
    ).unwrap();
    assert!(
      resolve_recovered_bind_expressions(
        &mut document,
        None,
        &HashMap::new(),
        &HashMap::new()
      )
      .is_err()
    );
  }

  #[test]
  fn nested_stack_bind_roots_collapse_to_the_ancestor() {
    let run_directory = tempfile::tempdir().unwrap();
    let binds = tempfile::tempdir().unwrap();
    let child = binds.path().join("cache");
    std::fs::create_dir(&child).unwrap();
    let mut paths = BTreeSet::new();
    insert_bind_backup_root(&mut paths, run_directory.path(), &child)
      .unwrap();
    insert_bind_backup_root(
      &mut paths,
      run_directory.path(),
      binds.path(),
    )
    .unwrap();
    assert_eq!(
      paths,
      [binds.path().canonicalize().unwrap()].into_iter().collect()
    );
    insert_bind_backup_root(&mut paths, run_directory.path(), &child)
      .unwrap();
    assert_eq!(paths.len(), 1);
  }

  #[test]
  fn recovered_compose_rewrite_rejects_leaf_and_parent_symlinks() {
    let staging = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let original = outside.path().join("compose.yaml");
    std::fs::write(&original, b"unrelated host file").unwrap();
    let leaf = staging.path().join("compose.yaml");
    std::os::unix::fs::symlink(&original, &leaf).unwrap();
    assert!(open_staged_compose_file(staging.path(), &leaf).is_err());
    let parent = staging.path().join("linked-directory");
    std::os::unix::fs::symlink(outside.path(), &parent).unwrap();
    assert!(
      open_staged_compose_file(
        staging.path(),
        &parent.join("compose.yaml")
      )
      .is_err()
    );
    assert_eq!(
      std::fs::read(&original).unwrap(),
      b"unrelated host file"
    );
    let regular = staging.path().join("regular.yaml");
    std::fs::write(&regular, b"services: {}").unwrap();
    assert!(
      open_staged_compose_file(staging.path(), &regular).is_ok()
    );
  }

  #[test]
  fn preview_rows_and_bytes_are_bounded_across_all_change_lists() {
    let mut budget = RestorePreviewBudget::new(
      Instant::now() + RESTORE_PREFLIGHT_TIMEOUT,
    );
    budget.rows = MAX_RESTORE_PREVIEW_ROWS - 1;
    let mut created = Vec::new();
    let mut deleted = Vec::new();
    budget.push(&mut created, Path::new("a")).unwrap();
    assert!(budget.push(&mut deleted, Path::new("b")).is_err());
    assert!(deleted.is_empty());
    budget.rows = 0;
    budget.path_bytes = MAX_RESTORE_PREVIEW_BYTES - 1;
    budget.push(&mut created, Path::new("c")).unwrap();
    assert!(budget.push(&mut deleted, Path::new("d")).is_err());
  }

  #[test]
  fn expired_preflight_fails_without_returning_a_partial_inventory() {
    let destination = tempfile::tempdir().unwrap();
    let publish = [RestorePublishPath {
      destination_root: None,
      snapshot_path: "root".into(),
      destination: destination.path().to_string_lossy().into_owned(),
    }];
    assert!(
      compare_restore_paths(&[], &publish, &[], Instant::now())
        .is_err()
    );
  }

  #[tokio::test]
  async fn preflight_deadline_covers_pending_discovery_and_releases_guards()
   {
    let filesystem = Arc::new(tokio::sync::RwLock::new(()));
    let guard = filesystem.clone().write_owned().await;
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let permit = slots.clone().try_acquire_owned().unwrap();
    let result =
      restore_preflight_before_deadline(Instant::now(), async move {
        let _guard = guard;
        let _permit = permit;
        // Model a Docker discovery future that never responds, before the
        // inventory worker exists.
        std::future::pending::<anyhow::Result<()>>().await
      })
      .await;
    assert!(
      result
        .unwrap_err()
        .to_string()
        .contains("exceeded 60 seconds")
    );
    assert!(filesystem.try_read().is_ok());
    assert_eq!(slots.available_permits(), 1);
  }

  #[tokio::test]
  async fn preflight_timeout_keeps_admission_until_inventory_worker_exits()
   {
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let permit = slots.clone().try_acquire_owned().unwrap();
    let (finish, finished) = tokio::sync::oneshot::channel::<()>();
    let mut worker = tokio::spawn(async move {
      let _permit = permit;
      finished.await.unwrap();
    });
    let result =
      restore_preflight_before_deadline(Instant::now(), async {
        (&mut worker).await?;
        Ok(())
      })
      .await;
    assert!(result.is_err());
    assert_eq!(slots.available_permits(), 0);
    finish.send(()).unwrap();
    worker.await.unwrap();
    assert_eq!(slots.available_permits(), 1);
  }

  #[test]
  fn failed_staging_cleanup_requires_recovery_even_after_cancellation()
   {
    for error in [Some(anyhow!("snapshot restore failed")), None] {
      let result = finish_restore_before_publication(
        error,
        Err(anyhow!("cannot remove staging journal")),
      );
      let RestoreTransactionResult::StagingCleanupFailed(error) =
        result
      else {
        panic!("A surviving staging journal must require recovery");
      };
      let message = format!("{error:#}");
      assert!(message.contains("restore staging cleanup failed"));
      assert!(message.contains("cannot remove staging journal"));
      assert!(
        message.contains("snapshot restore failed")
          || message.contains("cancelled")
      );
    }
  }

  #[test]
  fn successful_staging_cleanup_preserves_failure_or_proves_cancellation()
   {
    assert!(matches!(
      finish_restore_before_publication(
        Some(anyhow!("restore failed")),
        Ok(())
      ),
      RestoreTransactionResult::FailedBeforePublication(_)
    ));
    assert!(matches!(
      finish_restore_before_publication(None, Ok(())),
      RestoreTransactionResult::Published {
        rolled_back: true,
        finalization_pending: false
      }
    ));
  }

  #[test]
  fn destination_inventory_stops_when_preview_is_full() {
    let destination = tempfile::tempdir().unwrap();
    std::fs::write(destination.path().join("extra"), b"extra")
      .unwrap();
    let mut budget = RestorePreviewBudget::new(
      Instant::now() + RESTORE_PREFLIGHT_TIMEOUT,
    );
    budget.rows = MAX_RESTORE_PREVIEW_ROWS;
    let mut deleted = Vec::new();
    assert!(
      collect_unexpected_paths(
        destination.path(),
        &HashSet::new(),
        &mut deleted,
        &mut budget
      )
      .is_err()
    );
    assert!(deleted.is_empty());
    assert_eq!(
      std::fs::read(destination.path().join("extra")).unwrap(),
      b"extra"
    );
  }

  #[test]
  fn exact_restore_preflight_reports_create_overwrite_and_delete() {
    let destination = tempfile::tempdir().unwrap();
    std::fs::write(destination.path().join("old.txt"), b"old")
      .unwrap();
    std::fs::write(destination.path().join("extra.txt"), b"extra")
      .unwrap();
    let root = "source/root";
    let paths = vec![
      komodo_backup::SnapshotPath {
        path: root.into(),
        directory: true,
      },
      komodo_backup::SnapshotPath {
        path: format!("{root}/old.txt"),
        directory: false,
      },
      komodo_backup::SnapshotPath {
        path: format!("{root}/new.txt"),
        directory: false,
      },
    ];
    let publish = vec![RestorePublishPath {
      destination_root: None,
      snapshot_path: root.into(),
      destination: destination.path().to_string_lossy().into_owned(),
    }];
    let (created, overwritten, deleted) = compare_restore_paths(
      &paths,
      &publish,
      &[],
      Instant::now() + RESTORE_PREFLIGHT_TIMEOUT,
    )
    .unwrap();
    assert!(created.iter().any(|path| path.ends_with("new.txt")));
    assert!(overwritten.iter().any(|path| path.ends_with("old.txt")));
    assert!(deleted.iter().any(|path| path.ends_with("extra.txt")));
  }

  #[cfg(unix)]
  #[test]
  fn restore_preflight_never_follows_destination_symlinks() {
    let destination = tempfile::tempdir().unwrap();
    let unrelated = tempfile::tempdir().unwrap();
    std::fs::write(unrelated.path().join("private.txt"), b"private")
      .unwrap();
    std::fs::write(unrelated.path().join("restored.txt"), b"old")
      .unwrap();
    for nested in [false, true] {
      let publish_root = destination.path().join(if nested {
        "directory"
      } else {
        "root-link"
      });
      let link = if nested {
        std::fs::create_dir(&publish_root).unwrap();
        publish_root.join("nested-link")
      } else {
        publish_root.clone()
      };
      std::os::unix::fs::symlink(unrelated.path(), &link).unwrap();
      let snapshot_root = if nested {
        "source/root/nested-link"
      } else {
        "source/root"
      };
      let mut paths = vec![
        komodo_backup::SnapshotPath {
          path: snapshot_root.into(),
          directory: true,
        },
        komodo_backup::SnapshotPath {
          path: format!("{snapshot_root}/restored.txt"),
          directory: false,
        },
      ];
      if nested {
        paths.push(komodo_backup::SnapshotPath {
          path: "source/root".into(),
          directory: true,
        });
      }
      let publish = vec![RestorePublishPath {
        destination_root: None,
        snapshot_path: "source/root".into(),
        destination: publish_root.to_string_lossy().into_owned(),
      }];
      let (created, overwritten, deleted) = compare_restore_paths(
        &paths,
        &publish,
        &[],
        Instant::now() + RESTORE_PREFLIGHT_TIMEOUT,
      )
      .unwrap();
      assert_eq!(
        created,
        vec![
          link.join("restored.txt").to_string_lossy().into_owned()
        ]
      );
      assert_eq!(
        overwritten,
        vec![link.to_string_lossy().into_owned()]
      );
      assert!(deleted.is_empty());
    }
  }

  #[test]
  fn publish_failure_restores_original_destination() {
    let root = tempfile::tempdir().unwrap();
    let download = root.path().join("download");
    std::fs::create_dir_all(download.join("one")).unwrap();
    std::fs::write(download.join("one/new.txt"), b"new").unwrap();
    std::fs::write(download.join("two.txt"), b"two").unwrap();
    let first = root.path().join("destination");
    std::fs::create_dir_all(&first).unwrap();
    std::fs::write(first.join("original.txt"), b"original").unwrap();
    // The first publication creates this previously absent sibling as its
    // rollback copy. The second entry must then fail its existence recheck,
    // after the first destination has actually been replaced.
    let second =
      restore_rollback_path(&first, "rollback-test").unwrap();
    let publish = vec![
      RestorePublishPath {
        destination_root: None,
        snapshot_path: "one".into(),
        destination: first.to_string_lossy().into_owned(),
      },
      RestorePublishPath {
        destination_root: None,
        snapshot_path: "two.txt".into(),
        destination: second.to_string_lossy().into_owned(),
      },
    ];
    let publication_started = AtomicBool::new(false);
    let journals = root.path().join("journals");
    assert!(
      publish_restore_in(
        &download,
        &publish,
        "rollback-test",
        &publication_started,
        &journals,
        None,
        false,
      )
      .unwrap()
    );
    assert!(publication_started.load(Ordering::SeqCst));
    assert_eq!(
      std::fs::read(first.join("original.txt")).unwrap(),
      b"original"
    );
    assert!(!first.join("new.txt").exists());
    assert!(!second.exists());
    assert!(!download.exists());
    assert!(!journals.join("rollback-test.json").exists());
    for index in 0..publish.len() {
      assert!(
        !root
          .path()
          .join(format!(".komodo-restore-rollback-test-{index}"))
          .exists()
      );
    }
  }

  #[test]
  fn overlapping_destinations_fail_before_publication() {
    let root = tempfile::tempdir().unwrap();
    let download = root.path().join("download");
    std::fs::create_dir_all(download.join("one")).unwrap();
    std::fs::write(download.join("one/new.txt"), b"new").unwrap();
    std::fs::write(download.join("two.txt"), b"two").unwrap();
    let first = root.path().join("destination");
    std::fs::create_dir(&first).unwrap();
    std::fs::write(first.join("original.txt"), b"original").unwrap();
    let publish = [
      RestorePublishPath {
        destination_root: None,
        snapshot_path: "one".into(),
        destination: first.to_string_lossy().into_owned(),
      },
      RestorePublishPath {
        destination_root: None,
        snapshot_path: "two.txt".into(),
        destination: first
          .join("child.txt")
          .to_string_lossy()
          .into_owned(),
      },
    ];
    let publication_started = AtomicBool::new(false);
    let journals = root.path().join("journals");
    let error = publish_restore_in(
      &download,
      &publish,
      "overlap-test",
      &publication_started,
      &journals,
      None,
      false,
    )
    .unwrap_err();
    assert!(
      error.to_string().contains("Restore destinations overlap")
    );
    assert!(!publication_started.load(Ordering::SeqCst));
    assert_eq!(
      std::fs::read(first.join("original.txt")).unwrap(),
      b"original"
    );
    assert!(!first.join("new.txt").exists());
    assert!(!first.join("child.txt").exists());
    assert!(download.join("one/new.txt").exists());
    assert!(!journals.exists());
    assert!(root.path().read_dir().unwrap().all(|entry| {
      !entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with(".komodo-restore-")
    }));
  }

  #[test]
  fn deferred_publication_retains_rollback_until_finalized() {
    let root = tempfile::tempdir().unwrap();
    let download = root.path().join("download");
    std::fs::create_dir(&download).unwrap();
    std::fs::write(download.join("new.txt"), b"new").unwrap();
    let destination = root.path().join("destination.txt");
    std::fs::write(&destination, b"original").unwrap();
    let publish = [RestorePublishPath {
      destination_root: None,
      snapshot_path: "new.txt".into(),
      destination: destination.to_string_lossy().into_owned(),
    }];
    let journal_directory = root.path().join("journals");
    assert!(
      !publish_restore_in(
        &download,
        &publish,
        "deferred-test",
        &AtomicBool::new(false),
        &journal_directory,
        None,
        true,
      )
      .unwrap()
    );
    assert_eq!(std::fs::read(&destination).unwrap(), b"new");
    let journal_path = journal_directory.join("deferred-test.json");
    let mut journal: RestoreJournal =
      serde_json::from_slice(&std::fs::read(&journal_path).unwrap())
        .unwrap();
    assert!(!journal.committed);
    assert!(journal.deferred);
    assert!(!journal.completed);
    assert!(journal.entries[0].rollback.exists());
    rollback_published(&mut journal, &journal_path).unwrap();
    assert_eq!(std::fs::read(&destination).unwrap(), b"original");
  }

  #[test]
  fn repeated_recovery_preserves_an_already_restored_original() {
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("destination");
    std::fs::write(&destination, b"original").unwrap();
    let journal_path = root.path().join("journal.json");
    let mut journal = RestoreJournal {
      staging: root.path().join("staging"),
      entries: vec![RestoreJournalEntry {
        source: root.path().join("source"),
        destination: destination.clone(),
        rollback: root.path().join("rollback"),
        original_existed: Some(true),
        // Simulate a crash after rollback -> destination was synced but before
        // this flag was durably cleared.
        published: true,
      }],
      committed: false,
      finalized: false,
      deferred: false,
      completed: false,
      owned_volume: None,
    };
    rollback_published(&mut journal, &journal_path).unwrap();
    assert_eq!(std::fs::read(destination).unwrap(), b"original");
    assert!(!journal.entries[0].published);
  }

  #[test]
  fn rolled_back_restore_retains_created_volume_ownership() {
    let root = tempfile::tempdir().unwrap();
    let journal_path = root.path().join("journal.json");
    let journal = RestoreJournal {
      staging: root.path().join("staging"),
      entries: Vec::new(),
      committed: false,
      finalized: false,
      deferred: false,
      completed: false,
      owned_volume: Some(RestoreOwnedVolume {
        volume_name: "recovered-data".into(),
        restore_plan_id: "plan-id".into(),
      }),
    };
    std::fs::create_dir(&journal.staging).unwrap();

    cleanup_rolled_back_restore(&journal, &journal_path).unwrap();

    let retained: RestoreJournal =
      serde_json::from_slice(&std::fs::read(&journal_path).unwrap())
        .unwrap();
    assert!(retained.staging.as_os_str().is_empty());
    assert!(retained.entries.is_empty());
    assert_eq!(
      retained
        .owned_volume
        .as_ref()
        .map(|owned| owned.volume_name.as_str()),
      Some("recovered-data")
    );
  }

  #[test]
  fn rollback_names_preserve_complete_destination_filenames() {
    let root = tempfile::tempdir().unwrap();
    let download = root.path().join("download");
    std::fs::create_dir_all(&download).unwrap();
    std::fs::write(download.join("new-json"), b"new-json").unwrap();
    std::fs::write(download.join("new-yaml"), b"new-yaml").unwrap();
    let json = root.path().join("app.json");
    let yaml = root.path().join("app.yaml");
    assert_ne!(
      restore_rollback_path(&json, "unique-rollback-test").unwrap(),
      restore_rollback_path(&yaml, "unique-rollback-test").unwrap()
    );
    std::fs::write(&json, b"old-json").unwrap();
    std::fs::write(&yaml, b"old-yaml").unwrap();
    let publish = vec![
      RestorePublishPath {
        destination_root: None,
        snapshot_path: "new-json".into(),
        destination: json.to_string_lossy().into_owned(),
      },
      RestorePublishPath {
        destination_root: None,
        snapshot_path: "new-yaml".into(),
        destination: yaml.to_string_lossy().into_owned(),
      },
    ];
    assert!(
      !publish_restore_in(
        &download,
        &publish,
        "unique-rollback-test",
        &AtomicBool::new(false),
        &root.path().join("journals"),
        None,
        false,
      )
      .unwrap()
    );
    assert_eq!(std::fs::read(json).unwrap(), b"new-json");
    assert_eq!(std::fs::read(yaml).unwrap(), b"new-yaml");
  }

  #[test]
  fn preparation_errors_remove_same_filesystem_staging() {
    let root = tempfile::tempdir().unwrap();
    let download = root.path().join("download");
    std::fs::create_dir_all(&download).unwrap();
    std::fs::write(download.join("present"), b"present").unwrap();
    let publish = vec![
      RestorePublishPath {
        destination_root: None,
        snapshot_path: "present".into(),
        destination: root
          .path()
          .join("first")
          .to_string_lossy()
          .into_owned(),
      },
      RestorePublishPath {
        destination_root: None,
        snapshot_path: "missing".into(),
        destination: root
          .path()
          .join("second")
          .to_string_lossy()
          .into_owned(),
      },
    ];
    assert!(
      publish_restore_in(
        &download,
        &publish,
        "prepare-cleanup-test",
        &AtomicBool::new(false),
        &root.path().join("journals"),
        None,
        false,
      )
      .is_err()
    );
    assert!(
      !root
        .path()
        .join(".komodo-restore-prepare-cleanup-test-0")
        .exists()
    );
  }

  #[test]
  fn destination_existence_changes_are_detected_before_publish() {
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("destination");
    let entry = RestoreJournalEntry {
      source: root.path().join("source"),
      destination: destination.clone(),
      rollback: root.path().join("rollback"),
      original_existed: Some(false),
      published: false,
    };
    assert!(destination_existence_matches(&entry));
    std::fs::write(destination, b"concurrent data").unwrap();
    assert!(!destination_existence_matches(&entry));
  }

  #[test]
  fn staging_journal_removes_all_owned_restore_paths() {
    let root = tempfile::tempdir().unwrap();
    let staging = root.path().join("staging");
    let destination_copy = root.path().join("destination-copy");
    std::fs::create_dir_all(&staging).unwrap();
    std::fs::write(&destination_copy, b"prepared").unwrap();
    let journal_path = root.path().join("staging.json");
    persist_journal(
      &journal_path,
      &RestoreStagingJournal {
        paths: vec![staging.clone(), destination_copy.clone()],
      },
    )
    .unwrap();
    cleanup_restore_staging_journal(&journal_path).unwrap();
    assert!(!staging.exists());
    assert!(!destination_copy.exists());
    assert!(!journal_path.exists());
  }

  #[test]
  fn repeated_quiesce_attempts_preserve_every_pending_container() {
    assert_eq!(
      merge_container_quiesce_sets(
        &["original".into(), "shared".into()],
        &["retry".into(), "shared".into()],
      ),
      vec![
        "original".to_string(),
        "retry".to_string(),
        "shared".to_string(),
      ]
    );
  }

  #[test]
  fn backup_and_restore_quiescing_include_named_volume_mounts() {
    assert!(mount_type_affects_paths(Some("bind")));
    assert!(mount_type_affects_paths(Some("volume")));
    assert!(!mount_type_affects_paths(Some("tmpfs")));
    assert!(!mount_type_affects_paths(None));
  }

  #[test]
  fn volume_quiescing_includes_bind_sources_above_at_and_below_storage()
   {
    let paths = BTreeSet::from([PathBuf::from(
      "/docker-data/volumes/data/_data",
    )]);
    for source in [
      "/docker-data/volumes/data/_data",
      "/docker-data/volumes/data/_data/database",
      "/docker-data/volumes/data",
    ] {
      assert!(mount_affects_paths(
        Some("bind"),
        Some(source),
        &paths
      ));
      assert!(mount_affects_paths(
        Some("volume"),
        Some(source),
        &paths
      ));
    }
    for source in [
      "/docker-data/volumes/other/_data",
      "/docker-data/volumes/data/_data-other",
    ] {
      assert!(!mount_affects_paths(
        Some("bind"),
        Some(source),
        &paths
      ));
    }
    assert!(!mount_affects_paths(
      Some("tmpfs"),
      Some("/docker-data"),
      &paths
    ));
    assert!(!mount_affects_paths(Some("bind"), None, &paths));
  }

  #[test]
  fn quiescing_excludes_only_the_current_worker_identity() {
    let worker = ContainerListItem {
      id: Some("a".repeat(64)),
      state: ContainerStateStatusEnum::Running,
      ..Default::default()
    };
    assert!(!container_is_quiesce_candidate(
      &worker,
      Some(&"a".repeat(64))
    ));
    assert!(container_is_quiesce_candidate(
      &worker,
      Some(&"b".repeat(64))
    ));
    assert!(container_is_quiesce_candidate(&worker, None));
    assert!(container_is_quiesce_candidate(
      &worker,
      Some("periphery")
    ));
    let stopped = ContainerListItem {
      state: ContainerStateStatusEnum::Exited,
      ..worker
    };
    assert!(!container_is_quiesce_candidate(&stopped, None));
  }

  #[test]
  fn missing_volume_preview_is_relative_and_does_not_inspect_host_paths()
   {
    let publish = vec![RestorePublishPath {
      destination_root: None,
      snapshot_path: "snapshot/data".into(),
      destination: "/var/lib/docker/volumes/new-volume/_data".into(),
    }];
    let items = vec![komodo_backup::SnapshotPath {
      path: "snapshot/data/config.toml".into(),
      directory: false,
    }];
    let (created, overwritten, deleted) =
      compare_missing_volume_paths(
        &items,
        &publish,
        "new-volume",
        Instant::now() + RESTORE_PREFLIGHT_TIMEOUT,
      )
      .unwrap();
    assert_eq!(created, ["volume://new-volume/config.toml"]);
    assert!(overwritten.is_empty());
    assert!(deleted.is_empty());
    let outside = vec![RestorePublishPath {
      destination: "/etc".into(),
      ..publish[0].clone()
    }];
    assert!(
      compare_missing_volume_paths(
        &items,
        &outside,
        "new-volume",
        Instant::now() + RESTORE_PREFLIGHT_TIMEOUT
      )
      .is_err()
    );
  }
}
