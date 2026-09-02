use std::{
  collections::{BTreeMap, BTreeSet, HashMap, HashSet},
  fs::OpenOptions,
  io::{Read, Write},
  os::unix::fs::{MetadataExt, PermissionsExt},
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

#[derive(Deserialize)]
#[serde(untagged)]
enum BackupComposeMount {
  Short(String),
  Long {
    #[serde(rename = "type")]
    mount_type: Option<String>,
    source: Option<String>,
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
    let (_cancellation, _cancellation_registration) =
      register_operation_cancellation(&self.run_id);
    let mut discovered = Vec::new();
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

    let mut results = Vec::new();
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
      request.advanced.clone(),
      request.hostname.clone(),
      request.snapshot_name.clone(),
      request.source_label.clone(),
      paths.clone(),
      operation_cancellation_token(&request.run_id),
      !request.filters.include_cross_filesystem_mounts,
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
    Some(
      run_repository_backup(
        repository,
        request.advanced.clone(),
        request.hostname.clone(),
        request.snapshot_name.clone(),
        request.source_label.clone(),
        paths,
        operation_cancellation_token(&request.run_id),
        !request.filters.include_cross_filesystem_mounts,
      )
      .await,
    )
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
  advanced: komodo_client::entities::backup::BackupAdvancedSettings,
  hostname: String,
  snapshot_name: String,
  source_label: String,
  source_paths: Vec<String>,
  cancellation: Arc<AtomicBool>,
  one_file_system: bool,
) -> VykarBackupRepositoryResult {
  let result = tokio::task::spawn_blocking(move || {
    let cache = vykar_cache_dir(&hostname)?;
    let repository = VykarRepository::new(
      &repository,
      &hostname,
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
    return Err(anyhow!(
      "{label} '{}' overlaps Periphery's internal backup storage '{}'",
      path.display(),
      internal_storage.display()
    ));
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
  protected_repository_paths: &[String],
) -> anyhow::Result<()> {
  validate_resolved_restore_destinations(publish)?;
  if protected_repository_paths.is_empty() {
    return Ok(());
  }
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
    BackupComposeMount::Long { mount_type, source } => {
      source.filter(|source| {
        mount_type.as_deref() == Some("bind")
          || mount_type.is_none() && Path::new(source).is_absolute()
      })
    }
    BackupComposeMount::Short(value) => {
      value.split_once(':').and_then(|(source, _)| {
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

fn rewrite_compose_bind_mappings(
  document: &mut serde_yaml_ng::Value,
  mappings: &HashMap<String, String>,
  path_aliases: &HashMap<String, String>,
) -> usize {
  use serde_yaml_ng::Value;

  let key = |value: &str| Value::String(value.into());
  let Some(services) = document
    .as_mapping_mut()
    .and_then(|root| root.get_mut(&key("services")))
    .and_then(Value::as_mapping_mut)
  else {
    return 0;
  };
  let mut rewritten = 0;
  for service in services.values_mut() {
    let Some(volumes) = service
      .as_mapping_mut()
      .and_then(|service| service.get_mut(&key("volumes")))
      .and_then(Value::as_sequence_mut)
    else {
      continue;
    };
    for volume in volumes {
      match volume {
        Value::String(short) => {
          let Some((source, suffix)) = short.split_once(':') else {
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
            .get(&key("type"))
            .and_then(Value::as_str)
            .map(str::to_owned);
          let Some(source) = long
            .get_mut(&key("source"))
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
    let text = std::fs::read_to_string(&path).with_context(|| {
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
    if rewrite_compose_bind_mappings(
      &mut document,
      &request.bind_path_mappings,
      &request.bind_path_aliases,
    ) == 0
    {
      continue;
    }
    let rewritten = serde_yaml_ng::to_string(&document)?;
    let mut file =
      OpenOptions::new().truncate(true).write(true).open(&path)?;
    file.write_all(rewritten.as_bytes())?;
    file.sync_all()?;
  }
  Ok(())
}

async fn affected_running_containers(
  docker: &crate::docker::DockerClient,
  containers: &[ContainerListItem],
  project_name: Option<&str>,
  paths: &BTreeSet<PathBuf>,
  include_named_volume_mounts: bool,
) -> anyhow::Result<Vec<String>> {
  let mut affected = BTreeSet::new();
  for container in containers.iter().filter(|container| {
    container.state == ContainerStateStatusEnum::Running
  }) {
    let same_project = project_name.is_some_and(|project| {
      container
        .labels
        .get(COMPOSE_PROJECT_LABEL)
        .map(String::as_str)
        == Some(project)
    });
    if same_project {
      affected.insert(container.name.clone());
      continue;
    }
    let inspected = docker.inspect_container(&container.name).await?;
    if inspected
      .mounts
      .into_iter()
      .filter(|mount| {
        mount_type_affects_paths(
          mount.typ.as_deref(),
          include_named_volume_mounts,
        )
      })
      .filter_map(|mount| mount.source)
      .map(|source| {
        let source = PathBuf::from(source);
        source.canonicalize().unwrap_or(source)
      })
      .any(|source| {
        paths.iter().any(|path| paths_overlap(&source, path))
      })
    {
      affected.insert(container.name.clone());
    }
  }
  Ok(affected.into_iter().collect())
}

fn mount_type_affects_paths(
  mount_type: Option<&str>,
  include_named_volume_mounts: bool,
) -> bool {
  mount_type == Some("bind")
    || include_named_volume_mounts && mount_type == Some("volume")
}

async fn discover_source(
  target: &PeripheryBackupTarget,
  protected_repository_paths: &[String],
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
        Some(&project_name),
        &affected_paths,
        false,
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
      let running_containers = containers
        .into_iter()
        .filter(|container| {
          container.state == ContainerStateStatusEnum::Running
            && container.volumes.contains(volume_name)
        })
        .map(|container| container.name)
        .collect();
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
    if !filters.include_cross_filesystem_mounts {
      if metadata.dev() != run_device {
        continue;
      }
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
  protected_repository_paths: &[String],
) -> anyhow::Result<Vec<PathBuf>> {
  if protected_repository_paths.is_empty() {
    return Ok(Vec::new());
  }
  let protected = protected_repository_paths
    .iter()
    .map(PathBuf::from)
    .collect::<Vec<_>>();
  let mut sources = protected
    .iter()
    .cloned()
    .map(|path| path.canonicalize().unwrap_or(path))
    .collect::<BTreeSet<_>>();
  for container in containers {
    let inspected = docker.inspect_container(&container.name).await?;
    for mount in inspected.mounts {
      let Some(destination) = mount.destination.map(PathBuf::from)
      else {
        continue;
      };
      let Some(source) = mount.source.map(PathBuf::from) else {
        continue;
      };
      for repository in &protected {
        let Some(mapped) =
          map_path_through_mount(repository, &destination, &source)
        else {
          continue;
        };
        sources.insert(mapped.canonicalize().unwrap_or(mapped));
      }
    }
  }
  Ok(sources.into_iter().collect())
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
      return Err(anyhow!(
        "{label} '{}' overlaps a Core-local repository mount '{}'",
        path.display(),
        repository.display()
      ));
    }
  }
  Ok(())
}

async fn discover_running_containers(
  target: &PeripheryBackupTarget,
  publish: &[RestorePublishPath],
) -> anyhow::Result<Vec<String>> {
  let docker_guard = docker_client().load();
  let docker = docker_guard
    .as_ref()
    .as_ref()
    .context("Docker is unavailable")?;
  let containers = docker.list_containers().await?;
  match target {
    PeripheryBackupTarget::Stack { stack, .. } => {
      let paths = publish
        .iter()
        .map(|item| {
          resolve_existing_ancestor(Path::new(&item.destination))
        })
        .collect::<anyhow::Result<BTreeSet<_>>>()?;
      let project_name = stack.project_name(false);
      affected_running_containers(
        docker,
        &containers,
        Some(&project_name),
        &paths,
        true,
      )
      .await
    }
    PeripheryBackupTarget::Volume { .. } => {
      Ok(running_containers_for_target(&containers, target))
    }
  }
}

fn running_containers_for_target(
  containers: &[ContainerListItem],
  target: &PeripheryBackupTarget,
) -> Vec<String> {
  match target {
    PeripheryBackupTarget::Stack { stack, .. } => {
      let project_name = stack.project_name(false);
      containers
        .iter()
        .filter(|container| {
          container.state == ContainerStateStatusEnum::Running
            && container.labels.get(COMPOSE_PROJECT_LABEL)
              == Some(&project_name)
        })
        .map(|container| container.name.clone())
        .collect()
    }
    PeripheryBackupTarget::Volume { volume_name } => containers
      .iter()
      .filter(|container| {
        container.state == ContainerStateStatusEnum::Running
          && container.volumes.contains(volume_name)
      })
      .map(|container| container.name.clone())
      .collect(),
  }
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
  }
  Ok(())
}

impl Resolve<Args> for TransactionalVykarRestore {
  async fn resolve(
    mut self,
    _: &Args,
  ) -> anyhow::Result<TransactionalVykarRestoreResponse> {
    let _operation = backup_operation_lock().lock().await;
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
          &[],
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
      let running_containers =
        discover_running_containers(&self.target, &self.publish)
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
    let discovered = match &self.target {
      PeripheryBackupTarget::Stack { .. } => {
        // A missing Stack destination can legitimately be planned as a
        // recovered Stack; execution recreates its mapped filesystem roots.
        discover_source(
          &self.target,
          &[],
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
              &[],
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
    validate_restore_destinations(
      &self.publish,
      &self.protected_repository_paths,
    )
    .await?;
    let running_containers =
      discover_running_containers(&self.target, &self.publish)
        .await?;
    let repository = self.repository.clone();
    let advanced = self.advanced.clone();
    let hostname = self.hostname.clone();
    let snapshot = self.snapshot_name.clone();
    let selected = self.selected_paths.clone();
    let snapshot_paths = tokio::task::spawn_blocking(move || {
      let cache = vykar_cache_dir(&hostname)?;
      VykarRepository::new(&repository, &hostname, &cache, &advanced)?
        .snapshot_paths(&snapshot, &selected)
    })
    .await
    .context("Vykar preflight worker failed")??;
    let publish = self.publish;
    let selected = self.selected_paths;
    let (created_paths, overwritten_paths, deleted_paths) =
      tokio::task::spawn_blocking(move || {
        compare_restore_paths(&snapshot_paths, &publish, &selected)
      })
      .await
      .context("Restore preflight filesystem worker failed")??;
    Ok(PreflightVykarRestoreResponse {
      destination_exists,
      created_paths,
      overwritten_paths,
      deleted_paths,
      containers_to_stop: running_containers,
    })
  }
}

fn compare_restore_paths(
  snapshot_paths: &[komodo_backup::SnapshotPath],
  publish: &[RestorePublishPath],
  selected: &[String],
) -> anyhow::Result<(Vec<String>, Vec<String>, Vec<String>)> {
  let mut expected = HashSet::<PathBuf>::new();
  let mut created = Vec::new();
  let mut overwritten = Vec::new();
  for item in snapshot_paths {
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
    expected.insert(destination.clone());
    if !path_lexists(&destination) {
      created.push(destination.to_string_lossy().into_owned());
    } else if !item.directory || !destination.is_dir() {
      overwritten.push(destination.to_string_lossy().into_owned());
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
    collect_unexpected_paths(&root, &expected, &mut deleted)?;
  }
  created.sort();
  created.dedup();
  overwritten.sort();
  overwritten.dedup();
  deleted.sort();
  deleted.dedup();
  Ok((created, overwritten, deleted))
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
) -> anyhow::Result<()> {
  if !path_lexists(root) {
    return Ok(());
  }
  if !expected.contains(root) {
    deleted.push(root.to_string_lossy().into_owned());
  }
  if root.is_dir() {
    for entry in std::fs::read_dir(root)? {
      let entry = entry?;
      let path = entry.path();
      let file_type = entry.file_type()?;
      if !expected.contains(&path) {
        deleted.push(path.to_string_lossy().into_owned());
      }
      if file_type.is_dir() {
        collect_unexpected_paths(&path, expected, deleted)?;
      }
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
  Indeterminate(anyhow::Error),
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
      &advanced,
    )?;
    repository.restore(&snapshot, &restore_staging, &selected)
  })
  .await;
  match restore_result {
    Ok(Ok(())) => {}
    Ok(Err(error)) => {
      let _ = cleanup_restore_staging_journal(&staging_journal);
      return RestoreTransactionResult::FailedBeforePublication(
        error,
      );
    }
    Err(error) => {
      let _ = cleanup_restore_staging_journal(&staging_journal);
      return RestoreTransactionResult::FailedBeforePublication(
        anyhow::Error::new(error)
          .context("Vykar restore worker failed"),
      );
    }
  }

  if let Err(error) =
    rewrite_recovered_stack_compose_files(request, &staging)
  {
    let _ = cleanup_restore_staging_journal(&staging_journal);
    return RestoreTransactionResult::FailedBeforePublication(error);
  }

  if operation_cancelled(&request.journal_id) {
    let _ = cleanup_restore_staging_journal(&staging_journal);
    return RestoreTransactionResult::Published {
      rolled_back: true,
      finalization_pending: false,
    };
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
        let _ = cleanup_restore_staging_journal(&staging_journal);
        RestoreTransactionResult::FailedBeforePublication(error)
      }
    }
    Err(error) => {
      let error = anyhow::Error::new(error)
        .context("Restore publish worker failed");
      if publication_started.load(Ordering::SeqCst) {
        RestoreTransactionResult::Indeterminate(error)
      } else {
        let _ = cleanup_restore_staging_journal(&staging_journal);
        RestoreTransactionResult::FailedBeforePublication(error)
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
      running_containers_for_target(&containers, &target),
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
      running_containers_for_target(&containers, &target),
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
        snapshot_path: "source/one".into(),
        destination: real.join("app").to_string_lossy().into_owned(),
      },
      RestorePublishPath {
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
  fn selected_volume_destinations_use_the_inspected_mountpoint() {
    let mut publish = vec![RestorePublishPath {
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
      snapshot_path: root.into(),
      destination: destination.path().to_string_lossy().into_owned(),
    }];
    let (created, overwritten, deleted) =
      compare_restore_paths(&paths, &publish, &[]).unwrap();
    assert!(created.iter().any(|path| path.ends_with("new.txt")));
    assert!(overwritten.iter().any(|path| path.ends_with("old.txt")));
    assert!(deleted.iter().any(|path| path.ends_with("extra.txt")));
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
    let publish = vec![
      RestorePublishPath {
        snapshot_path: "one".into(),
        destination: first.to_string_lossy().into_owned(),
      },
      RestorePublishPath {
        snapshot_path: "two.txt".into(),
        destination: first
          .join("child.txt")
          .to_string_lossy()
          .into_owned(),
      },
    ];
    assert!(
      publish_restore_in(
        &download,
        &publish,
        "rollback-test",
        &AtomicBool::new(false),
        &root.path().join("journals"),
        None,
        false,
      )
      .unwrap()
    );
    assert_eq!(
      std::fs::read(first.join("original.txt")).unwrap(),
      b"original"
    );
    assert!(!first.join("new.txt").exists());
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
        snapshot_path: "new-json".into(),
        destination: json.to_string_lossy().into_owned(),
      },
      RestorePublishPath {
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
        snapshot_path: "present".into(),
        destination: root
          .path()
          .join("first")
          .to_string_lossy()
          .into_owned(),
      },
      RestorePublishPath {
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
  fn restore_quiescing_includes_named_volume_mounts() {
    assert!(mount_type_affects_paths(Some("bind"), false));
    assert!(!mount_type_affects_paths(Some("volume"), false));
    assert!(mount_type_affects_paths(Some("volume"), true));
    assert!(!mount_type_affects_paths(Some("tmpfs"), true));
  }
}
