use std::{
  collections::{BTreeSet, HashMap, HashSet},
  fs::OpenOptions,
  io::{Read, Write},
  os::unix::fs::{MetadataExt, PermissionsExt},
  path::{Path, PathBuf},
  sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicBool, Ordering},
  },
};

use anyhow::{Context, anyhow};
use command::{CommandOptions, run_komodo_standard_command};
use komodo_backup::VykarRepository;
use komodo_client::entities::docker::{
  container::{ContainerListItem, ContainerStateStatusEnum},
  volume::VolumeScopeEnum,
};
use mogh_resolver::Resolve;
use periphery_client::api::backup::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use shell_escape::unix::escape;

use crate::{config::periphery_config, state::docker_client};

use super::Args;

const COMPOSE_PROJECT_LABEL: &str = "com.docker.compose.project";

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

fn cancelled_operations() -> &'static Mutex<HashSet<String>> {
  static CANCELLED: OnceLock<Mutex<HashSet<String>>> =
    OnceLock::new();
  CANCELLED.get_or_init(Default::default)
}

fn operation_cancelled(operation_id: &str) -> bool {
  cancelled_operations()
    .lock()
    .unwrap()
    .contains(operation_id)
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
    discover_source(&self.target).await
  }
}

impl Resolve<Args> for RunVykarBackup {
  async fn resolve(
    self,
    _: &Args,
  ) -> anyhow::Result<RunVykarBackupResponse> {
    let _operation = backup_operation_lock().lock().await;
    let discovered = discover_source(&self.target).await?;
    let mut stopped: Vec<String> = Vec::new();
    if self.stop_containers {
      for container in &discovered.running_containers {
        if let Err(error) =
          run_container_command("stop", container).await
        {
          let (restarted, restart_errors) =
            restart_containers(&stopped).await;
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

    let (restarted, restart_errors) =
      restart_containers(&stopped).await;

    cancelled_operations().lock().unwrap().remove(&self.run_id);
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
    let mut discovered = Vec::new();
    let mut discovery_errors = Vec::new();
    let mut running = BTreeSet::new();
    for task in self.tasks {
      match discover_source(&task.target).await {
        Ok(source) => {
          running.extend(source.running_containers.iter().cloned());
          discovered.push((task, source.paths));
        }
        Err(error) => discovery_errors
          .push(format!("{}: {error:#}", task.source_label)),
      }
    }
    let mut stopped: Vec<String> = Vec::new();
    if self.stop_containers {
      for container in running {
        if let Err(error) =
          run_container_command("stop", &container).await
        {
          let (_, restart_errors) =
            restart_containers(&stopped).await;
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
        stop_containers: false,
        mirror_only: task.mirror_only,
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

    let (_, restart_errors) = restart_containers(&stopped).await;
    cancelled_operations().lock().unwrap().remove(&self.run_id);
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
  let manifest_dir = tempfile::Builder::new()
    .prefix("komodo-backup-manifest-")
    .tempdir()
    .context("Failed to create backup manifest staging directory")?;
  write_manifest(request, source_paths, manifest_dir.path())?;
  let mut paths = source_paths.to_vec();
  paths.push(manifest_dir.path().to_string_lossy().into_owned());

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
    )
    .await
  };
  if operation_cancelled(&request.run_id) {
    return Err(anyhow!("Backup cancelled before mirror write"));
  }
  let mirror = if let Some(repository) = request.mirror.clone() {
    Some(
      run_repository_backup(
        repository,
        request.advanced.clone(),
        request.hostname.clone(),
        request.snapshot_name.clone(),
        request.source_label.clone(),
        paths,
      )
      .await,
    )
  } else {
    if request.mirror_only {
      return Err(anyhow!(
        "Mirror-only retry requested without a configured mirror"
      ));
    }
    None
  };
  Ok((primary, mirror))
}

async fn run_repository_backup(
  repository: komodo_client::entities::backup::BackupRepository,
  advanced: komodo_client::entities::backup::BackupAdvancedSettings,
  hostname: String,
  snapshot_name: String,
  source_label: String,
  source_paths: Vec<String>,
) -> VykarBackupRepositoryResult {
  let result = tokio::task::spawn_blocking(move || {
    let cache = vykar_cache_dir(&hostname)?;
    let repository = VykarRepository::new(
      &repository,
      &hostname,
      &cache,
      &advanced,
    )?;
    repository.backup(&snapshot_name, &source_label, &source_paths)
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
  target: &'a PeripheryBackupTarget,
  configuration_sha256: String,
  paths_sha256: String,
}

fn write_manifest(
  request: &RunVykarBackup,
  paths: &[String],
  directory: &Path,
) -> anyhow::Result<()> {
  let target = serde_json::to_vec(&request.target)
    .context("Failed to serialize backup source identity")?;
  let manifest = KomodoBackupManifest {
    schema: "komodo.backup-manifest/v1",
    version: 1,
    run_id: &request.run_id,
    source_label: &request.source_label,
    hostname: &request.hostname,
    komodo_version: &request.komodo_version,
    paths,
    target: &request.target,
    configuration_sha256: hex::encode(Sha256::digest(target)),
    paths_sha256: hex::encode(Sha256::digest(
      serde_json::to_vec(paths)
        .context("Failed to serialize backup source paths")?,
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

fn paths_overlap(left: &Path, right: &Path) -> bool {
  left == right || left.starts_with(right) || right.starts_with(left)
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
    let source = match mount {
      BackupComposeMount::Long { mount_type, source } => source
        .filter(|source| {
          mount_type.as_deref() == Some("bind")
            || mount_type.is_none() && Path::new(source).is_absolute()
        }),
      BackupComposeMount::Short(value) => {
        value.split_once(':').and_then(|(source, _)| {
          (Path::new(source).is_absolute() || source.starts_with('.'))
            .then(|| source.to_string())
        })
      }
    };
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

async fn affected_running_containers(
  docker: &crate::docker::DockerClient,
  containers: &[ContainerListItem],
  project_name: Option<&str>,
  paths: &BTreeSet<PathBuf>,
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
      .filter(|mount| mount.typ.as_deref() == Some("bind"))
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

async fn discover_source(
  target: &PeripheryBackupTarget,
) -> anyhow::Result<DiscoverBackupSourceResponse> {
  let docker_guard = docker_client().load();
  let docker = docker_guard
    .as_ref()
    .as_ref()
    .context("Docker is unavailable")?;
  let containers = docker.list_containers().await?;
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
      let mut affected_paths = bind_paths.clone();
      affected_paths.insert(run_directory.clone());
      let running = affected_running_containers(
        docker,
        &containers,
        Some(&project_name),
        &affected_paths,
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
      let running_containers = containers
        .into_iter()
        .filter(|container| {
          container.state == ContainerStateStatusEnum::Running
            && container.volumes.contains(volume_name)
        })
        .map(|container| container.name)
        .collect();
      Ok(DiscoverBackupSourceResponse {
        paths: vec![
          validate_source_path(Path::new(&volume.mountpoint))?
            .to_string_lossy()
            .into_owned(),
        ],
        running_containers,
      })
    }
  }
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
          let path = PathBuf::from(&item.destination);
          path.canonicalize().unwrap_or(path)
        })
        .collect::<BTreeSet<_>>();
      let project_name = stack.project_name(false);
      affected_running_containers(
        docker,
        &containers,
        Some(&project_name),
        &paths,
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
    if self.create_volume_if_missing
      && let PeripheryBackupTarget::Volume { volume_name } =
        &self.target
      && discover_source(&self.target).await.is_err()
    {
      run_container_command("volume create", volume_name).await?;
    }
    let running_containers =
      discover_running_containers(&self.target, &self.publish)
        .await?;
    if let PeripheryBackupTarget::Volume { volume_name } =
      &self.target
    {
      let mountpoint = discover_source(&self.target)
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
    let mut stopped_containers: Vec<String> = Vec::new();
    for container in &running_containers {
      if let Err(stop_error) =
        run_container_command("stop", container).await
      {
        let mut restart_errors = Vec::new();
        for stopped in &stopped_containers {
          if let Err(error) =
            run_container_command("start", stopped).await
          {
            restart_errors.push(format!("{stopped}: {error:#}"));
          }
        }
        return Ok(TransactionalVykarRestoreResponse {
          complete: false,
          rolled_back: true,
          containers_restarted: if restart_errors.is_empty() {
            stopped_containers
          } else {
            Vec::new()
          },
          critical_error: (!restart_errors.is_empty()).then(|| {
            format!(
              "Restore quiesce failed ({stop_error:#}) and container state is indeterminate: {}",
              restart_errors.join("; ")
            )
          }),
        });
      }
      stopped_containers.push(container.clone());
    }

    let restore_result = transactional_restore(&self).await;
    cancelled_operations()
      .lock()
      .unwrap()
      .remove(&self.journal_id);
    let rolled_back = match restore_result {
      RestoreTransactionResult::Published { rolled_back } => {
        rolled_back
      }
      RestoreTransactionResult::FailedBeforePublication(error) => {
        warn!(
          "Restore failed before publication; original data is unchanged: {error:#}"
        );
        let mut restarted = Vec::new();
        let mut restart_errors = Vec::new();
        for container in &stopped_containers {
          match run_container_command("start", container).await {
            Ok(()) => restarted.push(container.clone()),
            Err(error) => {
              restart_errors.push(format!("{container}: {error:#}"))
            }
          }
        }
        return Ok(TransactionalVykarRestoreResponse {
          complete: false,
          rolled_back: true,
          containers_restarted: if restart_errors.is_empty() {
            restarted
          } else {
            Vec::new()
          },
          critical_error: (!restart_errors.is_empty()).then(|| {
            format!(
              "Restore failed before publication ({error:#}) and affected containers could not all be restarted: {}",
              restart_errors.join("; ")
            )
          }),
        });
      }
      RestoreTransactionResult::Indeterminate(error) => {
        return Ok(TransactionalVykarRestoreResponse {
          complete: false,
          rolled_back: false,
          containers_restarted: Vec::new(),
          critical_error: Some(format!(
            "Restore state is indeterminate; affected containers remain stopped: {error:#}"
          )),
        });
      }
    };
    let mut restarted = Vec::new();
    let mut restart_errors = Vec::new();
    for container in &stopped_containers {
      match run_container_command("start", container).await {
        Ok(()) => restarted.push(container.clone()),
        Err(error) => {
          restart_errors.push(format!("{container}: {error:#}"))
        }
      }
    }
    if restart_errors.is_empty() {
      Ok(TransactionalVykarRestoreResponse {
        complete: !rolled_back,
        rolled_back,
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
    let running_containers =
      discover_running_containers(&self.target, &self.publish)
        .await?;
    let discovered = discover_source(&self.target).await.ok();
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
  Published { rolled_back: bool },
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
    return RestoreTransactionResult::Published { rolled_back: true };
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
      let _ = std::fs::remove_dir_all(&staging);
      return RestoreTransactionResult::FailedBeforePublication(
        error,
      );
    }
    Err(error) => {
      let _ = std::fs::remove_dir_all(&staging);
      return RestoreTransactionResult::FailedBeforePublication(
        anyhow::Error::new(error)
          .context("Vykar restore worker failed"),
      );
    }
  }

  if operation_cancelled(&request.journal_id) {
    let _ = std::fs::remove_dir_all(&staging);
    return RestoreTransactionResult::Published { rolled_back: true };
  }

  let publish = request.publish.clone();
  let journal_id = request.journal_id.clone();
  let publication_started = Arc::new(AtomicBool::new(false));
  let worker_started = publication_started.clone();
  let result = tokio::task::spawn_blocking(move || {
    publish_restore(&staging, &publish, &journal_id, &worker_started)
  })
  .await;
  match result {
    Ok(Ok(rolled_back)) => {
      RestoreTransactionResult::Published { rolled_back }
    }
    Ok(Err(error)) => {
      if publication_started.load(Ordering::SeqCst) {
        RestoreTransactionResult::Indeterminate(error)
      } else {
        let _ = std::fs::remove_dir_all(&staging);
        RestoreTransactionResult::FailedBeforePublication(error)
      }
    }
    Err(error) => {
      let error = anyhow::Error::new(error)
        .context("Restore publish worker failed");
      if publication_started.load(Ordering::SeqCst) {
        RestoreTransactionResult::Indeterminate(error)
      } else {
        let _ = std::fs::remove_dir_all(&staging);
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
  published: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RestoreJournal {
  staging: PathBuf,
  entries: Vec<RestoreJournalEntry>,
  #[serde(default)]
  committed: bool,
}

fn restore_journal_dir() -> anyhow::Result<PathBuf> {
  let directory = periphery_config()
    .stack_dir()
    .join(".komodo-vykar")
    .join("restore-journals");
  std::fs::create_dir_all(&directory)?;
  Ok(directory)
}

/// Roll back any publication interrupted after its durable journal was
/// written. This runs before Periphery accepts requests.
pub(crate) fn recover_restore_journals() -> anyhow::Result<()> {
  let directory = restore_journal_dir()?;
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
    }
    remove_path(&journal.staging)?;
    remove_path(&path)?;
    fsync_parent(&path)?;
    warn!("Recovered interrupted restore journal {}", path.display());
  }
  Ok(())
}

fn publish_restore(
  staging: &Path,
  publish: &[RestorePublishPath],
  journal_id: &str,
  publication_started: &AtomicBool,
) -> anyhow::Result<bool> {
  let journal_directory = restore_journal_dir()?;
  publish_restore_in(
    staging,
    publish,
    journal_id,
    publication_started,
    &journal_directory,
  )
}

fn publish_restore_in(
  staging: &Path,
  publish: &[RestorePublishPath],
  journal_id: &str,
  publication_started: &AtomicBool,
  journal_directory: &Path,
) -> anyhow::Result<bool> {
  let mut entries = Vec::new();
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
    let rollback = destination
      .with_extension(format!("komodo-rollback-{journal_id}"));
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
    let destination_parent = destination
      .parent()
      .context("Restore destination has no parent")?;
    std::fs::create_dir_all(destination_parent)?;
    let source = destination_parent
      .join(format!(".komodo-restore-{journal_id}-{index}"));
    if path_lexists(&source) {
      return Err(anyhow!(
        "Same-filesystem restore staging path already exists: {}",
        source.display()
      ));
    }
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
      remove_path(&source)?;
      return Err(anyhow!(
        "Same-filesystem restore staging verification failed"
      ));
    }
    entries.push(RestoreJournalEntry {
      source,
      destination,
      rollback,
      published: false,
    });
  }

  std::fs::create_dir_all(journal_directory)?;
  let journal_path =
    journal_directory.join(format!("{journal_id}.json"));
  let mut journal = RestoreJournal {
    staging: staging.to_path_buf(),
    entries,
    committed: false,
  };
  persist_journal(&journal_path, &journal)?;
  publication_started.store(true, Ordering::SeqCst);

  for index in 0..journal.entries.len() {
    if path_lexists(&journal.entries[index].destination)
      && let Err(error) = std::fs::rename(
        &journal.entries[index].destination,
        &journal.entries[index].rollback,
      )
    {
      rollback_published(&mut journal, &journal_path)?;
      warn!(
        "Restore rollback preparation failed and earlier publications were rolled back: {error:#}"
      );
      cleanup_rolled_back_restore(&journal, &journal_path)?;
      return Ok(true);
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

  journal.committed = true;
  persist_journal(&journal_path, &journal)?;
  for entry in &journal.entries {
    if path_lexists(&entry.rollback) {
      remove_path(&entry.rollback)?;
    }
    fsync_parent(&entry.destination)?;
  }
  std::fs::remove_file(&journal_path)?;
  fsync_parent(&journal_path)?;
  let _ = std::fs::remove_dir_all(staging);
  Ok(false)
}

fn cleanup_rolled_back_restore(
  journal: &RestoreJournal,
  journal_path: &Path,
) -> anyhow::Result<()> {
  remove_path(journal_path)?;
  for entry in &journal.entries {
    remove_path(&entry.source)?;
  }
  remove_path(&journal.staging)?;
  fsync_parent(journal_path)
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

fn rollback_published(
  restore: &mut RestoreJournal,
  journal_path: &Path,
) -> anyhow::Result<()> {
  for index in (0..restore.entries.len()).rev() {
    let entry = &restore.entries[index];
    let published = entry.published;
    let rollback = entry.rollback.clone();
    let destination = entry.destination.clone();
    // A crash can happen after destination -> rollback but before publication
    // intent is persisted. A rollback path independently proves publication
    // preparation began.
    if (published || path_lexists(&rollback))
      && path_lexists(&destination)
    {
      remove_path(&destination)?;
    }
    if path_lexists(&rollback) {
      std::fs::rename(&rollback, &destination)?;
      fsync_parent(&destination)?;
    }
    restore.entries[index].published = false;
    persist_journal(journal_path, restore)?;
  }
  Ok(())
}

fn persist_journal(
  path: &Path,
  journal: &RestoreJournal,
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

impl Resolve<Args> for CancelVykarOperation {
  async fn resolve(
    self,
    _: &Args,
  ) -> anyhow::Result<CancelVykarOperationResponse> {
    cancelled_operations()
      .lock()
      .unwrap()
      .insert(self.operation_id);
    Ok(CancelVykarOperationResponse { cancelled: true })
  }
}

#[cfg(test)]
mod tests {
  use super::*;

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
      )
      .unwrap()
    );
    assert_eq!(
      std::fs::read(first.join("original.txt")).unwrap(),
      b"original"
    );
    assert!(!first.join("new.txt").exists());
  }
}
