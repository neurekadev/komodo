use std::{
  path::Path,
  sync::{Arc, OnceLock},
};

use anyhow::{Context, anyhow};
use arc_swap::ArcSwap;
use komodo_client::entities::{
  ImageDigest,
  action::ActionState,
  build::BuildState,
  deployment::DeploymentState,
  docker::{
    DockerLists, SwarmLists, container::ContainerListItem,
    service::SwarmServiceListItem, swarm::SwarmInspectInfo,
  },
  procedure::ProcedureState,
  repo::RepoState,
  server::{PeripheryInformation, ServerHealth, ServerState},
  stack::{StackService, StackState},
  stats::{SystemInformation, SystemStats},
  swarm::SwarmState,
};
use mogh_cache::CloneCache;
use tokio_util::sync::CancellationToken;

use crate::{
  config::core_config,
  connection::PeripheryConnections,
  helpers::{
    action_state::ActionStates, all_resources::AllResourcesById,
    builder::BuilderUsage, image_digest::ImageDigestCache,
  },
};

static DB_CLIENT: OnceLock<database::Client> = OnceLock::new();

/// Atomic recovery activation containing both the database pointer and the
/// restored Core backup identity. Only Core may write this authority.
pub const CORE_RECOVERY_ACTIVATION_PATH: &str =
  "/core-secrets/backup-recovery-activation.json";

pub(crate) fn read_core_recovery_activation()
-> std::io::Result<Option<Vec<u8>>> {
  read_private_recovery_activation(
    Path::new(CORE_RECOVERY_ACTIVATION_PATH),
    &[
      Path::new("/data/backup-recovery-activation.json"),
      Path::new("/config/backup-recovery-activation.json"),
      Path::new("/data/backup-active-database"),
      Path::new("/config/backup-active-database"),
    ],
  )
}

fn read_private_recovery_activation(
  path: &Path,
  untrusted_paths: &[&Path],
) -> std::io::Result<Option<Vec<u8>>> {
  match std::fs::symlink_metadata(path) {
    Ok(metadata) if !metadata.is_file() => {
      return Err(std::io::Error::other(
        "Core recovery activation must be a regular Core-only file",
      ));
    }
    Ok(_) => return std::fs::read(path).map(Some),
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
    Err(error) => return Err(error),
  }
  // Never import an unsigned database selection from shared storage, even
  // as a fallback. Refuse startup instead of silently selecting another DB.
  for untrusted in untrusted_paths {
    match std::fs::symlink_metadata(untrusted) {
      Ok(_) => {
        return Err(std::io::Error::other(format!(
          "Untrusted recovery pointer at {}; an administrator must verify the database selection and persist it under /core-secrets before startup",
          untrusted.display()
        )));
      }
      Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
      Err(error) => return Err(error),
    }
  }
  Ok(None)
}

pub fn db_client() -> &'static database::Client {
  DB_CLIENT.get().unwrap_or_else(|| {
    error!(
      "FATAL: db_client accessed before initialized | Ensure init_db_client() is called during startup | Exiting..."
    );
    std::process::exit(1)
  })
}

/// Must be called in app startup sequence.
pub async fn init_db_client() {
  let init = async {
    let mut database = core_config().database.clone();
    let active_database = match read_core_recovery_activation() {
      Ok(Some(bytes)) => {
        let value: serde_json::Value = serde_json::from_slice(&bytes)
          .context("Invalid Core recovery activation record")?;
        let name = value
          .get("database")
          .and_then(serde_json::Value::as_str)
          .context("Core recovery activation has no database")?;
        let identity = value
          .get("core_instance_id")
          .and_then(serde_json::Value::as_str)
          .context("Core recovery activation has no identity")?;
        if identity.len() != 32
          || !identity
            .chars()
            .all(|character| character.is_ascii_hexdigit())
        {
          return Err(anyhow!(
            "Invalid Core recovery activation identity"
          ));
        }
        Some(name.to_string())
      }
      Ok(None) => None,
      Err(error) => {
        return Err(error)
          .context("Failed to read Core recovery activation record");
      }
    };
    if let Some(name) = active_database {
      let name = name.trim();
      if !name.is_empty()
        && name.chars().all(|character| {
          character.is_ascii_alphanumeric()
            || matches!(character, '_' | '-')
        })
      {
        info!("Using recovered active database '{name}'");
        database.db_name = name.to_string();
      } else {
        return Err(anyhow!(
          "Invalid active database recovery pointer"
        ));
      }
    }
    let client = database::Client::new(&database)
      .await
      .context("failed to initialize database client")?;
    DB_CLIENT.set(client).map_err(|_| {
    anyhow!(
      "db_client initialized more than once - this should not happen"
    )
  })?;
    anyhow::Ok(())
  }
  .await;
  if let Err(e) = init {
    error!(
      "FATAL: Failed to initialize database::Client | {e:#} | Exiting..."
    );
    std::process::exit(1)
  }
}

/// server id => connection
pub fn periphery_connections() -> &'static PeripheryConnections {
  static CONNECTIONS: OnceLock<PeripheryConnections> =
    OnceLock::new();
  CONNECTIONS.get_or_init(Default::default)
}

pub fn action_states() -> &'static ActionStates {
  static ACTION_STATES: OnceLock<ActionStates> = OnceLock::new();
  ACTION_STATES.get_or_init(ActionStates::default)
}

#[derive(Default, Debug)]
pub struct History<Curr: Default, Prev> {
  pub curr: Curr,
  pub prev: Option<Prev>,
}

#[derive(Default, Clone, Debug)]
pub struct CachedSwarmStatus {
  pub id: String,
  pub state: SwarmState,
  pub inspect: Option<SwarmInspectInfo>,
  pub lists: Option<SwarmLists>,
  /// Store the error in communicating with Swarm
  pub err: Option<mogh_error::Serror>,
}

pub type SwarmStatusCache =
  CloneCache<String, Arc<CachedSwarmStatus>>;

pub fn swarm_status_cache() -> &'static SwarmStatusCache {
  static SWARM_STATUS_CACHE: OnceLock<SwarmStatusCache> =
    OnceLock::new();
  SWARM_STATUS_CACHE.get_or_init(Default::default)
}

#[derive(Default, Clone, Debug)]
pub struct CachedServerStatus {
  pub id: String,
  pub state: ServerState,
  pub health: Option<ServerHealth>,
  pub periphery_info: Option<PeripheryInformation>,
  pub system_info: Option<SystemInformation>,
  pub system_stats: Option<SystemStats>,
  pub docker: Option<DockerLists>,
  /// Store the error in reaching periphery
  pub err: Option<mogh_error::Serror>,
}

pub type ServerStatusCache =
  CloneCache<String, Arc<CachedServerStatus>>;

pub fn server_status_cache() -> &'static ServerStatusCache {
  static SERVER_STATUS_CACHE: OnceLock<ServerStatusCache> =
    OnceLock::new();
  SERVER_STATUS_CACHE.get_or_init(Default::default)
}

#[derive(Default, Clone, Debug)]
pub struct CachedStackStatus {
  /// The stack id
  pub id: String,
  /// The stack state
  pub state: StackState,
  /// The services connected to the stack
  pub services: Vec<StackService>,
}

pub type StackStatusCache =
  CloneCache<String, Arc<History<CachedStackStatus, StackState>>>;

pub fn stack_status_cache() -> &'static StackStatusCache {
  static STACK_STATUS_CACHE: OnceLock<StackStatusCache> =
    OnceLock::new();
  STACK_STATUS_CACHE.get_or_init(Default::default)
}

#[derive(Default, Clone, Debug)]
pub struct CachedDeploymentStatus {
  /// The deployment id
  pub id: String,
  pub state: DeploymentState,
  pub container: Option<ContainerListItem>,
  pub service: Option<SwarmServiceListItem>,
  pub image_digests: Option<Vec<ImageDigest>>,
}

/// Cache of ids to status
pub type DeploymentStatusCache = CloneCache<
  String,
  Arc<History<CachedDeploymentStatus, DeploymentState>>,
>;

/// Cache of ids to status
pub fn deployment_status_cache() -> &'static DeploymentStatusCache {
  static DEPLOYMENT_STATUS_CACHE: OnceLock<DeploymentStatusCache> =
    OnceLock::new();
  DEPLOYMENT_STATUS_CACHE.get_or_init(Default::default)
}

pub type BuildStateCache = CloneCache<String, BuildState>;

pub fn build_state_cache() -> &'static BuildStateCache {
  static BUILD_STATE_CACHE: OnceLock<BuildStateCache> =
    OnceLock::new();
  BUILD_STATE_CACHE.get_or_init(Default::default)
}

#[derive(Default, Clone, Debug)]
pub struct CachedRepoStatus {
  pub latest_hash: Option<String>,
  pub latest_message: Option<String>,
}

pub type RepoStatusCache = CloneCache<String, Arc<CachedRepoStatus>>;

pub fn repo_status_cache() -> &'static RepoStatusCache {
  static REPO_STATUS_CACHE: OnceLock<RepoStatusCache> =
    OnceLock::new();
  REPO_STATUS_CACHE.get_or_init(Default::default)
}

pub type RepoStateCache = CloneCache<String, RepoState>;

pub fn repo_state_cache() -> &'static RepoStateCache {
  static REPO_STATE_CACHE: OnceLock<RepoStateCache> = OnceLock::new();
  REPO_STATE_CACHE.get_or_init(Default::default)
}

pub type ProcedureStateCache = CloneCache<String, ProcedureState>;

pub fn procedure_state_cache() -> &'static ProcedureStateCache {
  static PROCEDURE_STATE_CACHE: OnceLock<ProcedureStateCache> =
    OnceLock::new();
  PROCEDURE_STATE_CACHE.get_or_init(Default::default)
}

pub type ActionStateCache = CloneCache<String, ActionState>;

pub fn action_state_cache() -> &'static ActionStateCache {
  static ACTION_STATE_CACHE: OnceLock<ActionStateCache> =
    OnceLock::new();
  ACTION_STATE_CACHE.get_or_init(Default::default)
}

/// Store all resources in local cache for fast lookup
pub fn all_resources_cache() -> &'static ArcSwap<AllResourcesById> {
  static ALL_RESOURCES: OnceLock<ArcSwap<AllResourcesById>> =
    OnceLock::new();
  ALL_RESOURCES.get_or_init(Default::default)
}

/// Maps Image name => (Digest, valid until ms).
/// Cache the latest queried image digests in order
/// to infer whether deployments / stacks have updates available.
pub fn image_digest_cache() -> &'static ImageDigestCache {
  static IMAGE_DIGEST_CACHE: OnceLock<Arc<ImageDigestCache>> =
    OnceLock::new();
  IMAGE_DIGEST_CACHE.get_or_init(ImageDigestCache::new)
}

/// Maps Builder id => downstream count map (eg server id => active count)
type BuilderUsageCache = CloneCache<String, Arc<BuilderUsage>>;

/// For builders with multiple downstream machines to choose.
/// Stores active build count for each downstream.
/// Maps Builder id => downstream count map
pub fn builder_usage_cache() -> &'static BuilderUsageCache {
  static BUILDER_USAGE_CACHE: OnceLock<BuilderUsageCache> =
    OnceLock::new();
  BUILDER_USAGE_CACHE.get_or_init(Default::default)
}

type CancelCache = CloneCache<String, CancellationToken>;

/// Maps procedure id => CancellationToken
pub fn procedure_cancel_cache() -> &'static CancelCache {
  static PROCEDURE_CANCEL_CACHE: OnceLock<CancelCache> =
    OnceLock::new();
  PROCEDURE_CANCEL_CACHE.get_or_init(Default::default)
}

/// Maps update id => CancellationToken
pub fn action_cancel_cache() -> &'static CancelCache {
  static ACTION_CANCEL_CACHE: OnceLock<CancelCache> = OnceLock::new();
  ACTION_CANCEL_CACHE.get_or_init(Default::default)
}

#[cfg(test)]
mod recovery_activation_tests {
  use super::*;

  #[test]
  fn shared_pointer_cannot_select_or_migrate_a_database() {
    let directory = tempfile::tempdir().unwrap();
    let private = directory.path().join("private.json");
    let shared = directory.path().join("shared.json");
    assert!(
      read_private_recovery_activation(&private, &[&shared])
        .unwrap()
        .is_none()
    );
    std::fs::write(&shared, br#"{"database":"attacker"}"#).unwrap();
    assert!(
      read_private_recovery_activation(&private, &[&shared]).is_err()
    );
    assert!(!private.exists());
    let trusted = br#"{"database":"verified"}"#;
    std::fs::write(&private, trusted).unwrap();
    assert_eq!(
      read_private_recovery_activation(&private, &[&shared])
        .unwrap()
        .unwrap(),
      trusted
    );
  }

  #[test]
  fn private_activation_cannot_be_a_symlink_to_shared_storage() {
    let directory = tempfile::tempdir().unwrap();
    let private = directory.path().join("private.json");
    let shared = directory.path().join("shared.json");
    std::fs::write(&shared, b"{}").unwrap();
    std::os::unix::fs::symlink(&shared, &private).unwrap();
    assert!(read_private_recovery_activation(&private, &[]).is_err());
  }
}
