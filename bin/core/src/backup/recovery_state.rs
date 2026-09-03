//! One durable commit point for the backup key, identity, and active database.

use std::{
  fs::OpenOptions, io::Write, os::unix::fs::OpenOptionsExt,
  path::Path, sync::OnceLock,
};

use anyhow::{Context, anyhow};
use rand::RngExt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const STATE_PATH: &str = "/data/core-secrets/state.json";

#[derive(Clone, Serialize, Deserialize)]
pub struct Identity {
  pub key: [u8; 32],
  pub core_instance_id: String,
}

impl Identity {
  pub fn validate(&self) -> anyhow::Result<()> {
    if self.core_instance_id.len() != 32
      || !self.core_instance_id.bytes().all(|b| b.is_ascii_hexdigit())
    {
      return Err(anyhow!("Invalid Core backup identity"));
    }
    Ok(())
  }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Activation {
  pub identity: Identity,
  pub database: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct CoreState {
  schema: u32,
  pub identity: Identity,
  pub database: Option<String>,
  pub previous: Option<Activation>,
}

impl CoreState {
  fn validate(&self) -> anyhow::Result<()> {
    if self.schema != 1 {
      return Err(anyhow!("Unsupported Core state schema"));
    }
    self.identity.validate()?;
    if let Some(database) = &self.database {
      validate_database(database)?;
    }
    if let Some(previous) = &self.previous {
      previous.identity.validate()?;
      validate_database(&previous.database)?;
    }
    Ok(())
  }

  pub fn activated(
    &self,
    identity: Identity,
    database: String,
    current_database: String,
  ) -> anyhow::Result<Self> {
    let next = Self {
      schema: 1,
      identity,
      database: Some(database),
      previous: Some(Activation {
        identity: self.identity.clone(),
        database: current_database,
      }),
    };
    next.validate()?;
    Ok(next)
  }
}

fn validate_database(name: &str) -> anyhow::Result<()> {
  if !crate::state::valid_recovery_database_name(name) {
    return Err(anyhow!("Invalid Core recovery database name"));
  }
  Ok(())
}

pub fn current() -> anyhow::Result<&'static CoreState> {
  static STATE: OnceLock<Result<CoreState, String>> = OnceLock::new();
  STATE
    .get_or_init(|| {
      load_or_create(Path::new(STATE_PATH))
        .map_err(|e| format!("{e:#}"))
    })
    .as_ref()
    .map_err(|e| anyhow!(e.clone()))
}

fn read(path: &Path) -> anyhow::Result<Option<CoreState>> {
  let metadata = match std::fs::symlink_metadata(path) {
    Ok(metadata) => metadata,
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
      return Ok(None);
    }
    Err(error) => return Err(error.into()),
  };
  if !metadata.is_file() || metadata.len() > 64 * 1024 {
    return Err(anyhow!(
      "Core state must be a regular file smaller than 64 KiB"
    ));
  }
  let state: CoreState =
    serde_json::from_slice(&std::fs::read(path)?)
      .context("Invalid persisted Core state")?;
  state.validate()?;
  Ok(Some(state))
}

fn load_or_create(path: &Path) -> anyhow::Result<CoreState> {
  if let Some(state) = read(path)? {
    return Ok(state);
  }
  let mut key = [0; 32];
  rand::rng().fill(&mut key);
  let state = CoreState {
    schema: 1,
    identity: Identity {
      key,
      core_instance_id: Uuid::new_v4().simple().to_string(),
    },
    database: None,
    previous: None,
  };
  let parent =
    path.parent().context("Core state path has no parent")?;
  std::fs::create_dir_all(parent)?;
  let mut file = match OpenOptions::new()
    .create_new(true)
    .write(true)
    .mode(0o600)
    .open(path)
  {
    Ok(file) => file,
    Err(error)
      if error.kind() == std::io::ErrorKind::AlreadyExists =>
    {
      return read(path)?
        .context("Core state disappeared during initialization");
    }
    Err(error) => return Err(error.into()),
  };
  file.write_all(&serde_json::to_vec(&state)?)?;
  file.sync_all()?;
  std::fs::File::open(parent)?.sync_all()?;
  Ok(state)
}

pub fn activate(state: &CoreState) -> anyhow::Result<()> {
  persist(Path::new(STATE_PATH), state)
}

fn persist(path: &Path, state: &CoreState) -> anyhow::Result<()> {
  state.validate()?;
  let parent =
    path.parent().context("Core state path has no parent")?;
  let temporary =
    parent.join(format!(".core-state-{}.tmp", Uuid::new_v4()));
  let result = (|| {
    let mut file = OpenOptions::new()
      .create_new(true)
      .write(true)
      .mode(0o600)
      .open(&temporary)?;
    file.write_all(&serde_json::to_vec(state)?)?;
    file.sync_all()?;
    let directory = std::fs::File::open(parent)?;
    std::fs::rename(&temporary, path)?;
    // Rename is the commit point. A directory-sync failure must not let the
    // caller resume writes against the old database instead of restarting.
    if let Err(error) = directory.sync_all() {
      warn!(
        "Core recovery state was activated but directory sync failed: {error}"
      );
    }
    anyhow::Ok(())
  })();
  if result.is_err() {
    let _ = std::fs::remove_file(temporary);
  }
  result
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn initialization_preserves_identity_and_rejects_corrupt_state() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state.json");
    let first = load_or_create(&path).unwrap();
    assert_eq!(
      first.identity.key,
      load_or_create(&path).unwrap().identity.key
    );
    std::fs::write(&path, b"broken").unwrap();
    assert!(load_or_create(&path).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"broken");
  }

  #[test]
  fn activation_switches_the_key_and_database_as_one_record() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state.json");
    let original = load_or_create(&path).unwrap();
    let recovered = Identity {
      key: [7; 32],
      core_instance_id: "a".repeat(32),
    };
    let next = original
      .activated(recovered, "recovered".into(), "original".into())
      .unwrap();
    assert_eq!(
      read(&path).unwrap().unwrap().identity.key,
      original.identity.key
    );
    persist(&path, &next).unwrap();
    let active = read(&path).unwrap().unwrap();
    assert_eq!(active.database.as_deref(), Some("recovered"));
    assert_eq!(active.identity.key, [7; 32]);
    let previous = active.previous.unwrap();
    assert_eq!(previous.database, "original");
    assert_eq!(previous.identity.key, original.identity.key);
  }

  #[test]
  fn invalid_activation_does_not_replace_existing_state() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state.json");
    let mut state = load_or_create(&path).unwrap();
    let before = std::fs::read(&path).unwrap();
    state.database = Some("unsafe/name".into());
    assert!(persist(&path, &state).is_err());
    assert_eq!(std::fs::read(path).unwrap(), before);
  }
}
