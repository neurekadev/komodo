use super::*;

pub(super) const MATERIAL_FILE: &str = "komodo-core-recovery.json";
pub(super) const EXPORT_SCHEMA: &str = "komodo.core-export/v2";

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Material {
  schema: u32,
  pub identity: recovery_state::Identity,
  pub settings: BackupSettings,
}

impl Material {
  pub fn validate(&self) -> anyhow::Result<()> {
    if self.schema != 1 {
      return Err(anyhow!(
        "Unsupported Core recovery material schema"
      ));
    }
    self.identity.validate()?;
    validate_settings(&self.settings)?;
    Ok(())
  }
}

pub(super) async fn write_material(
  root: &Path,
  settings: &BackupSettings,
) -> anyhow::Result<()> {
  let material = Material {
    schema: 1,
    identity: recovery_state::current()?.identity.clone(),
    settings: settings.clone(),
  };
  let bytes = serde_json::to_vec(&material)?;
  if bytes.len() > 4 * 1024 * 1024 {
    return Err(anyhow!("Core recovery material exceeds 4 MiB"));
  }
  let mut file = OpenOptions::new()
    .create_new(true)
    .write(true)
    .mode(0o600)
    .open(root.join(MATERIAL_FILE))?;
  file.write_all(&bytes)?;
  file.sync_all()?;
  Ok(())
}

pub(super) fn read_material(root: &Path) -> anyhow::Result<Material> {
  let path = root.join(MATERIAL_FILE);
  let metadata = std::fs::symlink_metadata(&path)
    .context("Core snapshot is missing recovery material")?;
  if !metadata.is_file() || metadata.len() > 4 * 1024 * 1024 {
    return Err(anyhow!(
      "Core recovery material must be a regular file of at most 4 MiB"
    ));
  }
  let material: Material =
    serde_json::from_slice(&std::fs::read(path)?)
      .context("Invalid Core recovery material")?;
  material.validate()?;
  Ok(material)
}

pub(super) fn validate_snapshot_material(
  root: &Path,
  snapshot: &BackupSnapshot,
) -> anyhow::Result<(Material, i64)> {
  let material = read_material(root)?;
  let (source, expected_digest, created_at) =
    crypto::authenticate_core_source_label_with_key(
      &snapshot.source_label,
      &snapshot.hostname,
      &snapshot.name,
      &material.identity.key,
    )?;
  if source
    != BackupTarget::Core
      .source_label(&material.identity.core_instance_id)
    || snapshot.hostname
      != format!("komodo-core-{}", material.identity.core_instance_id)
  {
    return Err(anyhow!(
      "Core recovery identity does not match the snapshot"
    ));
  }
  if core_export_digest(root)? != expected_digest {
    return Err(anyhow!(
      "Core snapshot contents do not match their recorded digest; recovery blocked"
    ));
  }
  Ok((material, created_at))
}

pub(super) async fn repository(
  provided: Option<BackupRepository>,
) -> anyhow::Result<BackupRepository> {
  let repository = match provided {
    Some(repository) => repository,
    None => get_settings().await?.primary,
  };
  // Recovery only opens an existing repository. Worker credentials are not
  // needed because nothing is dispatched to Periphery.
  validate_settings(&BackupSettings {
    primary: repository.clone(),
    ..Default::default()
  })?;
  Ok(repository)
}

pub(super) async fn snapshots(
  repository: BackupRepository,
) -> anyhow::Result<(Vec<BackupSnapshot>, u64)> {
  let permit = snapshot_inventory_slots()
    .clone()
    .try_acquire_owned()
    .context("Another snapshot inventory request is still running")?;
  let deadline =
    std::time::Instant::now() + std::time::Duration::from_secs(60);
  let ((snapshots, hidden), _permit) =
    run_snapshot_inventory_worker(permit, deadline, move || {
      let inventory =
        core_repository(&repository, &BackupSettings::default())?
          .list_snapshots()?;
      let snapshots = inventory
        .snapshots
        .into_iter()
        .filter_map(|mut snapshot| {
          // These are candidates, not proof of provenance. The selected export is
          // fully validated with its embedded recovery material during planning.
          if !snapshot
            .source_label
            .starts_with("komodo-core-auth/v3/")
            || !snapshot.hostname.starts_with("komodo-core-")
          {
            return None;
          }
          snapshot.target = BackupTarget::Core;
          Some(snapshot)
        })
        .collect();
      Ok((snapshots, inventory.hidden))
    })
    .await?;
  Ok((snapshots, hidden))
}

pub(super) fn configure_source(
  material: &mut Material,
  mut source: BackupRepository,
) -> anyhow::Result<()> {
  let previous = std::iter::once(&material.settings.primary)
    .chain(material.settings.mirror.iter())
    .find(|repository| {
      repositories_share_location(repository, &source)
        .unwrap_or(false)
    });
  if let Some(previous) = previous {
    // Preserve only credentials belonging to the same storage location.
    merge_repository_secrets(&mut source, previous, true, false)?;
  }
  if material
    .settings
    .mirror
    .as_ref()
    .map(|mirror| repositories_share_location(mirror, &source))
    .transpose()?
    .unwrap_or(false)
  {
    material.settings.mirror = None;
  }
  material.settings.primary = source;
  material.settings.enabled = false;
  material.settings.trusted_workers.clear();
  material.settings.updated_at = komodo_timestamp();
  material.validate()
}

pub(super) async fn save_settings(
  database: &database::mungos::mongodb::Database,
  material: &Material,
) -> anyhow::Result<()> {
  let record = SealedBackupSettings {
    id: SETTINGS_ID.into(),
    sealed: crypto::seal_with_key(
      &serde_json::to_vec(&material.settings)?,
      &material.identity.key,
    )?,
    updated_at: material.settings.updated_at,
    primary_initialized: true,
    mirror_initialized: false,
  };
  database
    .collection::<SealedBackupSettings>(SETTINGS_COLLECTION)
    .replace_one(doc! { "_id": SETTINGS_ID }, record)
    .upsert(true)
    .await?;
  database
    .collection::<RepositoryHealthRecord>(HEALTH_COLLECTION)
    .delete_many(doc! {})
    .await?;
  Ok(())
}

pub(super) fn unseal_material(
  value: &str,
) -> anyhow::Result<Material> {
  let material: Material =
    serde_json::from_slice(&crypto::open(value)?)?;
  material.validate()?;
  Ok(material)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn recovery_uses_the_embedded_key_and_rejects_changed_exports() {
    let root = tempfile::tempdir().unwrap();
    let material = Material {
      schema: 1,
      identity: recovery_state::Identity {
        key: [7; 32],
        core_instance_id: "b".repeat(32),
      },
      settings: BackupSettings::default(),
    };
    std::fs::write(
      root.path().join(MATERIAL_FILE),
      serde_json::to_vec(&material).unwrap(),
    )
    .unwrap();
    std::fs::write(root.path().join("User.gz"), b"database export")
      .unwrap();
    let mut snapshot = BackupSnapshot {
      name: "complete-core".into(),
      hostname: format!(
        "komodo-core-{}",
        material.identity.core_instance_id
      ),
      ..Default::default()
    };
    snapshot.source_label =
      crypto::authorize_core_source_label_with_key(
        &BackupTarget::Core
          .source_label(&material.identity.core_instance_id),
        &snapshot.hostname,
        &snapshot.name,
        &core_export_digest(root.path()).unwrap(),
        100,
        &material.identity.key,
      )
      .unwrap();
    // This path needs no current installation key or configured repository.
    let (recovered, created_at) =
      validate_snapshot_material(root.path(), &snapshot).unwrap();
    assert_eq!(recovered.identity.key, [7; 32]);
    assert_eq!(created_at, 100);
    std::fs::write(root.path().join("User.gz"), b"changed database")
      .unwrap();
    assert!(
      validate_snapshot_material(root.path(), &snapshot).is_err()
    );
    std::fs::write(root.path().join("User.gz"), b"database export")
      .unwrap();
    snapshot.name = "relabelled-core".into();
    assert!(
      validate_snapshot_material(root.path(), &snapshot).is_err()
    );
  }

  #[test]
  fn recovery_pauses_scheduling_and_adopts_the_supplied_repository() {
    let mut material = Material {
      schema: 1,
      identity: recovery_state::Identity {
        key: [9; 32],
        core_instance_id: "a".repeat(32),
      },
      settings: BackupSettings {
        enabled: true,
        ..Default::default()
      },
    };
    material.settings.trusted_workers.push(
      komodo_client::entities::backup::BackupTrustedWorker {
        server_id: "old-server".into(),
        address: "https://old-worker".into(),
        public_key: "old-key".into(),
      },
    );
    let source = BackupRepository {
      backend: BackupRepositoryBackend::CoreLocal {
        path: "/surviving-mirror".into(),
      },
      passphrase: BackupSecret {
        value: "recovery-passphrase".into(),
        configured: false,
      },
      ..Default::default()
    };
    configure_source(&mut material, source.clone()).unwrap();
    assert_eq!(material.settings.primary, source);
    assert!(!material.settings.enabled);
    assert!(material.settings.trusted_workers.is_empty());
  }

  #[test]
  fn missing_or_symlinked_recovery_material_is_rejected() {
    let root = tempfile::tempdir().unwrap();
    assert!(read_material(root.path()).is_err());
    let outside = root.path().join("outside.json");
    std::fs::write(&outside, b"{}").unwrap();
    std::os::unix::fs::symlink(
      outside,
      root.path().join(MATERIAL_FILE),
    )
    .unwrap();
    assert!(read_material(root.path()).is_err());
  }
}
