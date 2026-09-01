//! Komodo's deliberately small, pinned integration surface for Vykar.
//!
//! Snapshot inventory is never cached as authority here: every list/tree
//! operation opens the selected repository and reads Vykar metadata.

use std::{
  collections::{BTreeMap, HashMap, HashSet},
  io::Write,
  os::unix::fs::PermissionsExt,
  path::Path,
};

use anyhow::{Context, anyhow};
use komodo_client::entities::backup::{
  BackupAdvancedSettings, BackupRepository, BackupRepositoryBackend,
  BackupSnapshot, BackupSnapshotItem, BackupTarget,
};
use tempfile::NamedTempFile;
use vykar_core::{
  commands,
  compress::Compression,
  config::{EncryptionModeConfig, VykarConfig},
  snapshot::item::ItemType,
};

/// A configured repository plus temporary secret material kept alive for the
/// duration of an operation.
pub struct VykarRepository {
  pub config: VykarConfig,
  pub passphrase: String,
  _sftp_key: Option<NamedTempFile>,
}

impl VykarRepository {
  /// Build an encrypted Vykar config. Callers must unseal secrets first.
  pub fn new(
    repository: &BackupRepository,
    hostname: &str,
    cache_dir: &Path,
    advanced: &BackupAdvancedSettings,
  ) -> anyhow::Result<Self> {
    let passphrase = repository.passphrase.value.trim().to_string();
    if passphrase.is_empty() {
      return Err(anyhow!(
        "Repository encryption passphrase is required"
      ));
    }

    let mut config = VykarConfig::default();
    config.encryption.mode = EncryptionModeConfig::Auto;
    config.encryption.passphrase = Some(passphrase.clone());
    config.hostname_override = Some(hostname.to_string());
    config.cache_dir = Some(cache_dir.to_string_lossy().into_owned());
    config.compact.threshold =
      advanced.compact_threshold_percent as f64;
    config.check.full_every =
      Some(format!("{}d", advanced.full_verify_every_days.max(1)));
    config.limits.connections =
      advanced.node_concurrency.clamp(1, 16) as usize;
    config.limits.upload_mib_per_sec =
      if advanced.upload_bytes_per_second == 0 {
        0
      } else {
        advanced.upload_bytes_per_second.div_ceil(1024 * 1024)
      };

    let mut sftp_key = None;
    match &repository.backend {
      BackupRepositoryBackend::CoreLocal { path } => {
        if path.trim().is_empty() {
          return Err(anyhow!(
            "Core-local repository path is required"
          ));
        }
        config.repository.url = path.clone();
      }
      BackupRepositoryBackend::S3 {
        url,
        region,
        access_key_id,
        secret_access_key,
        soft_delete,
      } => {
        config.repository.url = url.clone();
        config.repository.region = Some(region.clone());
        config.repository.access_key_id =
          required_secret("S3 access key", &access_key_id.value)?;
        config.repository.secret_access_key = required_secret(
          "S3 secret access key",
          &secret_access_key.value,
        )?;
        config.repository.s3_soft_delete = *soft_delete;
      }
      BackupRepositoryBackend::Sftp {
        url,
        private_key,
        known_hosts,
        timeout_seconds,
      } => {
        config.repository.url = url.clone();
        let mut key = NamedTempFile::new_in(cache_dir)
          .context("Failed to create protected SFTP key file")?;
        key
          .as_file()
          .set_permissions(std::fs::Permissions::from_mode(0o600))
          .context("Failed to protect SFTP key file")?;
        key
          .write_all(private_key.value.as_bytes())
          .context("Failed to materialize SFTP private key")?;
        key.flush().context("Failed to flush SFTP private key")?;
        config.repository.sftp_key =
          Some(key.path().to_string_lossy().into_owned());
        config.repository.sftp_known_hosts =
          Some(known_hosts.clone());
        config.repository.sftp_timeout = Some(*timeout_seconds);
        sftp_key = Some(key);
      }
      BackupRepositoryBackend::Rest {
        url,
        access_token,
        allow_insecure_http,
      } => {
        config.repository.url = url.clone();
        config.repository.access_token =
          required_secret("REST access token", &access_token.value)?;
        config.repository.allow_insecure_http = *allow_insecure_http;
      }
    }

    Ok(Self {
      config,
      passphrase,
      _sftp_key: sftp_key,
    })
  }

  pub fn init(&self) -> anyhow::Result<()> {
    commands::init::run(&self.config, Some(&self.passphrase))
      .map(|_| ())
      .map_err(Into::into)
  }

  pub fn backup(
    &self,
    snapshot_name: &str,
    source_label: &str,
    source_paths: &[String],
  ) -> anyhow::Result<BackupResult> {
    let outcome = commands::backup::run(
      &self.config,
      commands::backup::BackupRequest {
        snapshot_name,
        passphrase: Some(&self.passphrase),
        source_paths,
        source_label,
        exclude_patterns: &[],
        exclude_if_present: &[],
        one_file_system: false,
        git_ignore: false,
        xattrs_enabled: true,
        compression: Compression::Lz4,
        command_dumps: &[],
        verbose: false,
      },
    )?;
    Ok(BackupResult {
      partial: outcome.is_partial || outcome.stats.errors > 0,
      files: outcome.stats.nfiles,
      original_size: outcome.stats.original_size,
      stored_size: outcome.stats.deduplicated_size,
    })
  }

  /// List snapshots directly from the repository. Unknown labels remain
  /// visible to administrators as unbound snapshots.
  pub fn list_snapshots(&self) -> anyhow::Result<SnapshotInventory> {
    let listing = commands::list::list_snapshots_with_stats(
      &self.config,
      Some(&self.passphrase),
    )?;
    let snapshots = listing
      .snapshots
      .into_iter()
      .map(|(entry, stats)| {
        let target = parse_source_label(&entry.source_label);
        let stats = stats.unwrap_or_default();
        BackupSnapshot {
          name: entry.name.clone(),
          source_label: entry.source_label,
          hostname: entry.hostname,
          target,
          source_paths: entry.source_paths,
          created_at: entry.time.timestamp_millis(),
          original_size: stats.original_size,
          stored_size: stats.deduplicated_size,
          file_count: stats.nfiles,
          partial: stats.errors > 0,
          run_id: run_id_from_snapshot_name(&entry.name),
          manifest_checksum: String::new(),
        }
      })
      .collect();
    Ok(SnapshotInventory {
      snapshots,
      hidden: listing.hidden.len() as u64,
    })
  }

  /// Return only immediate children of `parent`, ready for a lazy tree.
  pub fn list_directory(
    &self,
    snapshot_name: &str,
    parent: &str,
    search: &str,
    page: u64,
    limit: u64,
  ) -> anyhow::Result<SnapshotDirectoryPage> {
    let (items, source_paths) =
      commands::list::list_snapshot_items_with_source_paths(
        &self.config,
        Some(&self.passphrase),
        snapshot_name,
      )?;
    let manifest_roots = source_paths
      .into_iter()
      .filter(|path| {
        Path::new(path)
          .file_name()
          .and_then(|name| name.to_str())
          .and_then(|name| {
            name.strip_prefix("komodo-backup-manifest-")
          })
          .is_some_and(|suffix| {
            suffix.len() == 6
              && suffix
                .chars()
                .all(|character| character.is_ascii_alphanumeric())
          })
      })
      .map(|path| path.trim_matches('/').to_string())
      .collect::<HashSet<_>>();
    Ok(build_directory_page(
      items
        .into_iter()
        .filter(|item| {
          let item_path = item.path.trim_matches('/');
          !manifest_roots.iter().any(|root| {
            item_path == root
              || item_path.starts_with(&format!("{root}/"))
          })
        })
        .map(|item| ItemView {
          path: item.path,
          directory: item.entry_type == ItemType::Directory,
          size: item.size,
          modified_at: item.mtime / 1_000_000,
        }),
      parent,
      search,
      page,
      limit,
    ))
  }

  pub fn restore(
    &self,
    snapshot_name: &str,
    destination: &Path,
    selected_paths: &[String],
  ) -> anyhow::Result<()> {
    let destination = destination
      .to_str()
      .context("Restore destination is not valid UTF-8")?;
    if selected_paths.is_empty() {
      commands::restore::run(
        &self.config,
        Some(&self.passphrase),
        snapshot_name,
        destination,
        None,
        true,
        true,
      )?;
    } else {
      let selected: HashSet<String> =
        normalize_selected_paths(selected_paths)?
          .into_iter()
          .collect();
      commands::restore::run_selected(
        &self.config,
        Some(&self.passphrase),
        snapshot_name,
        destination,
        &selected,
        true,
        true,
      )?;
    }
    Ok(())
  }

  /// List every materialized snapshot path for restore preflight without
  /// downloading pack contents.
  pub fn snapshot_paths(
    &self,
    snapshot_name: &str,
    selected_paths: &[String],
  ) -> anyhow::Result<Vec<SnapshotPath>> {
    let (items, _) =
      commands::list::list_snapshot_items_with_source_paths(
        &self.config,
        Some(&self.passphrase),
        snapshot_name,
      )?;
    let selected = normalize_selected_paths(selected_paths)?;
    Ok(
      items
        .into_iter()
        .filter_map(|item| {
          let path = item.path.trim_matches('/').to_string();
          let included = selected.is_empty()
            || selected.iter().any(|selection| {
              path == *selection
                || path.starts_with(&format!("{selection}/"))
            });
          included.then_some(SnapshotPath {
            path,
            directory: item.entry_type == ItemType::Directory,
          })
        })
        .collect(),
    )
  }

  /// Apply retention per logical source, preserving partial snapshots in
  /// addition to the requested number of complete snapshots. All deletions
  /// are committed in one Vykar maintenance transaction.
  pub fn prune_complete_snapshots(
    &self,
    keep_last_by_label: &HashMap<String, u64>,
  ) -> anyhow::Result<PruneResult> {
    let inventory = self.list_snapshots()?;
    if inventory.hidden > 0 {
      return Err(anyhow!(
        "Repository inventory is incomplete; destructive maintenance is blocked"
      ));
    }
    let names =
      retention_deletions(&inventory.snapshots, keep_last_by_label);
    if names.is_empty() {
      return Ok(PruneResult::default());
    }
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let result = commands::delete::run(
      &self.config,
      Some(&self.passphrase),
      &refs,
      false,
      None,
    )?;
    Ok(PruneResult {
      snapshots_deleted: result.stats.len() as u64,
      warnings: result.warnings,
    })
  }

  /// Compact exactly once after a batch prune.
  pub fn compact(
    &self,
    threshold_percent: u64,
    max_repack_bytes: Option<u64>,
  ) -> anyhow::Result<CompactResult> {
    let stats = commands::compact::run(
      &self.config,
      Some(&self.passphrase),
      threshold_percent as f64,
      max_repack_bytes,
      false,
      None,
    )?;
    if stats.packs_corrupt > 0 {
      return Err(anyhow!(
        "Compaction found {} corrupt pack(s)",
        stats.packs_corrupt
      ));
    }
    Ok(CompactResult {
      packs_repacked: stats.packs_repacked,
      bytes_freed: stats.space_freed,
    })
  }

  pub fn verify(
    &self,
    full: bool,
    sample_percent: u64,
  ) -> anyhow::Result<VerifyResult> {
    let percent = if full {
      100
    } else {
      sample_percent.clamp(1, 100) as u8
    };
    let result = commands::check::run_with_progress(
      &self.config,
      Some(&self.passphrase),
      true,
      false,
      None,
      percent,
      true,
    )?;
    Ok(VerifyResult {
      full,
      snapshots_checked: result.snapshots_checked as u64,
      errors: result
        .errors
        .into_iter()
        .map(|error| format!("{error:?}"))
        .collect(),
    })
  }
}

fn required_secret(
  name: &str,
  value: &str,
) -> anyhow::Result<Option<String>> {
  if value.trim().is_empty() {
    Err(anyhow!("{name} is required"))
  } else {
    Ok(Some(value.to_string()))
  }
}

#[derive(Debug, Clone, Default)]
pub struct BackupResult {
  pub partial: bool,
  pub files: u64,
  pub original_size: u64,
  pub stored_size: u64,
}

#[derive(Debug, Clone, Default)]
pub struct SnapshotInventory {
  pub snapshots: Vec<BackupSnapshot>,
  /// Snapshots Vykar could not decode. A non-zero value blocks maintenance.
  pub hidden: u64,
}

#[derive(Debug, Clone, Default)]
pub struct SnapshotDirectoryPage {
  pub entries: Vec<BackupSnapshotItem>,
  pub total: u64,
  pub page: u64,
  pub has_more: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SnapshotPath {
  pub path: String,
  pub directory: bool,
}

#[derive(Debug, Clone, Default)]
pub struct PruneResult {
  pub snapshots_deleted: u64,
  pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct CompactResult {
  pub packs_repacked: u64,
  pub bytes_freed: u64,
}

#[derive(Debug, Clone, Default)]
pub struct VerifyResult {
  pub full: bool,
  pub snapshots_checked: u64,
  pub errors: Vec<String>,
}

#[derive(Debug, Clone)]
struct ItemView {
  path: String,
  directory: bool,
  size: u64,
  modified_at: i64,
}

fn build_directory_page(
  items: impl IntoIterator<Item = ItemView>,
  parent: &str,
  search: &str,
  page: u64,
  limit: u64,
) -> SnapshotDirectoryPage {
  let parent = parent.trim_matches('/');
  let search = search.to_lowercase();
  let mut children: BTreeMap<String, BackupSnapshotItem> =
    BTreeMap::new();
  for item in items {
    let path = item.path.trim_matches('/');
    let relative = if parent.is_empty() {
      path
    } else if path == parent {
      continue;
    } else if let Some(relative) =
      path.strip_prefix(&format!("{parent}/"))
    {
      relative
    } else {
      continue;
    };
    if relative.is_empty() {
      continue;
    }
    let (name, remainder) = relative
      .split_once('/')
      .map_or((relative, None), |(name, rest)| (name, Some(rest)));
    if !search.is_empty() && !path.to_lowercase().contains(&search) {
      continue;
    }
    let child_path = if parent.is_empty() {
      name.to_string()
    } else {
      format!("{parent}/{name}")
    };
    let entry =
      children.entry(child_path.clone()).or_insert_with(|| {
        BackupSnapshotItem {
          path: child_path,
          name: name.to_string(),
          directory: remainder.is_some() || item.directory,
          size: item.size,
          modified_at: item.modified_at,
          has_children: remainder.is_some(),
        }
      });
    if remainder.is_some() {
      entry.directory = true;
      entry.has_children = true;
    }
  }
  let mut entries: Vec<_> = children.into_values().collect();
  entries.sort_by(|a, b| {
    (!a.directory, a.name.to_lowercase())
      .cmp(&(!b.directory, b.name.to_lowercase()))
  });
  let total = entries.len() as u64;
  let limit = limit.clamp(1, 500);
  let start = page.saturating_mul(limit) as usize;
  let entries = entries
    .into_iter()
    .skip(start)
    .take(limit as usize)
    .collect::<Vec<_>>();
  SnapshotDirectoryPage {
    has_more: (start as u64 + entries.len() as u64) < total,
    entries,
    total,
    page,
  }
}

/// Remove children when an ancestor is selected and reject unsafe paths.
pub fn normalize_selected_paths(
  selected: &[String],
) -> anyhow::Result<Vec<String>> {
  let mut paths = Vec::with_capacity(selected.len());
  for selected in selected {
    let normalized = selected.trim_matches('/');
    if normalized.is_empty()
      || normalized == "."
      || normalized == ".."
      || normalized.starts_with("../")
      || normalized.contains("/../")
    {
      return Err(anyhow!("Unsafe snapshot path: {selected}"));
    }
    paths.push(normalized.to_string());
  }
  paths.sort();
  paths.dedup();
  let mut result: Vec<String> = Vec::new();
  for path in paths {
    if result.iter().any(|parent| {
      path == *parent || path.starts_with(&format!("{parent}/"))
    }) {
      continue;
    }
    result.push(path);
  }
  Ok(result)
}

fn retention_deletions(
  snapshots: &[BackupSnapshot],
  keep_last_by_label: &HashMap<String, u64>,
) -> Vec<String> {
  let mut by_label: HashMap<&str, Vec<&BackupSnapshot>> =
    HashMap::new();
  for snapshot in snapshots {
    by_label
      .entry(&snapshot.source_label)
      .or_default()
      .push(snapshot);
  }
  let mut delete = Vec::new();
  for (label, mut snapshots) in by_label {
    let Some(keep_last) = keep_last_by_label.get(label) else {
      continue;
    };
    snapshots
      .sort_by_key(|snapshot| std::cmp::Reverse(snapshot.created_at));
    let mut complete_seen = 0_u64;
    let mut partial_seen = 0_u64;
    for snapshot in snapshots {
      let keep = if snapshot.partial {
        partial_seen += 1;
        // Retain the newest partial snapshot for diagnosis. It is additional
        // to, and never consumes, the complete retention budget.
        partial_seen == 1
      } else {
        complete_seen += 1;
        complete_seen <= (*keep_last).max(1)
      };
      if !keep {
        delete.push(snapshot.name.clone());
      }
    }
  }
  delete
}

pub fn parse_source_label(label: &str) -> BackupTarget {
  let parts: Vec<_> = label.split('/').collect();
  match parts.as_slice() {
    ["komodo", "v1", "core", _] => BackupTarget::Core,
    ["komodo", "v1", "stack", stack_id] => BackupTarget::Stack {
      stack_id: (*stack_id).into(),
    },
    ["komodo", "v1", "volume", server_id, volume_name] => {
      BackupTarget::Volume {
        server_id: (*server_id).into(),
        volume_name: (*volume_name).into(),
      }
    }
    _ => BackupTarget::Unbound {
      source_label: label.into(),
    },
  }
}

fn run_id_from_snapshot_name(name: &str) -> String {
  name
    .rsplit_once('-')
    .map(|(_, suffix)| suffix.to_string())
    .unwrap_or_default()
}

/// Unique, sortable snapshot name. Vykar requires names to be unique.
pub fn snapshot_name(prefix: &str) -> String {
  format!(
    "{prefix}-{}-{}",
    chrono::Utc::now().format("%Y%m%dT%H%M%SZ"),
    uuid::Uuid::new_v4().simple()
  )
}

#[cfg(test)]
mod tests {
  use super::*;
  use komodo_client::entities::backup::{
    BackupAdvancedSettings, BackupRepository,
    BackupRepositoryBackend, BackupSecret,
  };

  fn snapshot(
    name: &str,
    created_at: i64,
    partial: bool,
  ) -> BackupSnapshot {
    BackupSnapshot {
      name: name.into(),
      source_label: "komodo/v1/stack/s1".into(),
      source_paths: Vec::new(),
      created_at,
      partial,
      target: BackupTarget::Stack {
        stack_id: "s1".into(),
      },
      ..Default::default()
    }
  }

  #[test]
  fn partial_snapshots_do_not_displace_complete_retention() {
    let snapshots = vec![
      snapshot("complete-new", 4, false),
      snapshot("partial-new", 3, true),
      snapshot("complete-old", 2, false),
      snapshot("partial-old", 1, true),
    ];
    let keep = HashMap::from([("komodo/v1/stack/s1".into(), 1)]);
    let delete = retention_deletions(&snapshots, &keep);
    assert_eq!(delete, vec!["complete-old", "partial-old"]);
  }

  #[test]
  fn selected_paths_are_safe_and_deduplicated() {
    assert_eq!(
      normalize_selected_paths(&[
        "data/logs".into(),
        "data".into(),
        "config/app".into(),
      ])
      .unwrap(),
      vec!["config/app", "data"]
    );
    assert!(normalize_selected_paths(&["../etc".into()]).is_err());
  }

  #[test]
  fn tree_is_lazy_and_collapsed() {
    let page = build_directory_page(
      [
        ItemView {
          path: "etc/app/config.toml".into(),
          directory: false,
          size: 10,
          modified_at: 1,
        },
        ItemView {
          path: "var/data".into(),
          directory: true,
          size: 0,
          modified_at: 2,
        },
      ],
      "",
      "",
      0,
      100,
    );
    assert_eq!(page.entries.len(), 2);
    assert!(page.entries.iter().any(|entry| {
      entry.name == "etc" && entry.directory && entry.has_children
    }));
    let children = build_directory_page(
      [
        ItemView {
          path: "etc/app/config.toml".into(),
          directory: false,
          size: 10,
          modified_at: 1,
        },
        ItemView {
          path: "etcetera/not-a-child".into(),
          directory: false,
          size: 10,
          modified_at: 1,
        },
      ],
      "etc",
      "",
      0,
      100,
    );
    assert_eq!(children.entries.len(), 1);
    assert_eq!(children.entries[0].name, "app");
  }

  fn local_repository(path: &Path) -> BackupRepository {
    BackupRepository {
      name: "test".into(),
      backend: BackupRepositoryBackend::CoreLocal {
        path: path.to_string_lossy().into_owned(),
      },
      passphrase: BackupSecret {
        value: "correct horse battery staple".into(),
        configured: false,
      },
    }
  }

  fn exercise_repository(repository: BackupRepository) {
    let source = tempfile::tempdir().unwrap();
    std::fs::create_dir(source.path().join("folder")).unwrap();
    std::fs::write(source.path().join("folder/keep.txt"), b"keep")
      .unwrap();
    std::fs::write(source.path().join("delete.txt"), b"delete")
      .unwrap();
    let cache = tempfile::tempdir().unwrap();
    let vykar = VykarRepository::new(
      &repository,
      "komodo-test-host",
      cache.path(),
      &BackupAdvancedSettings::default(),
    )
    .unwrap();
    vykar.init().unwrap();
    let source_path = source.path().to_string_lossy().into_owned();
    vykar
      .backup(
        "snapshot-one",
        "komodo/v1/stack/test",
        std::slice::from_ref(&source_path),
      )
      .unwrap();
    std::fs::write(source.path().join("folder/keep.txt"), b"new")
      .unwrap();
    vykar
      .backup("snapshot-two", "komodo/v1/stack/test", &[source_path])
      .unwrap();

    let inventory = vykar.list_snapshots().unwrap();
    assert_eq!(inventory.hidden, 0);
    assert_eq!(inventory.snapshots.len(), 2);
    let paths = vykar.snapshot_paths("snapshot-one", &[]).unwrap();
    let selected = paths
      .iter()
      .find(|item| item.path.ends_with("folder/keep.txt"))
      .unwrap()
      .path
      .clone();
    let restore = tempfile::tempdir().unwrap();
    let destination = restore.path().join("selected");
    vykar
      .restore(
        "snapshot-one",
        &destination,
        std::slice::from_ref(&selected),
      )
      .unwrap();
    assert_eq!(
      std::fs::read(destination.join(selected)).unwrap(),
      b"keep"
    );

    let pruned = vykar
      .prune_complete_snapshots(&HashMap::from([(
        "komodo/v1/stack/test".into(),
        1,
      )]))
      .unwrap();
    assert_eq!(pruned.snapshots_deleted, 1);
    assert_eq!(vykar.list_snapshots().unwrap().snapshots.len(), 1);
    vykar.compact(1, None).unwrap();
    assert!(vykar.verify(false, 5).unwrap().errors.is_empty());
  }

  #[test]
  fn encrypted_local_repository_lifecycle() {
    let repository = tempfile::tempdir().unwrap();
    exercise_repository(local_repository(repository.path()));
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn encrypted_rest_repository_lifecycle() {
    let data = tempfile::tempdir().unwrap();
    let token = "scoped-test-token";
    let state = vykar_server::state::AppState::new(
      vykar_server::config::ServerSection {
        listen: String::new(),
        data_dir: data.path().to_string_lossy().into_owned(),
        token: token.into(),
        append_only: false,
        log_format: "json".into(),
      },
      None,
    );
    let listener =
      tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
      axum::serve(listener, vykar_server::handlers::router(state))
        .await
        .unwrap();
    });
    let repository = BackupRepository {
      name: "rest".into(),
      backend: BackupRepositoryBackend::Rest {
        url: format!("http://{address}"),
        access_token: BackupSecret {
          value: token.into(),
          configured: false,
        },
        allow_insecure_http: true,
      },
      passphrase: BackupSecret {
        value: "correct horse battery staple".into(),
        configured: false,
      },
    };
    tokio::task::spawn_blocking(move || {
      exercise_repository(repository)
    })
    .await
    .unwrap();
    server.abort();
  }
}
