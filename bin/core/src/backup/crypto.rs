use std::{
  fs::OpenOptions, io::Write, os::unix::fs::OpenOptionsExt,
  path::Path, sync::OnceLock,
};

use anyhow::{Context, anyhow};
use chacha20poly1305::{
  XChaCha20Poly1305, XNonce,
  aead::{Aead, KeyInit},
};
use data_encoding::BASE64URL_NOPAD;
use hmac::{Hmac, KeyInit as HmacKeyInit, Mac};
use rand::RngExt as _;
use sha2::Sha256;

const BACKUP_KEY_PATH: &str = "/core-secrets/backup.key";
const LEGACY_SHARED_BACKUP_KEY_PATH: &str = "/data/keys/backup.key";
const LEGACY_CONFIG_BACKUP_KEY_PATH: &str = "/config/keys/backup.key";
const AAD: &[u8] = b"komodo-backup-settings/v1";
const SOURCE_AUTH_PREFIX: &str = "komodo-auth/v1";
const SOURCE_AUTH_CONTEXT: &[u8] = b"komodo-backup-source/v1";
const CORE_SOURCE_AUTH_PREFIX: &str = "komodo-core-auth/v3";
const CORE_SOURCE_AUTH_CONTEXT: &[u8] =
  b"komodo-core-export-content-and-time/v3";

type HmacSha256 = Hmac<Sha256>;

fn backup_key() -> anyhow::Result<&'static [u8; 32]> {
  static KEY: OnceLock<[u8; 32]> = OnceLock::new();
  if let Some(key) = KEY.get() {
    return Ok(key);
  }
  let key = load_or_create_key(
    Path::new(BACKUP_KEY_PATH),
    &[
      (Path::new(LEGACY_SHARED_BACKUP_KEY_PATH), true),
      (Path::new(LEGACY_CONFIG_BACKUP_KEY_PATH), false),
    ],
  )?;
  let _ = KEY.set(key);
  KEY.get().context("Failed to initialize backup sealing key")
}

fn load_or_create_key(
  path: &Path,
  legacy_paths: &[(&Path, bool)],
) -> anyhow::Result<[u8; 32]> {
  if let Some(key) = read_key(path)? {
    // Complete cleanup if a prior migration durably wrote the Core-only key
    // but exited before deleting a legacy copy from shared storage.
    for (legacy_path, remove_after_migration) in legacy_paths {
      if *remove_after_migration {
        remove_legacy_key(legacy_path)?;
      }
    }
    return Ok(key);
  }
  for (legacy_path, remove_after_migration) in legacy_paths {
    if let Some(key) = read_key(legacy_path)? {
      let key = persist_key(path, key)?;
      if *remove_after_migration {
        remove_legacy_key(legacy_path)?;
      }
      return Ok(key);
    }
  }
  let mut key = [0_u8; 32];
  rand::rng().fill(&mut key);
  persist_key(path, key)
}

fn remove_legacy_key(path: &Path) -> anyhow::Result<()> {
  match std::fs::remove_file(path) {
    Ok(()) => {}
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
      return Ok(());
    }
    Err(error) => {
      return Err(error).with_context(|| {
        format!(
          "Failed to remove legacy backup key at {}",
          path.display()
        )
      });
    }
  }
  if let Some(parent) = path.parent() {
    std::fs::File::open(parent)?.sync_all()?;
  }
  Ok(())
}

fn read_key(path: &Path) -> anyhow::Result<Option<[u8; 32]>> {
  let value = match std::fs::read_to_string(path) {
    Ok(value) => value,
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
      return Ok(None);
    }
    Err(error) => return Err(error.into()),
  };
  let bytes = hex::decode(value.trim())
    .context("Backup sealing key is not valid hex")?;
  bytes.try_into().map(Some).map_err(|_| {
    anyhow!("Backup sealing key must contain exactly 32 bytes")
  })
}

fn persist_key(
  path: &Path,
  key: [u8; 32],
) -> anyhow::Result<[u8; 32]> {
  let parent =
    path.parent().context("Backup key path has no parent")?;
  std::fs::create_dir_all(parent).with_context(|| {
    format!(
      "Failed to create backup key directory {}",
      parent.display()
    )
  })?;
  let file = OpenOptions::new()
    .create_new(true)
    .write(true)
    .mode(0o600)
    .open(path);
  let mut file = match file {
    Ok(file) => file,
    Err(error)
      if error.kind() == std::io::ErrorKind::AlreadyExists =>
    {
      return read_key(path)?
        .context("Backup key disappeared after concurrent creation");
    }
    Err(error) => {
      return Err(error).with_context(|| {
        format!(
          "Failed to create persisted backup key at {}",
          path.display()
        )
      });
    }
  };
  file.write_all(hex::encode(key).as_bytes())?;
  file.sync_all()?;
  std::fs::File::open(parent)?.sync_all()?;
  Ok(key)
}

pub fn seal(plaintext: &[u8]) -> anyhow::Result<String> {
  seal_with_key(plaintext, backup_key()?)
}

fn seal_with_key(
  plaintext: &[u8],
  key: &[u8; 32],
) -> anyhow::Result<String> {
  let cipher = XChaCha20Poly1305::new(key.into());
  let mut nonce = [0_u8; 24];
  rand::rng().fill(&mut nonce);
  let ciphertext = cipher
    .encrypt(
      XNonce::from_slice(&nonce),
      chacha20poly1305::aead::Payload {
        msg: plaintext,
        aad: AAD,
      },
    )
    .map_err(|_| anyhow!("Failed to seal backup settings"))?;
  let mut envelope =
    Vec::with_capacity(nonce.len() + ciphertext.len());
  envelope.extend_from_slice(&nonce);
  envelope.extend_from_slice(&ciphertext);
  Ok(BASE64URL_NOPAD.encode(&envelope))
}

pub fn open(envelope: &str) -> anyhow::Result<Vec<u8>> {
  open_with_key(envelope, backup_key()?)
}

fn open_with_key(
  envelope: &str,
  key: &[u8; 32],
) -> anyhow::Result<Vec<u8>> {
  let envelope = BASE64URL_NOPAD
    .decode(envelope.as_bytes())
    .context("Backup settings envelope is not valid base64")?;
  if envelope.len() < 24 {
    return Err(anyhow!("Backup settings envelope is truncated"));
  }
  let (nonce, ciphertext) = envelope.split_at(24);
  XChaCha20Poly1305::new(key.into())
    .decrypt(
      XNonce::from_slice(nonce),
      chacha20poly1305::aead::Payload {
        msg: ciphertext,
        aad: AAD,
      },
    )
    .map_err(|_| anyhow!("Backup settings authentication failed"))
}

pub fn embedded_server_token() -> anyhow::Result<String> {
  use sha2::{Digest, Sha256};
  let mut hasher = Sha256::new();
  hasher.update(b"komodo-embedded-vykar-server/v1");
  hasher.update(backup_key()?);
  Ok(hex::encode(hasher.finalize()))
}

pub fn authorize_source_label(
  source_label: &str,
  hostname: &str,
  snapshot_name: &str,
) -> anyhow::Result<String> {
  authorize_source_label_with_key(
    source_label,
    hostname,
    snapshot_name,
    backup_key()?,
  )
}

fn authorize_source_label_with_key(
  source_label: &str,
  hostname: &str,
  snapshot_name: &str,
  key: &[u8; 32],
) -> anyhow::Result<String> {
  let encoded = BASE64URL_NOPAD.encode(source_label.as_bytes());
  let signature =
    source_label_signature(&encoded, hostname, snapshot_name, key)?;
  Ok(format!(
    "{SOURCE_AUTH_PREFIX}/{encoded}/{}",
    hex::encode(signature)
  ))
}

pub fn authenticate_source_label(
  authorized_label: &str,
  hostname: &str,
  snapshot_name: &str,
) -> anyhow::Result<String> {
  if authorized_label
    .starts_with(&format!("{CORE_SOURCE_AUTH_PREFIX}/"))
  {
    return authenticate_core_source_label(
      authorized_label,
      hostname,
      snapshot_name,
    )
    .map(|(source, _, _)| source);
  }
  authenticate_source_label_with_key(
    authorized_label,
    hostname,
    snapshot_name,
    backup_key()?,
  )
}

pub fn authorize_core_source_label(
  source: &str,
  hostname: &str,
  name: &str,
  digest: &str,
  created_at: i64,
) -> anyhow::Result<String> {
  authorize_core_source_label_with_key(
    source,
    hostname,
    name,
    digest,
    created_at,
    backup_key()?,
  )
}

fn authorize_core_source_label_with_key(
  source: &str,
  hostname: &str,
  name: &str,
  digest: &str,
  created_at: i64,
  key: &[u8; 32],
) -> anyhow::Result<String> {
  let encoded = BASE64URL_NOPAD.encode(source.as_bytes());
  let signature = core_source_mac(
    &encoded,
    hostname,
    name,
    digest,
    &created_at.to_string(),
    key,
  )?
  .finalize()
  .into_bytes();
  Ok(format!(
    "{CORE_SOURCE_AUTH_PREFIX}/{encoded}/{digest}/{created_at}/{}",
    hex::encode(signature)
  ))
}

pub fn authenticate_core_source_label(
  label: &str,
  hostname: &str,
  name: &str,
) -> anyhow::Result<(String, String, i64)> {
  authenticate_core_source_label_with_key(
    label,
    hostname,
    name,
    backup_key()?,
  )
}

fn authenticate_core_source_label_with_key(
  label: &str,
  hostname: &str,
  name: &str,
  key: &[u8; 32],
) -> anyhow::Result<(String, String, i64)> {
  let rest = label
    .strip_prefix(&format!("{CORE_SOURCE_AUTH_PREFIX}/"))
    .context("Core snapshot has no content-and-time authorization")?;
  let parts = rest.split('/').collect::<Vec<_>>();
  if parts.len() != 4
    || parts[1].len() != 64
    || !parts[1].bytes().all(|byte| byte.is_ascii_hexdigit())
  {
    return Err(anyhow!(
      "Core snapshot content authorization is malformed"
    ));
  }
  let created_at: i64 = parts[2].parse().context(
    "Core snapshot authorization has an invalid creation time",
  )?;
  if created_at <= 0 {
    return Err(anyhow!(
      "Core snapshot creation time must be positive"
    ));
  }
  let signature = hex::decode(parts[3])?;
  core_source_mac(parts[0], hostname, name, parts[1], parts[2], key)?
    .verify_slice(&signature)
    .map_err(|_| {
      anyhow!("Core snapshot content authorization is invalid")
    })?;
  let source =
    String::from_utf8(BASE64URL_NOPAD.decode(parts[0].as_bytes())?)?;
  if !source.starts_with("komodo/v1/core/") {
    return Err(anyhow!(
      "Core content authorization has a non-Core identity"
    ));
  }
  Ok((source, parts[1].to_string(), created_at))
}

fn core_source_mac(
  encoded: &str,
  hostname: &str,
  name: &str,
  digest: &str,
  created_at: &str,
  key: &[u8; 32],
) -> anyhow::Result<HmacSha256> {
  let mut mac = <HmacSha256 as HmacKeyInit>::new_from_slice(key)
    .map_err(|_| {
      anyhow!("Failed to initialize Core content authorization")
    })?;
  mac.update(CORE_SOURCE_AUTH_CONTEXT);
  for value in [encoded, hostname, name, digest, created_at] {
    mac.update(&(value.len() as u64).to_be_bytes());
    mac.update(value.as_bytes());
  }
  Ok(mac)
}

fn authenticate_source_label_with_key(
  authorized_label: &str,
  hostname: &str,
  snapshot_name: &str,
  key: &[u8; 32],
) -> anyhow::Result<String> {
  let remainder = authorized_label
    .strip_prefix(&format!("{SOURCE_AUTH_PREFIX}/"))
    .context("Snapshot source label is not Core-authorized")?;
  let (encoded, signature) = remainder
    .split_once('/')
    .context("Snapshot source authorization is malformed")?;
  if signature.contains('/') {
    return Err(anyhow!(
      "Snapshot source authorization is malformed"
    ));
  }
  let signature = hex::decode(signature)
    .context("Snapshot source authorization is not valid hex")?;
  let verifier =
    source_label_mac(encoded, hostname, snapshot_name, key)?;
  verifier.verify_slice(&signature).map_err(|_| {
    anyhow!("Snapshot source authorization is invalid")
  })?;
  let source_label = BASE64URL_NOPAD
    .decode(encoded.as_bytes())
    .context("Snapshot source identity is not valid base64")?;
  String::from_utf8(source_label)
    .context("Snapshot source identity is not valid UTF-8")
}

fn source_label_signature(
  encoded_source_label: &str,
  hostname: &str,
  snapshot_name: &str,
  key: &[u8; 32],
) -> anyhow::Result<Vec<u8>> {
  Ok(
    source_label_mac(
      encoded_source_label,
      hostname,
      snapshot_name,
      key,
    )?
    .finalize()
    .into_bytes()
    .to_vec(),
  )
}

fn source_label_mac(
  encoded_source_label: &str,
  hostname: &str,
  snapshot_name: &str,
  key: &[u8; 32],
) -> anyhow::Result<HmacSha256> {
  let mut mac = <HmacSha256 as HmacKeyInit>::new_from_slice(key)
    .map_err(|_| {
      anyhow!("Failed to initialize source authorization")
    })?;
  mac.update(SOURCE_AUTH_CONTEXT);
  for value in [encoded_source_label, hostname, snapshot_name] {
    mac.update(&(value.len() as u64).to_be_bytes());
    mac.update(value.as_bytes());
  }
  Ok(mac)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn xchacha_envelope_detects_tampering() {
    let key = [7_u8; 32];
    let sealed = seal_with_key(b"credential", &key).unwrap();
    let mut bytes =
      BASE64URL_NOPAD.decode(sealed.as_bytes()).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    assert!(
      open_with_key(&BASE64URL_NOPAD.encode(&bytes), &key).is_err()
    );
  }

  #[test]
  fn legacy_key_is_migrated_without_rotation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("data/backup.key");
    let legacy_path = directory.path().join("config/backup.key");
    std::fs::create_dir_all(legacy_path.parent().unwrap()).unwrap();
    let expected = [19_u8; 32];
    std::fs::write(&legacy_path, hex::encode(expected)).unwrap();

    let actual =
      load_or_create_key(&path, &[(&legacy_path, true)]).unwrap();

    assert_eq!(actual, expected);
    assert_eq!(read_key(&path).unwrap(), Some(expected));
    assert!(!legacy_path.exists());
  }

  #[test]
  fn completed_key_migration_cleans_up_a_shared_legacy_copy() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("core-only.key");
    let legacy_path = directory.path().join("shared.key");
    let expected = [23_u8; 32];
    std::fs::write(&path, hex::encode(expected)).unwrap();
    std::fs::write(&legacy_path, hex::encode([31_u8; 32])).unwrap();

    let actual =
      load_or_create_key(&path, &[(&legacy_path, true)]).unwrap();

    assert_eq!(actual, expected);
    assert!(!legacy_path.exists());
  }

  #[test]
  fn source_authorization_is_bound_to_writer_and_snapshot() {
    let key = [11_u8; 32];
    let label = "komodo/v1/volume/server-a/data";
    let authorized = authorize_source_label_with_key(
      label,
      "komodo-periphery-server-a",
      "volume-snapshot-a",
      &key,
    )
    .unwrap();
    assert_eq!(
      authenticate_source_label_with_key(
        &authorized,
        "komodo-periphery-server-a",
        "volume-snapshot-a",
        &key,
      )
      .unwrap(),
      label
    );
    assert!(
      authenticate_source_label_with_key(
        &authorized,
        "komodo-periphery-server-b",
        "volume-snapshot-a",
        &key,
      )
      .is_err()
    );
    assert!(
      authenticate_source_label_with_key(
        &authorized,
        "komodo-periphery-server-a",
        "volume-snapshot-b",
        &key,
      )
      .is_err()
    );
  }

  #[test]
  fn core_authorization_cannot_be_reused_for_replacement_contents() {
    let key = [13_u8; 32];
    let original = "a".repeat(64);
    let replacement = "b".repeat(64);
    let label = authorize_core_source_label_with_key(
      "komodo/v1/core/instance",
      "komodo-core-instance",
      "reused-name",
      &original,
      1_700_000_000_000,
      &key,
    )
    .unwrap();
    let (_, digest, created_at) =
      authenticate_core_source_label_with_key(
        &label,
        "komodo-core-instance",
        "reused-name",
        &key,
      )
      .unwrap();
    assert_eq!(digest, original);
    assert_eq!(created_at, 1_700_000_000_000);
    assert!(
      authenticate_core_source_label_with_key(
        &label.replace("1700000000000", "1800000000000"),
        "komodo-core-instance",
        "reused-name",
        &key,
      )
      .is_err()
    );
    assert_ne!(digest, replacement);
    assert!(
      authenticate_core_source_label_with_key(
        &label.replace(&original, &replacement),
        "komodo-core-instance",
        "reused-name",
        &key,
      )
      .is_err()
    );
    assert!(
      authenticate_core_source_label_with_key(
        &label,
        "komodo-core-instance",
        "different-name",
        &key,
      )
      .is_err()
    );
    let legacy = authorize_source_label_with_key(
      "komodo/v1/core/instance",
      "komodo-core-instance",
      "reused-name",
      &key,
    )
    .unwrap();
    assert!(
      authenticate_core_source_label_with_key(
        &legacy,
        "komodo-core-instance",
        "reused-name",
        &key,
      )
      .is_err()
    );
  }
}
