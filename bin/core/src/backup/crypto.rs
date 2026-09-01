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
use rand::RngExt as _;

const BACKUP_KEY_PATH: &str = "/data/keys/backup.key";
const LEGACY_BACKUP_KEY_PATH: &str = "/config/keys/backup.key";
const AAD: &[u8] = b"komodo-backup-settings/v1";

fn backup_key() -> anyhow::Result<&'static [u8; 32]> {
  static KEY: OnceLock<[u8; 32]> = OnceLock::new();
  if let Some(key) = KEY.get() {
    return Ok(key);
  }
  let key = load_or_create_key(
    Path::new(BACKUP_KEY_PATH),
    Path::new(LEGACY_BACKUP_KEY_PATH),
  )?;
  let _ = KEY.set(key);
  KEY.get().context("Failed to initialize backup sealing key")
}

fn load_or_create_key(
  path: &Path,
  legacy_path: &Path,
) -> anyhow::Result<[u8; 32]> {
  if let Some(key) = read_key(path)? {
    return Ok(key);
  }
  if let Some(key) = read_key(legacy_path)? {
    return persist_key(path, key);
  }
  let mut key = [0_u8; 32];
  rand::rng().fill(&mut key);
  persist_key(path, key)
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

    let actual = load_or_create_key(&path, &legacy_path).unwrap();

    assert_eq!(actual, expected);
    assert_eq!(read_key(&path).unwrap(), Some(expected));
  }
}
