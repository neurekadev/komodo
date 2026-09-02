//! Identify the current Docker container without requiring its socket.

/// Docker mounts its hostname file from a directory named by the full
/// container ID. Unlike the hostname value, this also works with a custom
/// Compose hostname and with private cgroup namespaces.
pub fn current_container_id() -> Option<String> {
  let mounts = std::fs::read_to_string("/proc/self/mountinfo")
    .unwrap_or_default();
  let hostname =
    std::fs::read_to_string("/etc/hostname").unwrap_or_default();
  container_id(&mounts, hostname.trim())
}

fn container_id(mounts: &str, hostname: &str) -> Option<String> {
  for line in mounts.lines() {
    let fields = line.split_whitespace().collect::<Vec<_>>();
    if fields.get(4) != Some(&"/etc/hostname") {
      continue;
    }
    let root = std::path::Path::new(fields[3]);
    if let Some(id) = root
      .parent()
      .and_then(std::path::Path::file_name)
      .and_then(|id| id.to_str())
      .filter(|id| valid_id(id, 64))
    {
      return Some(id.to_string());
    }
  }
  // Docker's default hostname is the first 12 characters of its ID.
  (valid_id(hostname, 12) || valid_id(hostname, 64))
    .then(|| hostname.to_string())
}

fn valid_id(value: &str, length: usize) -> bool {
  value.len() == length
    && value.bytes().all(|byte| {
      byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
    })
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn mount_identity_takes_precedence_over_custom_hostname() {
    let id = "a".repeat(64);
    let mounts = format!(
      "20 19 8:1 /docker/containers/{id}/hostname /etc/hostname rw - ext4 /dev/sda rw"
    );
    assert_eq!(container_id(&mounts, "custom-core"), Some(id));
  }

  #[test]
  fn identity_fallback_never_accepts_an_arbitrary_container_name() {
    assert_eq!(
      container_id("", "abc123def456"),
      Some("abc123def456".into())
    );
    assert_eq!(container_id("", "komodo"), None);
    assert_eq!(container_id("malformed", ""), None);
  }
}
