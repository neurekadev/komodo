use std::sync::OnceLock;

const EDGE_TAG: &str = "edge";
const DEV_VERSION: &str = "dev";
const UNKNOWN_HASH: &str = "unknown";
const SHORT_HASH_LEN: usize = 12;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildIdentity {
  pub version: String,
  pub git_tag: Option<String>,
  pub git_hash: Option<String>,
}

impl BuildIdentity {
  pub fn from_git_metadata(
    git_tag: Option<&str>,
    git_hash: Option<&str>,
  ) -> Self {
    let git_tag = normalized(git_tag).map(str::to_string);
    let git_hash = normalized_hash(git_hash).map(str::to_string);
    let version = match (git_tag.as_deref(), git_hash.as_deref()) {
      (Some(EDGE_TAG), Some(git_hash)) => {
        let short_hash = git_hash.chars().take(SHORT_HASH_LEN);
        format!("{EDGE_TAG}@{}", short_hash.collect::<String>())
      }
      (Some(EDGE_TAG | DEV_VERSION), None) | (None, _) => {
        DEV_VERSION.to_string()
      }
      (Some(DEV_VERSION), Some(_)) => DEV_VERSION.to_string(),
      (Some(git_tag), _) => git_tag.to_string(),
    };
    Self {
      version,
      git_tag,
      git_hash,
    }
  }
}

fn normalized(value: Option<&str>) -> Option<&str> {
  value.map(str::trim).filter(|value| !value.is_empty())
}

fn normalized_hash(value: Option<&str>) -> Option<&str> {
  normalized(value).filter(|value| *value != UNKNOWN_HASH)
}

pub fn build_identity() -> &'static BuildIdentity {
  static BUILD_IDENTITY: OnceLock<BuildIdentity> = OnceLock::new();
  BUILD_IDENTITY.get_or_init(|| {
    BuildIdentity::from_git_metadata(
      option_env!("GIT_TAG"),
      option_env!("GIT_HASH"),
    )
  })
}

pub fn version() -> &'static str {
  &build_identity().version
}

pub fn versions_match(left: &str, right: &str) -> bool {
  left == right
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn release_tag_is_the_product_version() {
    let identity = BuildIdentity::from_git_metadata(
      Some("3.0.0"),
      Some("0123456789abcdef"),
    );
    assert_eq!(identity.version, "3.0.0");
    assert_eq!(identity.git_tag.as_deref(), Some("3.0.0"));
    assert_eq!(
      identity.git_hash.as_deref(),
      Some("0123456789abcdef")
    );
  }

  #[test]
  fn edge_version_uses_twelve_hash_characters() {
    let identity = BuildIdentity::from_git_metadata(
      Some("edge"),
      Some("0123456789abcdef"),
    );
    assert_eq!(identity.version, "edge@0123456789ab");
  }

  #[test]
  fn missing_tag_is_a_dev_build() {
    let identity = BuildIdentity::from_git_metadata(
      None,
      Some("0123456789abcdef"),
    );
    assert_eq!(identity.version, "dev");
  }

  #[test]
  fn edge_without_a_usable_hash_is_a_dev_build() {
    for git_hash in [None, Some("unknown")] {
      let identity =
        BuildIdentity::from_git_metadata(Some("edge"), git_hash);
      assert_eq!(identity.version, "dev");
      assert_eq!(identity.git_hash, None);
    }
  }

  #[test]
  fn edge_version_keeps_a_short_hash() {
    let identity =
      BuildIdentity::from_git_metadata(Some("edge"), Some("abc123"));
    assert_eq!(identity.version, "edge@abc123");
  }

  #[test]
  fn edge_builds_match_only_when_the_commit_matches() {
    let core = BuildIdentity::from_git_metadata(
      Some("edge"),
      Some("0123456789abcdef"),
    );
    let matching_periphery = BuildIdentity::from_git_metadata(
      Some("edge"),
      Some("0123456789abcdef"),
    );
    let different_periphery = BuildIdentity::from_git_metadata(
      Some("edge"),
      Some("fedcba9876543210"),
    );
    assert!(versions_match(
      &core.version,
      &matching_periphery.version
    ));
    assert!(!versions_match(
      &core.version,
      &different_periphery.version
    ));
  }
}
