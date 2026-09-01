use std::{
  ffi::OsString,
  path::{Component, Path, PathBuf},
};

use anyhow::{Context, anyhow};
use cap_fs_ext::DirExt as _;
use cap_std::fs::Dir;

pub const MAX_DEPTH: usize = 128;
pub const PRIVATE_STATE_DIRECTORY: &str = ".komodo-file-manager";

/// Parse the only path representation accepted by File Manager APIs.
///
/// The empty string represents the root when `allow_root` is true. Every
/// other path is normalized UTF-8 with forward-slash separators.
pub fn relative_path(
  value: &str,
  allow_root: bool,
) -> anyhow::Result<PathBuf> {
  if value.is_empty() {
    return if allow_root {
      Ok(PathBuf::new())
    } else {
      Err(anyhow!("Path cannot be empty"))
    };
  }
  if value.contains('\0') {
    return Err(anyhow!("Path cannot contain NUL"));
  }
  if value.contains('\\') {
    return Err(anyhow!("Path must use '/' separators"));
  }
  if value.starts_with('/')
    || value.starts_with("//")
    || value.as_bytes().get(1).is_some_and(|byte| *byte == b':')
  {
    return Err(anyhow!("Absolute paths are not allowed"));
  }
  if value.ends_with('/') || value.contains("//") {
    return Err(anyhow!("Path contains an empty component"));
  }

  let path = Path::new(value);
  let mut depth = 0;
  for component in path.components() {
    match component {
      Component::Normal(name) if !name.is_empty() => {
        if name.to_str().is_some_and(|name| {
          name.eq_ignore_ascii_case(PRIVATE_STATE_DIRECTORY)
        }) {
          return Err(anyhow!(
            "Path is reserved for File Manager recovery state"
          ));
        }
        depth += 1;
      }
      _ => {
        return Err(anyhow!(
          "Path must contain only normal relative components"
        ));
      }
    }
  }
  if depth == 0 || depth > MAX_DEPTH {
    return Err(anyhow!(
      "Path depth must be between 1 and {MAX_DEPTH}"
    ));
  }
  Ok(path.to_path_buf())
}

pub fn single_name(value: &str) -> anyhow::Result<OsString> {
  let path = relative_path(value, false)?;
  let mut components = path.components();
  let Some(Component::Normal(name)) = components.next() else {
    return Err(anyhow!("Name is invalid"));
  };
  if components.next().is_some() {
    return Err(anyhow!("Name cannot contain path separators"));
  }
  Ok(name.to_os_string())
}

pub fn open_dir_nofollow(
  root: &Dir,
  path: &Path,
) -> anyhow::Result<Dir> {
  let mut current = root.try_clone()?;
  for component in path.components() {
    let Component::Normal(name) = component else {
      return Err(anyhow!("Invalid normalized path"));
    };
    current = current.open_dir_nofollow(name).with_context(|| {
      format!("Directory component is inaccessible: {name:?}")
    })?;
  }
  Ok(current)
}

pub fn open_parent_nofollow(
  root: &Dir,
  path: &Path,
) -> anyhow::Result<(Dir, OsString)> {
  let name = path
    .file_name()
    .context("Path must name a filesystem entry")?
    .to_os_string();
  let parent = path.parent().unwrap_or_else(|| Path::new(""));
  Ok((open_dir_nofollow(root, parent)?, name))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn accepts_normal_relative_paths_and_root() {
    assert_eq!(
      relative_path("config/app.toml", false).unwrap(),
      PathBuf::from("config/app.toml")
    );
    assert_eq!(relative_path("", true).unwrap(), PathBuf::new());
  }

  #[test]
  fn rejects_escape_and_alternate_syntax() {
    for path in [
      "",
      "/etc/passwd",
      "../secret",
      "config/../secret",
      "./config",
      "config//file",
      "config/",
      "C:/secret",
      "C:\\secret",
      "config\\file",
    ] {
      assert!(
        relative_path(path, false).is_err(),
        "accepted {path:?}"
      );
    }
  }

  #[test]
  fn rejects_embedded_nul() {
    assert!(relative_path("config/\0secret", false).is_err());
  }

  #[test]
  fn rejects_private_recovery_paths() {
    assert!(relative_path(PRIVATE_STATE_DIRECTORY, false).is_err());
    assert!(relative_path(".KOMODO-FILE-MANAGER", false).is_err());
    assert!(
      relative_path("data/.komodo-file-manager/entry", false)
        .is_err()
    );
  }

  #[test]
  fn name_is_exactly_one_component() {
    assert!(single_name("renamed.txt").is_ok());
    assert!(single_name("nested/renamed.txt").is_err());
  }

  #[cfg(unix)]
  #[test]
  fn refuses_symlinked_directory_components() {
    use cap_std::ambient_authority;
    use std::{fs, os::unix::fs::symlink};
    use uuid::Uuid;

    let base = std::env::temp_dir().join(Uuid::new_v4().to_string());
    let root_path = base.join("root");
    let outside = base.join("outside");
    fs::create_dir_all(&root_path).unwrap();
    fs::create_dir_all(&outside).unwrap();
    symlink(&outside, root_path.join("escape")).unwrap();
    let root =
      Dir::open_ambient_dir(&root_path, ambient_authority()).unwrap();

    assert!(open_dir_nofollow(&root, Path::new("escape")).is_err());
    fs::remove_dir_all(base).unwrap();
  }
}
