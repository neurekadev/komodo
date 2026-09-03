//! Filesystem identity checks shared by backup, restore, and File Manager.
//!
//! Canonical names resolve symlinks, but directory bind mounts can still give
//! the same objects different names. Compare existing ancestor identities
//! together with the suffix below each ancestor, including missing leaves.

use std::{
  fs,
  os::unix::fs::MetadataExt,
  path::{Path, PathBuf},
};

use anyhow::{Context, anyhow};

type Identity = (u64, u64);
type Anchors = Vec<(Identity, PathBuf)>;

pub fn resolve_existing_ancestor(
  path: &Path,
) -> anyhow::Result<PathBuf> {
  let mut ancestor = path;
  let mut missing = Vec::new();
  loop {
    match ancestor.canonicalize() {
      Ok(mut resolved) => {
        for component in missing.iter().rev() {
          resolved.push(component);
        }
        return Ok(resolved);
      }
      Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
        missing.push(
          ancestor
            .file_name()
            .with_context(|| {
              format!(
                "Cannot resolve filesystem path {}",
                path.display()
              )
            })?
            .to_os_string(),
        );
        ancestor = ancestor.parent().with_context(|| {
          format!("Cannot resolve filesystem path {}", path.display())
        })?;
      }
      Err(error) => {
        return Err(error).with_context(|| {
          format!("Cannot inspect filesystem path {}", path.display())
        });
      }
    }
  }
}

fn anchors_with(
  path: &Path,
  mut identity: impl FnMut(&Path) -> anyhow::Result<Option<Identity>>,
) -> anyhow::Result<Anchors> {
  let mut anchors = Vec::new();
  for ancestor in path.ancestors() {
    if let Some(identity) = identity(ancestor)? {
      anchors
        .push((identity, path.strip_prefix(ancestor)?.to_path_buf()));
    }
  }
  if anchors.is_empty() {
    return Err(anyhow!(
      "Filesystem path has no inspectable ancestor: {}",
      path.display()
    ));
  }
  Ok(anchors)
}

fn anchors(path: &Path) -> anyhow::Result<Anchors> {
  let resolved = resolve_existing_ancestor(path)?;
  anchors_with(&resolved, |ancestor| match fs::metadata(ancestor) {
    Ok(metadata) => Ok(Some((metadata.dev(), metadata.ino()))),
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
      Ok(None)
    }
    Err(error) => Err(error).with_context(|| {
      format!(
        "Cannot inspect filesystem identity {}",
        ancestor.display()
      )
    }),
  })
}

fn entry_anchors_with(
  path: &Path,
  mut identity: impl FnMut(
    &Path,
  ) -> anyhow::Result<Option<(Identity, bool)>>,
) -> anyhow::Result<Anchors> {
  anchors_with(path, |ancestor| {
    Ok(identity(ancestor)?.and_then(|(identity, is_directory)| {
      // Renaming replaces an entry, not a symlink's target or every hard link
      // to a file. Directory identities still reveal bind-mounted aliases.
      (ancestor != path || is_directory).then_some(identity)
    }))
  })
}

fn entry_anchors(path: &Path) -> anyhow::Result<Anchors> {
  let resolved = match (path.parent(), path.file_name()) {
    (Some(parent), Some(name)) => {
      resolve_existing_ancestor(parent)?.join(name)
    }
    _ => resolve_existing_ancestor(path)?,
  };
  entry_anchors_with(
    &resolved,
    |ancestor| match fs::symlink_metadata(ancestor) {
      Ok(metadata) => Ok(Some((
        (metadata.dev(), metadata.ino()),
        metadata.is_dir(),
      ))),
      Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
        Ok(None)
      }
      Err(error) => Err(error).with_context(|| {
        format!(
          "Cannot inspect filesystem entry {}",
          ancestor.display()
        )
      }),
    },
  )
}

fn contains_anchored(parent: &Anchors, child: &Anchors) -> bool {
  // A higher shared ancestor must not override a nearer existing object:
  // one namespace may overlay a mount beneath an otherwise shared directory.
  parent.first().is_some_and(|(parent_id, parent_suffix)| {
    child.iter().any(|(child_id, child_suffix)| {
      parent_id == child_id && child_suffix.starts_with(parent_suffix)
    })
  })
}

fn same_location_anchored(left: &Anchors, right: &Anchors) -> bool {
  // Equality is stricter than containment in either direction. In particular,
  // a parent repository cannot inherit a child's initialization or secrets.
  left.first().is_some_and(|left| Some(left) == right.first())
}

/// Whether two paths name the same location, including directory bind aliases
/// and equal missing suffixes, without treating a parent or child as equal.
pub fn paths_same_location(
  left: &Path,
  right: &Path,
) -> anyhow::Result<bool> {
  Ok(same_location_anchored(&anchors(left)?, &anchors(right)?))
}

/// Whether `parent` names the same object as `child`, or contains it, even
/// through directory bind aliases and not-yet-created descendants.
pub fn path_contains(
  parent: &Path,
  child: &Path,
) -> anyhow::Result<bool> {
  Ok(contains_anchored(&anchors(parent)?, &anchors(child)?))
}

/// Whether two paths overlap physically. Shared ancestors alone do not imply
/// overlap: their remaining suffixes must also have a prefix relationship.
pub fn paths_overlap(
  left: &Path,
  right: &Path,
) -> anyhow::Result<bool> {
  let left = anchors(left)?;
  let right = anchors(right)?;
  Ok(
    contains_anchored(&left, &right)
      || contains_anchored(&right, &left),
  )
}

/// Overlap between entries to be renamed or replaced. Follow ancestors, but
/// never follow the final symlink or conflate distinct hard-linked leaf names.
pub fn entry_paths_overlap(
  left: &Path,
  right: &Path,
) -> anyhow::Result<bool> {
  let left = entry_anchors(left)?;
  let right = entry_anchors(right)?;
  Ok(
    contains_anchored(&left, &right)
      || contains_anchored(&right, &left),
  )
}

/// Whether replacing an entry could replace or contain protected storage.
/// The protected root is followed because it denotes the stored data itself.
pub fn entry_overlaps_path(
  entry: &Path,
  protected: &Path,
) -> anyhow::Result<bool> {
  let entry = entry_anchors(entry)?;
  let protected = anchors(protected)?;
  Ok(
    contains_anchored(&entry, &protected)
      || contains_anchored(&protected, &entry),
  )
}

#[cfg(test)]
mod tests {
  use super::*;

  fn aliased(path: &str) -> Anchors {
    anchors_with(Path::new(path), |ancestor| {
      Ok(match ancestor.to_str().unwrap() {
        "/" => Some((1, 1)),
        "/real" | "/alias" => Some((1, 2)),
        "/unrelated" => Some((1, 3)),
        _ => None,
      })
    })
    .unwrap()
  }

  #[test]
  fn bind_aliases_preserve_missing_suffix_identity() {
    assert!(contains_anchored(
      &aliased("/real/private"),
      &aliased("/alias/private/new/file"),
    ));
    assert!(contains_anchored(
      &aliased("/alias"),
      &aliased("/real/private"),
    ));
    assert!(!contains_anchored(
      &aliased("/real/private"),
      &aliased("/alias/application"),
    ));
    assert!(!contains_anchored(
      &aliased("/real/private"),
      &aliased("/unrelated/private"),
    ));
  }

  #[test]
  fn bind_alias_equality_is_not_parent_or_sibling_overlap() {
    assert!(same_location_anchored(
      &aliased("/real"),
      &aliased("/alias"),
    ));
    assert!(same_location_anchored(
      &aliased("/real/new/repository"),
      &aliased("/alias/new/repository"),
    ));
    for (left, right) in [
      ("/real", "/alias/repository"),
      ("/real/repository", "/alias"),
      ("/real/primary", "/alias/mirror"),
      ("/real/repository", "/unrelated/repository"),
    ] {
      assert!(!same_location_anchored(
        &aliased(left),
        &aliased(right)
      ));
    }
  }

  #[test]
  fn aliased_destination_and_rollback_names_collide() {
    for name in ["config", "config.komodo-rollback-operation"] {
      assert!(contains_anchored(
        &aliased(&format!("/real/{name}")),
        &aliased(&format!("/alias/{name}")),
      ));
    }
    assert!(!contains_anchored(
      &aliased("/real/first.conf"),
      &aliased("/alias/second.conf"),
    ));
  }

  #[test]
  fn aliased_relative_symlink_entries_do_not_follow_different_targets()
   {
    let entry = |path: &str| {
      entry_anchors_with(Path::new(path), |ancestor| {
        Ok(match ancestor.to_str().unwrap() {
          "/" => Some(((1, 1), true)),
          "/real" | "/nested/alias" => Some(((1, 2), true)),
          // The shared symlink ../target/file resolves to /target/file
          // versus /nested/target/file; neither target identifies the entry.
          "/real/config" | "/nested/alias/config" => {
            Some(((1, 3), false))
          }
          _ => None,
        })
      })
      .unwrap()
    };
    assert!(contains_anchored(
      &entry("/real/config"),
      &entry("/nested/alias/config")
    ));
    assert!(contains_anchored(
      &entry("/real/config.komodo-rollback-id"),
      &entry("/nested/alias/config.komodo-rollback-id"),
    ));
  }

  #[test]
  fn distinct_symlink_and_hardlink_entries_remain_independent() {
    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("target");
    fs::write(&target, b"data").unwrap();
    let first = root.path().join("first");
    let second = root.path().join("second");
    std::os::unix::fs::symlink(&target, &first).unwrap();
    std::os::unix::fs::symlink(&target, &second).unwrap();
    assert!(paths_overlap(&first, &second).unwrap());
    assert!(!entry_paths_overlap(&first, &second).unwrap());
    assert!(!entry_overlaps_path(&first, &target).unwrap());
    let hardlink = root.path().join("hardlink");
    fs::hard_link(&target, &hardlink).unwrap();
    assert!(!entry_paths_overlap(&target, &hardlink).unwrap());
  }

  #[test]
  fn identity_errors_are_not_treated_as_disjoint_paths() {
    assert!(
      anchors_with(Path::new("/private/new"), |_| {
        Err(
          std::io::Error::from(std::io::ErrorKind::PermissionDenied)
            .into(),
        )
      })
      .is_err()
    );
  }

  #[test]
  fn a_shared_ancestor_does_not_override_distinct_existing_mounts() {
    let left = vec![
      ((1, 10), PathBuf::new()),
      ((1, 2), PathBuf::from("private")),
    ];
    let right = vec![
      ((2, 20), PathBuf::new()),
      ((1, 2), PathBuf::from("private")),
    ];
    assert!(!contains_anchored(&left, &right));
    assert!(!contains_anchored(&right, &left));
    assert!(!same_location_anchored(&left, &right));
  }

  #[test]
  fn existing_aliases_and_missing_siblings_use_the_same_policy() {
    let root = tempfile::tempdir().unwrap();
    let real = root.path().join("real");
    let alias = root.path().join("alias");
    fs::create_dir(&real).unwrap();
    std::os::unix::fs::symlink(&real, &alias).unwrap();
    assert!(paths_same_location(&real, &alias).unwrap());
    assert!(
      paths_same_location(
        &real.join("missing/repository"),
        &alias.join("missing/repository"),
      )
      .unwrap()
    );
    assert!(!paths_same_location(&real, &alias.join("new")).unwrap());
    assert!(
      paths_overlap(
        &real.join("private"),
        &alias.join("private/new")
      )
      .unwrap()
    );
    assert!(
      !paths_overlap(
        &real.join("private"),
        &alias.join("application")
      )
      .unwrap()
    );
    fs::write(real.join("file"), b"original").unwrap();
    fs::hard_link(real.join("file"), real.join("another-name"))
      .unwrap();
    assert!(
      paths_overlap(&real.join("file"), &real.join("another-name"))
        .unwrap()
    );
  }
}
