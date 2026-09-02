use std::sync::{Arc, OnceLock};

use anyhow::Context;
use tokio::sync::{
  OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock,
};

fn gate() -> &'static Arc<RwLock<()>> {
  static GATE: OnceLock<Arc<RwLock<()>>> = OnceLock::new();
  GATE.get_or_init(Default::default)
}

/// Actions may write files without calling the API. Keep their process lifetime
/// separate from the mutation barrier so nested API calls never need a recursive
/// read behind a backup writer. Neither side queues on this gate.
pub(crate) fn begin_action()
-> anyhow::Result<OwnedRwLockReadGuard<()>> {
  begin_action_on(gate())
}

fn begin_action_on(
  gate: &Arc<RwLock<()>>,
) -> anyhow::Result<OwnedRwLockReadGuard<()>> {
  gate.clone().try_read_owned().context(
    "Cannot start an Action while backup or recovery work is active; retry after it completes",
  )
}

pub(super) fn quiesce_actions()
-> anyhow::Result<OwnedRwLockWriteGuard<()>> {
  quiesce_actions_on(gate())
}

fn quiesce_actions_on(
  gate: &Arc<RwLock<()>>,
) -> anyhow::Result<OwnedRwLockWriteGuard<()>> {
  gate.clone().try_write_owned().context(
    "Cannot start backup or recovery work while an Action or another protected backup operation is active; finish or cancel Actions, then retry",
  )
}

#[cfg(test)]
mod tests {
  use super::*;
  use futures_util::FutureExt;

  #[tokio::test]
  async fn running_actions_reject_backup_without_blocking_nested_calls()
   {
    let gate = Arc::new(RwLock::new(()));
    let mutations = Arc::new(RwLock::new(()));
    let action = begin_action_on(&gate).unwrap();
    assert!(quiesce_actions_on(&gate).is_err());
    // Refusal must not queue a writer that blocks the Action's API work.
    let nested =
      mutations.clone().read_owned().now_or_never().unwrap();
    let child_action = begin_action_on(&gate).unwrap();
    drop(action);
    assert!(quiesce_actions_on(&gate).is_err());
    drop(child_action);
    drop(nested);
    assert!(quiesce_actions_on(&gate).is_ok());
  }

  #[test]
  fn protected_backup_refuses_actions_until_its_guard_is_dropped() {
    let gate = Arc::new(RwLock::new(()));
    let backup = quiesce_actions_on(&gate).unwrap();
    assert!(begin_action_on(&gate).is_err());
    assert!(quiesce_actions_on(&gate).is_err());
    drop(backup);
    assert!(begin_action_on(&gate).is_ok());
  }
}
