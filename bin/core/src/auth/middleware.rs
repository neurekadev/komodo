use anyhow::{Context, anyhow};
use database::mungos::mongodb::bson::doc;
use komodo_client::entities::{komodo_timestamp, user::User};
use mogh_auth_server::RequestAuthentication;

use crate::{
  auth::JWT_PROVIDER, helpers::query::get_user, state::db_client,
};

tokio::task_local! {
  static AUTH_REQUEST_MUTATIONS: ();
}

/// Mark auth requests without holding the snapshot barrier while parsing bodies,
/// hashing passwords, or awaiting OAuth providers. The AuthImpl database
/// callbacks acquire it only for their mutation. Other AuthImpl callers are
/// already protected by the write/execute barrier and must not lock recursively.
pub async fn backup_mutation_scope(
  request: axum::extract::Request,
  next: axum::middleware::Next,
) -> axum::response::Response {
  AUTH_REQUEST_MUTATIONS.scope((), next.run(request)).await
}

pub(super) async fn mutation_guard()
-> Option<tokio::sync::RwLockReadGuard<'static, ()>> {
  auth_mutation_guard_for(crate::backup::mutation_barrier()).await
}

async fn auth_mutation_guard_for(
  barrier: &tokio::sync::RwLock<()>,
) -> Option<tokio::sync::RwLockReadGuard<'_, ()>> {
  if AUTH_REQUEST_MUTATIONS.try_with(|_| ()).is_ok() {
    Some(barrier.read().await)
  } else {
    None
  }
}

pub async fn extract_user_from_auth(
  auth: RequestAuthentication,
  require_user_enabled: bool,
) -> anyhow::Result<User> {
  let user_id = match auth {
    RequestAuthentication::UserId(user_id) => user_id,
    RequestAuthentication::KeyAndSecret { key, secret } => {
      auth_api_key_get_user_id(&key, &secret).await?
    }
    RequestAuthentication::PublicKey(_) => todo!(),
  };
  if require_user_enabled {
    check_enabled(&user_id).await
  } else {
    get_user(&user_id).await
  }
}

pub async fn auth_jwt_check_enabled(
  jwt: &str,
) -> anyhow::Result<User> {
  let user_id = JWT_PROVIDER.decode_sub(jwt)?;
  check_enabled(&user_id).await
}

pub async fn auth_api_key_check_enabled(
  key: &str,
  secret: &str,
) -> anyhow::Result<User> {
  let user_id = auth_api_key_get_user_id(key, secret).await?;
  check_enabled(&user_id).await
}

/// Api Key Clock skew tolerance in milliseconds (5 minutes for Api Keys)
const API_KEY_CLOCK_SKEW_TOLERANCE_MS: i64 = 5 * 60 * 1000;

pub async fn auth_api_key_get_user_id(
  key: &str,
  secret: &str,
) -> anyhow::Result<String> {
  let key = db_client()
    .api_keys
    .find_one(doc! { "key": key })
    .await
    .context("Failed to query db")?
    .context("Invalid user credentials")?;
  // Apply clock skew tolerance.
  // Token is invalid if expiration is less than (now - tolerance)
  if key.expires != 0
    && key.expires
      < komodo_timestamp()
        .saturating_sub(API_KEY_CLOCK_SKEW_TOLERANCE_MS)
  {
    return Err(anyhow!("Invalid user credentials"));
  }
  if bcrypt::verify(secret, &key.secret)
    .map_err(|_| anyhow!("Invalid user credentials"))?
  {
    // secret matches
    Ok(key.user_id)
  } else {
    // secret mismatch
    Err(anyhow!("Invalid user credentials"))
  }
}

async fn check_enabled(user_id: &str) -> anyhow::Result<User> {
  let user = get_user(user_id).await?;
  if user.enabled {
    Ok(user)
  } else {
    Err(anyhow!("Invalid user credentials"))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use futures_util::poll;
  use std::{future::pending, pin::pin, task::Poll};
  use tokio::sync::RwLock;

  #[tokio::test]
  async fn stalled_auth_preparation_does_not_hold_the_barrier() {
    let barrier = RwLock::new(());
    let mut preparation =
      pin!(AUTH_REQUEST_MUTATIONS.scope((), pending::<()>()));
    assert!(poll!(&mut preparation).is_pending());
    assert!(barrier.try_write().is_ok());
  }

  #[tokio::test]
  async fn auth_mutations_wait_for_exports_and_hold_the_barrier() {
    let barrier = RwLock::new(());
    let export = barrier.write().await;
    let mut mutation = pin!(
      AUTH_REQUEST_MUTATIONS
        .scope((), auth_mutation_guard_for(&barrier),)
    );
    assert!(poll!(&mut mutation).is_pending());
    drop(export);
    let Poll::Ready(Some(guard)) = poll!(&mut mutation) else {
      panic!("Auth mutation should acquire the released barrier");
    };
    assert!(barrier.try_write().is_err());
    drop(guard);
    assert!(barrier.try_write().is_ok());
  }

  #[tokio::test]
  async fn already_guarded_internal_callers_do_not_reacquire() {
    let barrier = RwLock::new(());
    let _outer = barrier.read().await;
    let mut export = pin!(barrier.write());
    assert!(poll!(&mut export).is_pending());
    // A second read would wait behind the queued writer and deadlock.
    assert!(auth_mutation_guard_for(&barrier).await.is_none());
  }
}
