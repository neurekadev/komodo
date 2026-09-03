use std::{sync::Arc, time::Duration};

use anyhow::{Context, anyhow};
use encoding::Decode as _;
use mogh_resolver::HasResponse;
use periphery_client::api;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::json;
use transport::channel::channel;
use uuid::Uuid;

use crate::{
  connection::{PeripheryConnection, PeripheryConnectionArgs},
  state::periphery_connections,
};

pub mod file_manager;
pub mod terminal;

#[derive(Debug)]
pub struct PeripheryClient {
  /// Usually the server id
  pub id: String,
}

impl PeripheryClient {
  pub async fn new(
    args: PeripheryConnectionArgs<'_>,
    insecure_tls: bool,
  ) -> anyhow::Result<PeripheryClient> {
    let connections = periphery_connections();

    let id = args.id.to_string();

    // Spawn client side connection if one doesn't exist.
    let Some(connection) = connections.get(&id).await else {
      if args.address.is_none() {
        return Err(anyhow!("Server {id} is not connected"));
      }
      return args
        .spawn_client_connection(id.clone(), insecure_tls)
        .await;
    };

    // Ensure the connection args are unchanged.
    if args.matches(&connection.args) {
      return Ok(PeripheryClient { id });
    }

    // The args have changed.
    if args.address.is_none() {
      // Periphery -> Core connection
      // Remove this connection, wait and see if client reconnects
      connections.remove(&id).await;
      tokio::time::sleep(Duration::from_millis(500)).await;
      connections
        .get(&id)
        .await
        .with_context(|| format!("Server {id} is not connected"))?;
      Ok(PeripheryClient { id })
    } else {
      // Core -> Periphery connection
      args.spawn_client_connection(id.clone(), insecure_tls).await
    }
  }

  pub async fn cleanup(self) -> Option<Arc<PeripheryConnection>> {
    periphery_connections().remove(&self.id).await
  }

  pub async fn health_check(&self) -> anyhow::Result<()> {
    self.request(api::GetHealth {}).await?;
    Ok(())
  }

  pub async fn request<T>(
    &self,
    request: T,
  ) -> anyhow::Result<T::Response>
  where
    T: std::fmt::Debug + Serialize + HasResponse,
    T::Response: DeserializeOwned,
  {
    let connection =
      periphery_connections().get(&self.id).await.with_context(
        || format!("No connection found for server {}", self.id),
      )?;

    self.request_on_connection(connection, request, None).await
  }

  /// Bound read-only requests even while the worker continues sending pings.
  /// Timed-out mutating requests need a separate worker-lifetime protocol.
  pub async fn request_with_timeout<T>(
    &self,
    request: T,
    timeout: Duration,
  ) -> anyhow::Result<T::Response>
  where
    T: std::fmt::Debug + Serialize + HasResponse,
    T::Response: DeserializeOwned,
  {
    let connection =
      periphery_connections().get(&self.id).await.with_context(
        || format!("No connection found for server {}", self.id),
      )?;
    self
      .request_on_connection(connection, request, Some(timeout))
      .await
  }

  /// Validate and retain the exact connection used to send capabilities.
  /// A concurrent Server edit/reconnect must not redirect a pinned request.
  pub async fn request_pinned<T>(
    &self,
    expected: PeripheryConnectionArgs<'_>,
    request: T,
  ) -> anyhow::Result<T::Response>
  where
    T: std::fmt::Debug + Serialize + HasResponse,
    T::Response: DeserializeOwned,
  {
    self.request_pinned_inner(expected, request, None).await
  }

  /// Completion polling has a deadline, without implying the writer exited.
  pub async fn request_pinned_with_timeout<T>(
    &self,
    expected: PeripheryConnectionArgs<'_>,
    request: T,
    timeout: Duration,
  ) -> anyhow::Result<T::Response>
  where
    T: std::fmt::Debug + Serialize + HasResponse,
    T::Response: DeserializeOwned,
  {
    self
      .request_pinned_inner(expected, request, Some(timeout))
      .await
  }

  async fn request_pinned_inner<T>(
    &self,
    expected: PeripheryConnectionArgs<'_>,
    request: T,
    timeout: Option<Duration>,
  ) -> anyhow::Result<T::Response>
  where
    T: std::fmt::Debug + Serialize + HasResponse,
    T::Response: DeserializeOwned,
  {
    let connection = periphery_connections()
      .get(&self.id)
      .await
      .context("Trusted backup worker is not connected")?;
    if !expected.matches(&connection.args) {
      return Err(anyhow!(
        "Backup worker connection changed; verify its enrolled identity and retry"
      ));
    }
    self
      .request_on_connection(connection, request, timeout)
      .await
  }

  async fn request_on_connection<T>(
    &self,
    connection: Arc<PeripheryConnection>,
    request: T,
    timeout: Option<Duration>,
  ) -> anyhow::Result<T::Response>
  where
    T: std::fmt::Debug + Serialize + HasResponse,
    T::Response: DeserializeOwned,
  {
    let channel_id = Uuid::new_v4();
    let (response_sender, mut response_receiever) = channel();
    connection
      .responses
      .insert(channel_id, response_sender)
      .await;

    let work = async {
      // Include connection readiness and the send in the total deadline.
      connection.bail_if_not_connected().await?;
      connection
        .sender
        .send_request(
          channel_id,
          &json!({
            "type": T::req_type(),
            "params": request
          }),
        )
        .await
        .context("Failed to send request over channel")?;

      // Poll for the associated response
      loop {
        let message = response_receiever
          .recv()
          // Periphery request handler sends pings every 4s
          // *on this channel specifically* so Core knows
          // request is being processed. Hardcoded 11s
          // allows for missed 5s ping due to network reconnect.
          .with_timeout(Duration::from_secs(10))
          .await?;

        let Some(message) = message.decode()? else {
          // Just a ping from periphery request handler
          continue;
        };

        return message.decode();
      }
    };
    let res = if let Some(timeout) = timeout {
      tokio::time::timeout(timeout, work)
        .await
        .context("Periphery request exceeded its total deadline")
        .and_then(|result| result)
    } else {
      work.await
    };

    // Also remove timed-out registrations instead of abandoning a response
    // sender each time a read-only discovery deadline expires.
    connection.responses.remove(&channel_id).await;

    res
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use komodo_client::entities::server::Server;

  #[tokio::test]
  async fn read_only_request_deadline_removes_its_response_registration()
   {
    use encoding::WithChannel;
    use periphery_client::transport::TransportMessage;

    let server = Server {
      id: "inventory-test".into(),
      ..Default::default()
    };
    let (connection, mut outgoing) = PeripheryConnection::new(
      PeripheryConnectionArgs::from_server(&server),
    );
    connection.set_connected(true);
    let client = PeripheryClient {
      id: server.id.clone(),
    };
    let error = client
      .request_on_connection(
        connection.clone(),
        api::backup::GetBackupVolumeInventory {},
        Some(Duration::ZERO),
      )
      .await
      .unwrap_err();
    assert!(error.to_string().contains("total deadline"));
    let message = outgoing
      .recv()
      .with_timeout(Duration::from_secs(1))
      .await
      .unwrap();
    let TransportMessage::Request(message) =
      message.decode().unwrap()
    else {
      panic!("expected an inventory request")
    };
    let WithChannel { channel, .. } = message
      .decode()
      .unwrap()
      .map_decode::<serde_json::Value>()
      .unwrap();
    assert!(connection.responses.get(&channel).await.is_none());
  }

  #[test]
  fn pinned_connection_identity_does_not_follow_server_replacement() {
    let mut server = Server {
      id: "server".into(),
      ..Default::default()
    };
    server.config.address = "wss://trusted.example".into();
    server.info.public_key = "trusted-key".into();
    let expected = PeripheryConnectionArgs::from_server(&server);
    let (connection, _receiver) = PeripheryConnection::new(expected);
    assert!(expected.matches(&connection.args));
    let mut changed = server.clone();
    changed.info.public_key = "replacement-key".into();
    let (replacement, _receiver) = connection
      .with_new_args(PeripheryConnectionArgs::from_server(&changed));
    assert!(!expected.matches(&replacement.args));
    assert!(expected.matches(&connection.args));
    changed = server.clone();
    changed.config.address = "wss://other.example".into();
    assert!(
      !expected
        .matches(PeripheryConnectionArgs::from_server(&changed))
    );
  }
}
