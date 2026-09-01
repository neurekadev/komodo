use std::{sync::Arc, time::Duration};

use anyhow::{Context as _, anyhow};
use periphery_client::{
  api::file_manager::{
    StartFileManagerDownload, StartFileManagerDownloadResponse,
    StartFileManagerUpload,
  },
  transport::{EncodedTransportMessage, FileTransferMessage},
};
use tokio_util::sync::CancellationToken;
use transport::channel::{
  Receiver, Sender, channel, channel_with_capacity,
};
use uuid::Uuid;

use crate::{
  connection::{
    FileTransferChannels, FileTransferRoute,
    send_file_transfer_cancel_bounded,
  },
  periphery::PeripheryClient,
  state::periphery_connections,
};

const DOWNLOAD_CREDIT_WINDOW: u32 = 4;
const DOWNLOAD_TERMINAL_HEADROOM: usize = 1;
const DOWNLOAD_ROUTE_CAPACITY: usize =
  DOWNLOAD_CREDIT_WINDOW as usize + DOWNLOAD_TERMINAL_HEADROOM;
const DOWNLOAD_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const DOWNLOAD_HEARTBEAT_SEND_TIMEOUT: Duration =
  Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileTransferMode {
  Legacy,
  DownloadCredit,
  DownloadCreditHeartbeat,
}

impl FileTransferMode {
  fn for_download(
    supports_download_credit: bool,
    supports_download_heartbeat: bool,
  ) -> Self {
    if supports_download_credit && supports_download_heartbeat {
      Self::DownloadCreditHeartbeat
    } else if supports_download_credit {
      Self::DownloadCredit
    } else {
      Self::Legacy
    }
  }

  fn channel(
    self,
  ) -> (FileTransferRoute, Receiver<anyhow::Result<Vec<u8>>>) {
    match self {
      Self::Legacy => {
        let (sender, receiver) = channel();
        (FileTransferRoute::Legacy(sender), receiver)
      }
      Self::DownloadCredit | Self::DownloadCreditHeartbeat => {
        let (sender, receiver) =
          channel_with_capacity(DOWNLOAD_ROUTE_CAPACITY);
        (FileTransferRoute::FlowControlled(sender), receiver)
      }
    }
  }

  fn begin(self) -> FileTransferMessage {
    match self {
      Self::Legacy => FileTransferMessage::Begin,
      Self::DownloadCredit => FileTransferMessage::BeginWithCredit {
        credits: DOWNLOAD_CREDIT_WINDOW,
      },
      Self::DownloadCreditHeartbeat => {
        FileTransferMessage::BeginWithCreditAndHeartbeat {
          credits: DOWNLOAD_CREDIT_WINDOW,
        }
      }
    }
  }

  fn returned_credit(
    self,
    message: &FileTransferMessage,
  ) -> Option<FileTransferMessage> {
    (matches!(
      self,
      Self::DownloadCredit | Self::DownloadCreditHeartbeat
    ) && matches!(message, FileTransferMessage::Chunk(_)))
    .then_some(FileTransferMessage::Credit { credits: 1 })
  }

  fn uses_heartbeat(self) -> bool {
    self == Self::DownloadCreditHeartbeat
  }
}

pub struct FileTransferConnection {
  pub channel: Uuid,
  pub sender: Sender<EncodedTransportMessage>,
  pub receiver: Receiver<anyhow::Result<Vec<u8>>>,
  pub channels: Arc<FileTransferChannels>,
  server_id: String,
  mode: FileTransferMode,
  heartbeat: Option<CancellationToken>,
  closed: bool,
}

impl FileTransferConnection {
  pub async fn send(
    &self,
    message: FileTransferMessage,
  ) -> anyhow::Result<()> {
    let sender = self.current_sender().await?;
    sender
      .send_file_transfer(self.channel, Ok(message.into_raw()))
      .await
  }

  async fn current_sender(
    &self,
  ) -> anyhow::Result<Sender<EncodedTransportMessage>> {
    if !self.mode.uses_heartbeat() {
      return Ok(self.sender.clone());
    }
    periphery_connections()
      .get(&self.server_id)
      .await
      .map(|connection| connection.sender.clone())
      .with_context(|| {
        format!("No connection found for server {}", self.server_id)
      })
  }

  pub async fn receive(
    &mut self,
  ) -> anyhow::Result<FileTransferMessage> {
    let bytes = self.receiver.recv().await??;
    let message = FileTransferMessage::from_raw(bytes)?;
    if let Some(credit) = self.mode.returned_credit(&message) {
      self
        .send(credit)
        .await
        .context("Failed to return file-transfer credit")?;
    }
    Ok(message)
  }

  pub async fn send_while_observing_incoming(
    &mut self,
    message: FileTransferMessage,
  ) -> anyhow::Result<Option<FileTransferMessage>> {
    let sender = self.current_sender().await?;
    let send =
      sender.send_file_transfer(self.channel, Ok(message.into_raw()));
    tokio::pin!(send);
    tokio::select! {
      biased;
      incoming = self.receive() => incoming.map(Some),
      result = &mut send => result.map(|_| None),
    }
  }

  pub async fn close(&mut self) {
    if let Some(heartbeat) = self.heartbeat.take() {
      heartbeat.cancel();
    }
    self.channels.remove(&self.channel).await;
    self.closed = true;
  }

  pub async fn abort(&mut self) {
    self.close().await;
    let channel = self.channel;
    let sender = self.current_sender().await.ok();
    tokio::spawn(async move {
      if let Some(sender) = sender {
        send_file_transfer_cancel_bounded(&sender, channel).await;
      }
    });
  }
}

impl Drop for FileTransferConnection {
  fn drop(&mut self) {
    if let Some(heartbeat) = self.heartbeat.take() {
      heartbeat.cancel();
    }
    if self.closed {
      return;
    }
    let channel = self.channel;
    let sender = self.sender.clone();
    let server_id = self.server_id.clone();
    let refresh_sender = self.mode.uses_heartbeat();
    let channels = self.channels.clone();
    tokio::spawn(async move {
      channels.remove(&channel).await;
      let sender = if refresh_sender {
        periphery_connections()
          .get(&server_id)
          .await
          .map(|connection| connection.sender.clone())
          .unwrap_or(sender)
      } else {
        sender
      };
      send_file_transfer_cancel_bounded(&sender, channel).await;
    });
  }
}

impl PeripheryClient {
  pub async fn start_file_manager_upload(
    &self,
    request: StartFileManagerUpload,
  ) -> anyhow::Result<FileTransferConnection> {
    let connection =
      periphery_connections().get(&self.id).await.with_context(
        || format!("No connection found for server {}", self.id),
      )?;
    let channel = self.request(request).await?;
    transfer_connection(
      connection,
      self.id.clone(),
      channel,
      FileTransferMode::Legacy,
    )
    .await
  }

  pub async fn start_file_manager_download(
    &self,
    request: StartFileManagerDownload,
  ) -> anyhow::Result<(
    StartFileManagerDownloadResponse,
    FileTransferConnection,
  )> {
    let connection =
      periphery_connections().get(&self.id).await.with_context(
        || format!("No connection found for server {}", self.id),
      )?;
    let response = self.request(request).await?;
    let mode = FileTransferMode::for_download(
      response.supports_download_credit,
      response.supports_download_heartbeat,
    );
    let transfer = transfer_connection(
      connection,
      self.id.clone(),
      response.channel,
      mode,
    )
    .await?;
    Ok((response, transfer))
  }
}

async fn transfer_connection(
  connection: Arc<crate::connection::PeripheryConnection>,
  server_id: String,
  channel_id: Uuid,
  mode: FileTransferMode,
) -> anyhow::Result<FileTransferConnection> {
  let (route, receiver) = mode.channel();
  connection.file_transfers.insert(channel_id, route).await;
  if let Err(error) = connection
    .sender
    .send_file_transfer(channel_id, Ok(mode.begin().into_raw()))
    .await
  {
    connection.file_transfers.remove(&channel_id).await;
    return Err(
      anyhow!(error).context("Failed to begin file transfer"),
    );
  }
  let heartbeat = mode
    .uses_heartbeat()
    .then(|| spawn_download_heartbeat(server_id.clone(), channel_id));
  Ok(FileTransferConnection {
    channel: channel_id,
    sender: connection.sender.clone(),
    receiver,
    channels: connection.file_transfers.clone(),
    server_id,
    mode,
    heartbeat,
    closed: false,
  })
}

fn spawn_download_heartbeat(
  server_id: String,
  channel: Uuid,
) -> CancellationToken {
  let cancel = CancellationToken::new();
  let task_cancel = cancel.clone();
  tokio::spawn(async move {
    loop {
      tokio::select! {
        _ = task_cancel.cancelled() => break,
        _ = tokio::time::sleep(DOWNLOAD_HEARTBEAT_INTERVAL) => {}
      }
      let Some(connection) =
        periphery_connections().get(&server_id).await
      else {
        continue;
      };
      let send = connection.sender.send_file_transfer(
        channel,
        Ok(FileTransferMessage::Heartbeat.into_raw()),
      );
      let _ =
        tokio::time::timeout(DOWNLOAD_HEARTBEAT_SEND_TIMEOUT, send)
          .await;
    }
  });
  cancel
}

#[cfg(test)]
mod tests {
  use super::*;
  use encoding::{Decode as _, WithChannel};
  use periphery_client::transport::TransportMessage;
  use std::time::Duration;

  #[test]
  fn download_credit_mode_uses_small_window_with_terminal_headroom() {
    let mode = FileTransferMode::for_download(true, false);
    assert_eq!(
      mode.begin(),
      FileTransferMessage::BeginWithCredit {
        credits: DOWNLOAD_CREDIT_WINDOW
      }
    );
    let (route, _receiver) = mode.channel();
    let FileTransferRoute::FlowControlled(sender) = route else {
      panic!("Expected flow-controlled download route");
    };
    for _ in 0..DOWNLOAD_ROUTE_CAPACITY {
      sender.try_send(Ok(Vec::new())).unwrap();
    }
    assert!(sender.try_send(Ok(Vec::new())).is_err());
    assert_eq!(
      mode.returned_credit(&FileTransferMessage::Chunk(vec![1])),
      Some(FileTransferMessage::Credit { credits: 1 })
    );
    assert_eq!(
      mode.returned_credit(&FileTransferMessage::Complete {
        bytes: 1,
        sha256: [0; 32],
      }),
      None
    );
  }

  #[test]
  fn heartbeat_download_mode_is_separately_negotiated() {
    let mode = FileTransferMode::for_download(true, true);
    assert_eq!(
      mode.begin(),
      FileTransferMessage::BeginWithCreditAndHeartbeat {
        credits: DOWNLOAD_CREDIT_WINDOW
      }
    );
    assert!(mode.uses_heartbeat());
    assert!(
      !FileTransferMode::for_download(true, false).uses_heartbeat()
    );
  }

  #[test]
  fn legacy_download_mode_preserves_begin_and_channel() {
    let mode = FileTransferMode::for_download(false, true);
    assert_eq!(mode.begin(), FileTransferMessage::Begin);
    assert_eq!(
      mode.returned_credit(&FileTransferMessage::Chunk(vec![1])),
      None
    );
    let (route, _receiver) = mode.channel();
    assert!(matches!(route, FileTransferRoute::Legacy(_)));
  }

  #[tokio::test]
  async fn abort_closes_the_route_and_sends_bounded_cancel() {
    let mode = FileTransferMode::DownloadCredit;
    let channel_id = Uuid::new_v4();
    let channels = Arc::new(FileTransferChannels::default());
    let (route, receiver) = mode.channel();
    channels.insert(channel_id, route).await;
    let (sender, mut outgoing) = channel::<EncodedTransportMessage>();
    let mut transfer = FileTransferConnection {
      channel: channel_id,
      sender,
      receiver,
      channels: channels.clone(),
      server_id: String::new(),
      mode,
      heartbeat: None,
      closed: false,
    };

    transfer.abort().await;

    assert!(channels.get(&channel_id).await.is_none());
    let encoded =
      tokio::time::timeout(Duration::from_secs(1), outgoing.recv())
        .await
        .unwrap()
        .unwrap();
    let TransportMessage::FileTransfer(encoded) =
      encoded.decode().unwrap()
    else {
      panic!("Expected file-transfer cancellation");
    };
    let WithChannel { channel, data } = encoded.decode().unwrap();
    assert_eq!(channel, channel_id);
    assert_eq!(
      FileTransferMessage::from_raw(data.unwrap()).unwrap(),
      FileTransferMessage::Cancel
    );
  }

  #[tokio::test]
  async fn abort_does_not_wait_for_a_stalled_cancel_send() {
    let mode = FileTransferMode::DownloadCredit;
    let channel_id = Uuid::new_v4();
    let channels = Arc::new(FileTransferChannels::default());
    let (route, receiver) = mode.channel();
    channels.insert(channel_id, route).await;
    let (sender, _outgoing) =
      channel_with_capacity::<EncodedTransportMessage>(1);
    sender
      .send_file_transfer(
        Uuid::new_v4(),
        Ok(FileTransferMessage::Cancel.into_raw()),
      )
      .await
      .unwrap();
    let mut transfer = FileTransferConnection {
      channel: channel_id,
      sender,
      receiver,
      channels: channels.clone(),
      server_id: String::new(),
      mode,
      heartbeat: None,
      closed: false,
    };

    tokio::time::timeout(
      Duration::from_millis(100),
      transfer.abort(),
    )
    .await
    .unwrap();

    assert!(channels.get(&channel_id).await.is_none());
  }

  #[tokio::test]
  async fn blocked_upload_send_observes_terminal_response() {
    let mode = FileTransferMode::Legacy;
    let channel_id = Uuid::new_v4();
    let channels = Arc::new(FileTransferChannels::default());
    let (route, receiver) = mode.channel();
    let FileTransferRoute::Legacy(incoming) = route.clone() else {
      panic!("Expected legacy upload route");
    };
    channels.insert(channel_id, route).await;
    let (sender, _outgoing) =
      channel_with_capacity::<EncodedTransportMessage>(1);
    sender
      .send_file_transfer(
        Uuid::new_v4(),
        Ok(FileTransferMessage::Cancel.into_raw()),
      )
      .await
      .unwrap();
    let mut transfer = FileTransferConnection {
      channel: channel_id,
      sender,
      receiver,
      channels: channels.clone(),
      server_id: String::new(),
      mode,
      heartbeat: None,
      closed: false,
    };
    incoming
      .send(Ok(FileTransferMessage::Cancel.into_raw()))
      .await
      .unwrap();

    let response = tokio::time::timeout(
      Duration::from_millis(100),
      transfer.send_while_observing_incoming(
        FileTransferMessage::Chunk(vec![1]),
      ),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(response, Some(FileTransferMessage::Cancel));
    transfer.close().await;
  }

  #[tokio::test]
  async fn drop_removes_route_without_waiting_for_stalled_cancel() {
    let mode = FileTransferMode::DownloadCredit;
    let channel_id = Uuid::new_v4();
    let channels = Arc::new(FileTransferChannels::default());
    let (route, receiver) = mode.channel();
    channels.insert(channel_id, route).await;
    let (sender, _outgoing) =
      channel_with_capacity::<EncodedTransportMessage>(1);
    sender
      .send_file_transfer(
        Uuid::new_v4(),
        Ok(FileTransferMessage::Cancel.into_raw()),
      )
      .await
      .unwrap();
    let transfer = FileTransferConnection {
      channel: channel_id,
      sender,
      receiver,
      channels: channels.clone(),
      server_id: String::new(),
      mode,
      heartbeat: None,
      closed: false,
    };

    drop(transfer);

    tokio::time::timeout(Duration::from_millis(100), async {
      while channels.get(&channel_id).await.is_some() {
        tokio::task::yield_now().await;
      }
    })
    .await
    .unwrap();
  }
}
