use std::sync::Arc;

use anyhow::{Context as _, anyhow};
use periphery_client::{
  api::file_manager::{
    StartFileManagerDownload, StartFileManagerDownloadResponse,
    StartFileManagerUpload,
  },
  transport::{EncodedTransportMessage, FileTransferMessage},
};
use transport::channel::{Receiver, Sender, channel};
use uuid::Uuid;

use crate::{
  connection::FileTransferChannels, periphery::PeripheryClient,
  state::periphery_connections,
};

pub struct FileTransferConnection {
  pub channel: Uuid,
  pub sender: Sender<EncodedTransportMessage>,
  pub receiver: Receiver<anyhow::Result<Vec<u8>>>,
  pub channels: Arc<FileTransferChannels>,
  closed: bool,
}

impl FileTransferConnection {
  pub async fn send(
    &self,
    message: FileTransferMessage,
  ) -> anyhow::Result<()> {
    self
      .sender
      .send_file_transfer(self.channel, Ok(message.into_raw()))
      .await
  }

  pub async fn receive(
    &mut self,
  ) -> anyhow::Result<FileTransferMessage> {
    let bytes = self.receiver.recv().await??;
    FileTransferMessage::from_raw(bytes)
  }

  pub async fn close(&mut self) {
    self.channels.remove(&self.channel).await;
    self.closed = true;
  }
}

impl Drop for FileTransferConnection {
  fn drop(&mut self) {
    if self.closed {
      return;
    }
    let channel = self.channel;
    let sender = self.sender.clone();
    let channels = self.channels.clone();
    tokio::spawn(async move {
      channels.remove(&channel).await;
      let _ = sender
        .send_file_transfer(
          channel,
          Ok(FileTransferMessage::Cancel.into_raw()),
        )
        .await;
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
    transfer_connection(connection, channel).await
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
    let transfer =
      transfer_connection(connection, response.channel).await?;
    Ok((response, transfer))
  }
}

async fn transfer_connection(
  connection: Arc<crate::connection::PeripheryConnection>,
  channel_id: Uuid,
) -> anyhow::Result<FileTransferConnection> {
  let (sender, receiver) = channel();
  connection.file_transfers.insert(channel_id, sender).await;
  if let Err(error) = connection
    .sender
    .send_file_transfer(
      channel_id,
      Ok(FileTransferMessage::Begin.into_raw()),
    )
    .await
  {
    connection.file_transfers.remove(&channel_id).await;
    return Err(
      anyhow!(error).context("Failed to begin file transfer"),
    );
  }
  Ok(FileTransferConnection {
    channel: channel_id,
    sender: connection.sender.clone(),
    receiver,
    channels: connection.file_transfers.clone(),
    closed: false,
  })
}
