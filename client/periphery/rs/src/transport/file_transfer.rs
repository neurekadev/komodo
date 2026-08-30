use anyhow::{Context, anyhow};

const BEGIN: u8 = 0;
const CHUNK: u8 = 1;
const COMPLETE: u8 = 2;
const CANCEL: u8 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileTransferMessage {
  Begin,
  Chunk(Vec<u8>),
  Complete { bytes: u64, sha256: [u8; 32] },
  Cancel,
}

impl FileTransferMessage {
  pub fn into_raw(self) -> Vec<u8> {
    match self {
      FileTransferMessage::Begin => vec![BEGIN],
      FileTransferMessage::Chunk(mut bytes) => {
        bytes.push(CHUNK);
        bytes
      }
      FileTransferMessage::Complete { bytes, sha256 } => {
        let mut data = Vec::with_capacity(41);
        data.extend_from_slice(&bytes.to_le_bytes());
        data.extend_from_slice(&sha256);
        data.push(COMPLETE);
        data
      }
      FileTransferMessage::Cancel => vec![CANCEL],
    }
  }

  pub fn from_raw(mut data: Vec<u8>) -> anyhow::Result<Self> {
    let variant =
      data.pop().context("File-transfer message is empty")?;
    match variant {
      BEGIN if data.is_empty() => Ok(FileTransferMessage::Begin),
      CHUNK => Ok(FileTransferMessage::Chunk(data)),
      COMPLETE if data.len() == 40 => {
        let bytes = u64::from_le_bytes(
          data[..8]
            .try_into()
            .map_err(|_| anyhow!("Invalid transfer byte count"))?,
        );
        let sha256 = data[8..]
          .try_into()
          .map_err(|_| anyhow!("Invalid transfer checksum"))?;
        Ok(FileTransferMessage::Complete { bytes, sha256 })
      }
      CANCEL if data.is_empty() => Ok(FileTransferMessage::Cancel),
      _ => Err(anyhow!("Invalid file-transfer message")),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn transfer_messages_round_trip() {
    for message in [
      FileTransferMessage::Begin,
      FileTransferMessage::Chunk(vec![0, 1, 2, 255]),
      FileTransferMessage::Complete {
        bytes: 42,
        sha256: [7; 32],
      },
      FileTransferMessage::Cancel,
    ] {
      assert_eq!(
        FileTransferMessage::from_raw(message.clone().into_raw())
          .unwrap(),
        message
      );
    }
  }
}
