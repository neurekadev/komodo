use anyhow::{Context, anyhow};

const BEGIN: u8 = 0;
const CHUNK: u8 = 1;
const COMPLETE: u8 = 2;
const CANCEL: u8 = 3;
const BEGIN_WITH_CREDIT: u8 = 4;
const CREDIT: u8 = 5;
const HEARTBEAT: u8 = 6;
const BEGIN_WITH_CREDIT_AND_HEARTBEAT: u8 = 7;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileTransferMessage {
  Begin,
  BeginWithCredit { credits: u32 },
  BeginWithCreditAndHeartbeat { credits: u32 },
  Chunk(Vec<u8>),
  Complete { bytes: u64, sha256: [u8; 32] },
  Cancel,
  Credit { credits: u32 },
  Heartbeat,
}

impl FileTransferMessage {
  pub fn into_raw(self) -> Vec<u8> {
    match self {
      FileTransferMessage::Begin => vec![BEGIN],
      FileTransferMessage::BeginWithCredit { credits } => {
        let mut data = Vec::with_capacity(5);
        data.extend_from_slice(&credits.to_le_bytes());
        data.push(BEGIN_WITH_CREDIT);
        data
      }
      FileTransferMessage::BeginWithCreditAndHeartbeat {
        credits,
      } => {
        let mut data = Vec::with_capacity(5);
        data.extend_from_slice(&credits.to_le_bytes());
        data.push(BEGIN_WITH_CREDIT_AND_HEARTBEAT);
        data
      }
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
      FileTransferMessage::Credit { credits } => {
        let mut data = Vec::with_capacity(5);
        data.extend_from_slice(&credits.to_le_bytes());
        data.push(CREDIT);
        data
      }
      FileTransferMessage::Heartbeat => vec![HEARTBEAT],
    }
  }

  pub fn from_raw(mut data: Vec<u8>) -> anyhow::Result<Self> {
    let variant =
      data.pop().context("File-transfer message is empty")?;
    match variant {
      BEGIN if data.is_empty() => Ok(FileTransferMessage::Begin),
      BEGIN_WITH_CREDIT if data.len() == 4 => {
        let credits =
          u32::from_le_bytes(data.try_into().map_err(|_| {
            anyhow!("Invalid initial transfer credit")
          })?);
        Ok(FileTransferMessage::BeginWithCredit { credits })
      }
      BEGIN_WITH_CREDIT_AND_HEARTBEAT if data.len() == 4 => {
        let credits =
          u32::from_le_bytes(data.try_into().map_err(|_| {
            anyhow!("Invalid initial heartbeat transfer credit")
          })?);
        Ok(FileTransferMessage::BeginWithCreditAndHeartbeat {
          credits,
        })
      }
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
      CREDIT if data.len() == 4 => {
        let credits = u32::from_le_bytes(
          data
            .try_into()
            .map_err(|_| anyhow!("Invalid transfer credit"))?,
        );
        Ok(FileTransferMessage::Credit { credits })
      }
      HEARTBEAT if data.is_empty() => {
        Ok(FileTransferMessage::Heartbeat)
      }
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
      FileTransferMessage::BeginWithCredit { credits: 8 },
      FileTransferMessage::BeginWithCreditAndHeartbeat { credits: 8 },
      FileTransferMessage::Chunk(vec![0, 1, 2, 255]),
      FileTransferMessage::Complete {
        bytes: 42,
        sha256: [7; 32],
      },
      FileTransferMessage::Cancel,
      FileTransferMessage::Credit { credits: 3 },
      FileTransferMessage::Heartbeat,
    ] {
      assert_eq!(
        FileTransferMessage::from_raw(message.clone().into_raw())
          .unwrap(),
        message
      );
    }
  }
}
