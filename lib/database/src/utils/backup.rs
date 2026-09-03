use std::{
  path::Path,
  sync::{Arc, atomic},
};

use anyhow::{Context, anyhow};
use async_compression::tokio::write::GzipEncoder;
use chrono::Local;
use futures_util::{
  SinkExt, StreamExt, TryStreamExt, stream::FuturesUnordered,
};
use mungos::mongodb::{
  Database,
  bson::{Document, RawDocumentBuf},
};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio_util::codec::{FramedWrite, LinesCodec};
use tracing::{error, info};

pub async fn backup(
  db: &Database,
  backups_folder: &Path,
) -> anyhow::Result<()> {
  backup_excluding(db, backups_folder, &[]).await
}

/// Create a logical database backup without exporting explicitly excluded
/// collections. Callers use this for sealed material that must not leave the
/// active Core database.
pub async fn backup_excluding(
  db: &Database,
  backups_folder: &Path,
  excluded_collections: &[&str],
) -> anyhow::Result<()> {
  let collections = db
    .list_collection_names()
    .await
    .context("Failed to list collections on source db")?
    .into_iter()
    .filter(|name| !excluded_collections.contains(&name.as_str()))
    .collect::<Vec<_>>();

  // Stats lives at the backup root so it is shared across dated exports.
  // Remove a prior export when the source has no Stats collection (or the
  // caller explicitly excludes it), otherwise a later restore could ingest
  // stale statistics that were not part of this backup.
  if !collections.iter().any(|name| name == "Stats") {
    match tokio::fs::remove_file(backups_folder.join("Stats.gz"))
      .await
    {
      Ok(()) => {}
      Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
      Err(error) => {
        return Err(error)
          .context("Failed to remove stale Stats backup");
      }
    }
  }

  let now_backups_folder = backups_folder
    .join(Local::now().format("%Y-%m-%d_%H-%M-%S").to_string());

  tokio::fs::create_dir_all(&now_backups_folder)
    .await
    .context("Failed to create backup folder")?;

  info!("Backing up to {now_backups_folder:?}...");

  let has_error = Arc::new(atomic::AtomicBool::new(false));

  let mut handles = collections
    .into_iter()
    .map(|collection| {
      let source = db.collection::<RawDocumentBuf>(&collection);
      let file_path = if collection == "Stats" {
        backups_folder.join("Stats.gz")
      } else {
        now_backups_folder.join(format!("{collection}.gz"))
      };
      let has_error = has_error.clone();
      tokio::spawn(async move {
        let res = async {
          let mut count = 0;
          let _ = tokio::fs::remove_file(&file_path).await;
          let file =
            tokio::fs::File::create(&file_path).await.with_context(
              || format!("Failed to create file at {file_path:?}"),
            )?;
          let mut writer = FramedWrite::new(
            BufWriter::new(GzipEncoder::with_quality(
              file,
              async_compression::Level::Best,
            )),
            LinesCodec::new(),
          );
          let mut cursor = source
            .find(Document::new())
            .await
            .context("Failed to query source collection")?;
          while let Some(doc) = cursor
            .try_next()
            .await
            .context("Failed to get next document")?
          {
            count += 1;
            let str = serde_json::to_string(&doc)
              .context("Failed to serialize document")?;
            writer
              .send(str)
              .await
              .context("Failed to write document to file")?;
          }

          <_ as SinkExt<String>>::flush(&mut writer)
            .await
            .context("Failed to flush writer")?;

          writer
            .into_inner()
            .shutdown()
            .await
            .context("Failed to shutdown writer compression")?;

          anyhow::Ok(count)
        }
        .await;
        match res {
          Ok(count) => {
            if count > 0 {
              info!("[{collection}]: Backed up {count} items");
            }
          }
          Err(e) => {
            error!("[{collection}]: {e:#}");
            has_error.store(true, atomic::Ordering::Relaxed);
          }
        }
      })
    })
    .collect::<FuturesUnordered<_>>();

  loop {
    match handles.next().await {
      Some(Ok(())) => {}
      Some(Err(e)) => {
        error!("{e:#}");
        has_error.store(true, atomic::Ordering::Relaxed);
      }
      None => break,
    }
  }

  if has_error.load(atomic::Ordering::Relaxed) {
    Err(anyhow!("Finished backing up database with errors 🚨"))
  } else {
    info!("Finished backing up database ✅");
    Ok(())
  }
}
