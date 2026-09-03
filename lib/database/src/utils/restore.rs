use std::path::{Path, PathBuf};

use anyhow::Context;
use async_compression::tokio::bufread::GzipDecoder;
use futures_util::{
  StreamExt, TryStreamExt, stream::FuturesUnordered,
};
use mungos::{
  bulk_update::{BulkUpdate, bulk_update_retry_too_big},
  mongodb::{
    Database,
    bson::{Bson, Document, doc},
  },
};
use tokio::io::BufReader;
use tokio_util::codec::{FramedRead, LinesCodec};
use tracing::{error, info};

pub async fn restore(
  db: &Database,
  backups_folder: &Path,
  restore_folder: Option<&Path>,
) -> anyhow::Result<()> {
  // Get the specific dated folder to restore contents of
  let restore_folder = if let Some(restore_folder) = restore_folder {
    backups_folder.join(restore_folder)
  } else {
    latest_restore_folder(backups_folder).await?
  }
  .components()
  .collect::<PathBuf>();

  info!("Restore folder: {restore_folder:?}");

  let restore_files =
    get_restore_files(backups_folder, &restore_folder).await?;

  let mut handles = restore_files
    .into_iter()
    .map(|(collection, restore_file)| {
      let db = db.clone();
      async {
        let col = collection.clone();
        tokio::join!(
          async { col },
          tokio::spawn(async move {
            let res = async {
              let mut buffer = Vec::<BulkUpdate>::new();
              // The update collection is bigger than others,
              // can hit the max bson limit on the bulk upsert call without this.
              let max_buffer = if collection == "Update" {
                1_000
              } else {
                10_000
              };
              let mut count = 0;
              let mut line_number = 0;

              let file = tokio::fs::File::open(&restore_file)
                .await
                .with_context(|| {
                format!("Failed to open file {restore_file:?}")
              })?;

              let mut reader = FramedRead::new(
                GzipDecoder::new(BufReader::new(file)),
                LinesCodec::new(),
              );

              while let Some(line) = reader
                .try_next()
                .await
                .context("Failed to get next line")?
              {
                line_number += 1;
                if line.is_empty() {
                  continue;
                }
                let update = restore_document_update(
                  &line,
                  &collection,
                  line_number,
                )?;
                count += 1;
                buffer.push(update);
                if buffer.len() >= max_buffer {
                  bulk_update_retry_too_big(
                    &db,
                    &collection,
                    &buffer,
                    true,
                  )
                  .await
                  .context("Failed to flush documents")?;
                  buffer.clear();
                }
              }
              if !buffer.is_empty() {
                bulk_update_retry_too_big(
                  &db,
                  &collection,
                  &buffer,
                  true,
                )
                .await
                .context("Failed to flush documents")?;
              }
              anyhow::Ok(count)
            }
            .await;
            match &res {
              Ok(count) => {
                if *count > 0 {
                  info!("[{collection}]: Restored {count} items");
                }
              }
              Err(e) => {
                error!("[{collection}]: {e:#}");
              }
            }
            res.map(|_| ())
          })
        )
      }
    })
    .collect::<FuturesUnordered<_>>();

  let mut failures = Vec::new();
  loop {
    match handles.next().await {
      Some((_collection, Ok(Ok(())))) => {}
      Some((collection, Ok(Err(error)))) => {
        failures.push(format!("{collection}: {error:#}"));
      }
      Some((collection, Err(e))) => {
        failures.push(format!("{collection}: worker failed: {e:#}"));
      }
      None => break,
    }
  }

  if !failures.is_empty() {
    return Err(anyhow::anyhow!(
      "Database restore failed: {}",
      failures.join("; ")
    ));
  }

  info!("Finished restoring database ✅");

  Ok(())
}

fn restore_id_query(id: Bson) -> Document {
  doc! { "_id": id }
}

fn restore_document_update(
  line: &str,
  collection: &str,
  line_number: usize,
) -> anyhow::Result<BulkUpdate> {
  let mut document = serde_json::from_str::<Document>(line)
    .with_context(|| {
      format!(
        "Failed to deserialize restore document in collection '{collection}' at line {line_number}"
      )
    })?;
  let id = document.remove("_id").with_context(|| {
    format!(
      "Restore document in collection '{collection}' at line {line_number} has no _id"
    )
  })?;
  Ok(BulkUpdate {
    query: restore_id_query(id),
    update: doc! { "$set": document },
  })
}

async fn latest_restore_folder(
  backups_folder: &Path,
) -> anyhow::Result<PathBuf> {
  let mut max = PathBuf::new();
  let mut backups_dir = tokio::fs::read_dir(backups_folder)
    .await
    .context("Failed to read backup directory")?;
  loop {
    match backups_dir
      .next_entry()
      .await
      .context("Failed to read backup dir entry")
    {
      Ok(Some(entry)) => {
        let path = entry.path();
        if path.is_dir() && path > max {
          max = path;
        }
      }
      Ok(None) => break,
      Err(e) => {
        return Err(e);
      }
    }
  }
  Ok(max.components().collect())
}

async fn get_restore_files(
  backups_folder: &Path,
  restore_folder: &Path,
) -> anyhow::Result<Vec<(String, PathBuf)>> {
  let mut restore_dir =
    tokio::fs::read_dir(restore_folder).await.with_context(|| {
      format!("Failed to read restore directory {restore_folder:?}")
    })?;

  let stats_file = backups_folder.join("Stats.gz");
  let mut restore_files: Vec<(String, PathBuf)> = Vec::new();
  if tokio::fs::try_exists(&stats_file)
    .await
    .context("Failed to inspect optional Stats backup")?
  {
    restore_files.push((
      String::from("Stats"),
      stats_file.components().collect(),
    ));
  }

  loop {
    match restore_dir
      .next_entry()
      .await
      .context("Failed to read restore dir entry")
    {
      Ok(Some(file)) => {
        let path = file.path();
        let Some(file_name) = path.file_name() else {
          continue;
        };
        let Some(file_name) = file_name.to_str() else {
          continue;
        };
        let Some(collection) = file_name.strip_suffix(".gz") else {
          continue;
        };
        restore_files.push((
          collection.to_string(),
          path.components().collect(),
        ));
      }
      Ok(None) => break,
      Err(e) => {
        return Err(e);
      }
    }
  }

  Ok(restore_files)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn restore_document_decode_failures_are_fatal() {
    for line in ["not-json", r#"{"_id":{"$oid":"not-an-object-id"}}"#]
    {
      let error = restore_document_update(line, "User", 7)
        .err()
        .expect("invalid document must fail the import");
      let message = error.to_string();
      assert!(message.contains("collection 'User'"));
      assert!(message.contains("line 7"));
    }
  }

  #[test]
  fn restore_document_requires_an_id_and_preserves_fields() {
    assert!(
      restore_document_update(r#"{"name":"admin"}"#, "User", 1)
        .is_err()
    );
    let update = restore_document_update(
      r#"{"_id":"user-id","name":"admin"}"#,
      "User",
      2,
    )
    .unwrap();
    assert_eq!(update.query.get_str("_id").unwrap(), "user-id");
    let fields = update.update.get_document("$set").unwrap();
    assert_eq!(fields.get_str("name").unwrap(), "admin");
    assert!(!fields.contains_key("_id"));
  }

  #[tokio::test]
  async fn stats_backup_is_optional() {
    let root = tempfile::tempdir().unwrap();
    let restore_folder = root.path().join("snapshot");
    tokio::fs::create_dir(&restore_folder).await.unwrap();
    tokio::fs::write(restore_folder.join("Stack.gz"), b"")
      .await
      .unwrap();

    let without_stats =
      get_restore_files(root.path(), &restore_folder)
        .await
        .unwrap();
    assert_eq!(without_stats.len(), 1);
    assert_eq!(without_stats[0].0, "Stack");

    tokio::fs::write(root.path().join("Stats.gz"), b"")
      .await
      .unwrap();
    let with_stats = get_restore_files(root.path(), &restore_folder)
      .await
      .unwrap();
    assert!(
      with_stats
        .iter()
        .any(|(collection, _)| collection == "Stats")
    );
  }

  #[test]
  fn restore_preserves_string_and_object_id_types() {
    let string = restore_id_query(Bson::String("stack-id".into()));
    assert_eq!(string.get_str("_id").unwrap(), "stack-id");

    let object_id = mungos::mongodb::bson::oid::ObjectId::new();
    let object = restore_id_query(Bson::ObjectId(object_id));
    assert_eq!(object.get_object_id("_id").unwrap(), object_id);
  }
}
