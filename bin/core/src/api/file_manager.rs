use std::time::Duration;

use anyhow::{Context as _, anyhow};
use axum::{
  Extension, Router,
  body::{Body, Bytes},
  extract::Path,
  http::{HeaderValue, Response, header},
  middleware,
  routing::{get, post},
};
use futures_util::{StreamExt as _, stream};
use komodo_client::entities::{
  Operation, ResourceTarget,
  file_manager::FileManagerOperationStatus,
  permission::PermissionLevel, stack::StackFileDependency,
  user::User,
};
use mogh_auth_server::middleware::authenticate_request;
use mogh_error::Json;
use periphery_client::{
  api::{
    compose::{
      GetComposeContentsOnHost, GetComposeContentsOnHostResponse,
    },
    file_manager::{
      StartFileManagerDownload, StartFileManagerUpload,
    },
  },
  transport::FileTransferMessage,
};
use sha2::{Digest as _, Sha256};

use crate::{
  auth::KomodoAuthImpl,
  file_manager::{
    ResolvedFileManagerTarget, TransferSessionKind, cancel_operation,
    complete_operation, consume_transfer_session, fail_operation,
    resolve_target,
  },
  helpers::{
    periphery_client,
    update::{add_update, make_update},
  },
};

struct DownloadAudit {
  operation_id: String,
  target: ResourceTarget,
  paths: Vec<String>,
  user: User,
  finalized: bool,
}

impl DownloadAudit {
  async fn finish(&mut self, result: anyhow::Result<Vec<String>>) {
    self.finalized = true;
    let operation_error =
      result.as_ref().err().map(ToString::to_string);
    let audit = audit_transfer(
      self.target.clone(),
      "Download files",
      result,
      &self.user,
    )
    .await;
    if let Some(error) = operation_error {
      fail_operation(&self.operation_id, error);
    } else if let Err(error) = audit {
      fail_operation(&self.operation_id, error.to_string());
    } else {
      complete_operation(&self.operation_id);
    }
  }
}

impl Drop for DownloadAudit {
  fn drop(&mut self) {
    if self.finalized {
      return;
    }
    cancel_operation(
      &self.operation_id,
      "Download was cancelled before completion",
    );
    let target = self.target.clone();
    let user = self.user.clone();
    tokio::spawn(async move {
      let _ = audit_transfer(
        target,
        "Download files",
        Err(anyhow!("Download was cancelled before completion")),
        &user,
      )
      .await;
    });
  }
}

async fn legacy_managed_download_bytes(
  resolved: &ResolvedFileManagerTarget,
  path: &str,
) -> anyhow::Result<(String, Vec<u8>)> {
  let managed_file = resolved
    .managed_file
    .as_deref()
    .context("Managed compose download target is unavailable")?;
  if path != managed_file {
    return Err(anyhow!(
      "Managed compose compatibility download path changed"
    ));
  }
  let stack = resolved
    .stack
    .as_ref()
    .context("Managed compose stack snapshot is unavailable")?;
  let compose_path = stack
    .compose_file_paths()
    .first()
    .cloned()
    .context("Managed stack does not declare a compose file")?;
  let GetComposeContentsOnHostResponse {
    mut contents,
    errors,
  } = periphery_client(&resolved.server)
    .await?
    .request(GetComposeContentsOnHost {
      name: stack.name.clone(),
      run_directory: String::new(),
      file_paths: vec![StackFileDependency::full_redeploy(
        compose_path.clone(),
      )],
    })
    .await?;
  let position = contents
    .iter()
    .position(|contents| contents.path == compose_path);
  let Some(position) = position else {
    let detail = errors
      .iter()
      .find(|error| error.path == compose_path)
      .map(|error| error.contents.as_str())
      .unwrap_or(
        "Periphery did not return the requested compose file",
      );
    return Err(anyhow!(
      "Managed compose compatibility download failed: {detail}"
    ));
  };
  Ok((
    managed_file.to_string(),
    contents.swap_remove(position).contents.into_bytes(),
  ))
}

fn fallback_download_response(
  file_name: String,
  contents: Vec<u8>,
  audit: DownloadAudit,
) -> anyhow::Result<Response<Body>> {
  let total_bytes = contents.len() as u64;
  let sha256 = hex::encode(Sha256::digest(&contents));
  let stream = stream::unfold(
    Some((Some(Bytes::from(contents)), audit)),
    |state| async move {
      let (contents, mut audit) = state?;
      if let Some(contents) = contents {
        Some((
          Ok::<Bytes, std::io::Error>(contents),
          Some((None, audit)),
        ))
      } else {
        let paths = audit.paths.clone();
        audit.finish(Ok(paths)).await;
        None
      }
    },
  );
  build_download_response(
    Body::from_stream(stream),
    &file_name,
    total_bytes,
    &sha256,
  )
}

fn build_download_response(
  body: Body,
  file_name: &str,
  total_bytes: u64,
  sha256: &str,
) -> anyhow::Result<Response<Body>> {
  let disposition = format!(
    "attachment; filename=\"{}\"",
    safe_download_name(file_name)
  );
  let mut response = Response::new(body);
  response.headers_mut().insert(
    header::CONTENT_TYPE,
    HeaderValue::from_static("application/octet-stream"),
  );
  response.headers_mut().insert(
    header::CONTENT_DISPOSITION,
    HeaderValue::from_str(&disposition)?,
  );
  response.headers_mut().insert(
    header::CONTENT_LENGTH,
    HeaderValue::from_str(&total_bytes.to_string())?,
  );
  response
    .headers_mut()
    .insert("x-komodo-sha256", HeaderValue::from_str(sha256)?);
  Ok(response)
}

fn premature_upload_response(
  message: FileTransferMessage,
) -> anyhow::Error {
  match message {
    FileTransferMessage::Cancel => {
      anyhow!("Periphery cancelled the upload")
    }
    FileTransferMessage::Complete { .. } => anyhow!(
      "Periphery completed the upload before Core finished sending it"
    ),
    _ => anyhow!("Periphery sent an invalid upload response"),
  }
}

pub fn router() -> Router {
  Router::new()
    .route("/upload/{token}", post(upload))
    .route("/download/{token}", get(download))
    .layer(middleware::from_fn(
      authenticate_request::<KomodoAuthImpl, true>,
    ))
}

async fn upload(
  Extension(user): Extension<User>,
  Path(token): Path<String>,
  body: Body,
) -> mogh_error::Result<Json<FileManagerOperationStatus>> {
  // A ticket can predate a backup/restore. Keep the barrier through body
  // forwarding, Periphery publication acknowledgement, and the audit write.
  let _mutation_guard =
    crate::backup::mutation_barrier().read().await;
  let session = consume_transfer_session(&token, &user.id)?;
  let TransferSessionKind::Upload {
    destination,
    file_name,
    total_bytes,
    overwrite,
    expected_revision,
  } = session.kind
  else {
    fail_operation(
      &session.operation_id,
      "Transfer token is not an upload token",
    );
    return Err(
      anyhow!("Transfer token is not an upload token").into(),
    );
  };
  let resolved = match resolve_target(
    &session.target,
    &user,
    PermissionLevel::Write,
  )
  .await
  {
    Ok(resolved) => resolved,
    Err(error) => {
      fail_operation(&session.operation_id, error.to_string());
      return Err(error.into());
    }
  };
  let path = if destination.is_empty() {
    file_name.clone()
  } else {
    format!("{destination}/{file_name}")
  };
  let result = async {
    let mut transfer = periphery_client(&resolved.server)
      .await?
      .start_file_manager_upload(StartFileManagerUpload {
        target: resolved.periphery,
        actor: user.id.clone(),
        operation_id: session.operation_id.clone(),
        destination,
        file_name,
        total_bytes,
        overwrite,
        expected_revision,
      })
      .await?;
    let mut stream = body.into_data_stream();
    let mut bytes = 0_u64;
    let mut hasher = Sha256::new();
    loop {
      let next_chunk = tokio::select! {
        biased;
        message = transfer.receive() => {
          let message = message.context(
            "Periphery upload failed while forwarding the request body"
          )?;
          return Err(premature_upload_response(message));
        }
        chunk = stream.next() => chunk,
      };
      let Some(chunk) = next_chunk else {
        break;
      };
      let chunk = chunk.context("Failed to read upload body")?;
      bytes = bytes
        .checked_add(chunk.len() as u64)
        .context("Upload size overflow")?;
      if bytes > total_bytes {
        transfer.abort().await;
        return Err(anyhow!("Upload exceeded its declared size"));
      }
      hasher.update(&chunk);
      if let Some(message) = transfer
        .send_while_observing_incoming(FileTransferMessage::Chunk(
          chunk.to_vec(),
        ))
        .await
        .context(
          "Periphery upload failed while forwarding the request body",
        )?
      {
        return Err(premature_upload_response(message));
      }
    }
    let sha256: [u8; 32] = hasher.finalize().into();
    transfer
      .send(FileTransferMessage::Complete { bytes, sha256 })
      .await?;
    let acknowledgement = tokio::time::timeout(
      Duration::from_secs(60),
      transfer.receive(),
    )
    .await
    .context("Upload acknowledgement timed out")??;
    transfer.close().await;
    match acknowledgement {
      FileTransferMessage::Complete {
        bytes: acknowledged,
        sha256: acknowledged_hash,
      } if acknowledged == bytes && acknowledged_hash == sha256 => {
        Ok((bytes, hex::encode(sha256)))
      }
      _ => Err(anyhow!("Upload acknowledgement was invalid")),
    }
  }
  .await;

  match result {
    Ok((bytes, _checksum)) => {
      if let Err(error) = audit_transfer(
        resolved.resource,
        "Upload file",
        Ok(vec![path]),
        &user,
      )
      .await
      {
        fail_operation(&session.operation_id, error.to_string());
        return Err(error.into());
      }
      complete_operation(&session.operation_id);
      Ok(Json(FileManagerOperationStatus {
        operation_id: session.operation_id,
        state: komodo_client::entities::file_manager::FileManagerOperationState::Complete,
        phase: komodo_client::entities::file_manager::FileManagerOperationPhase::Finalizing,
        description: "Upload file".into(),
        completed_entries: 1,
        total_entries: 1,
        completed_bytes: bytes,
        total_bytes: bytes,
        error: None,
        ..Default::default()
      }))
    }
    Err(error) => {
      fail_operation(&session.operation_id, error.to_string());
      audit_transfer(
        resolved.resource,
        "Upload file",
        Err(anyhow!(error.to_string())),
        &user,
      )
      .await?;
      Err(error.into())
    }
  }
}

async fn download(
  Extension(user): Extension<User>,
  Path(token): Path<String>,
) -> mogh_error::Result<Response<Body>> {
  let session = consume_transfer_session(&token, &user.id)?;
  let TransferSessionKind::Download {
    paths,
    allow_managed,
  } = session.kind
  else {
    fail_operation(
      &session.operation_id,
      "Transfer token is not a download token",
    );
    return Err(
      anyhow!("Transfer token is not a download token").into(),
    );
  };
  let resolved = match resolve_target(
    &session.target,
    &user,
    PermissionLevel::Read,
  )
  .await
  {
    Ok(resolved) => resolved,
    Err(error) => {
      fail_operation(&session.operation_id, error.to_string());
      return Err(error.into());
    }
  };
  let result = async {
    periphery_client(&resolved.server)
      .await?
      .start_file_manager_download(StartFileManagerDownload {
        target: resolved.periphery.clone(),
        actor: user.id.clone(),
        operation_id: session.operation_id.clone(),
        paths: paths.clone(),
        allow_managed,
      })
      .await
  }
  .await;
  let (metadata, transfer) = match result {
    Ok(result) => result,
    Err(stream_error) if allow_managed => {
      let fallback = match paths.as_slice() {
        [path] => {
          legacy_managed_download_bytes(&resolved, path).await
        }
        _ => Err(anyhow!(
          "Managed compose compatibility download expected one path"
        )),
      };
      match fallback {
        Ok((file_name, contents)) => {
          let audit = DownloadAudit {
            operation_id: session.operation_id,
            target: resolved.resource,
            paths,
            user,
            finalized: false,
          };
          return Ok(fallback_download_response(
            file_name, contents, audit,
          )?);
        }
        Err(fallback_error) => {
          let error = anyhow!(
            "Managed download streaming failed: {stream_error:#}; compatibility fallback also failed: {fallback_error:#}"
          );
          fail_operation(&session.operation_id, error.to_string());
          audit_transfer(
            resolved.resource,
            "Download files",
            Err(anyhow!(error.to_string())),
            &user,
          )
          .await?;
          return Err(error.into());
        }
      }
    }
    Err(error) => {
      fail_operation(&session.operation_id, error.to_string());
      audit_transfer(
        resolved.resource,
        "Download files",
        Err(anyhow!(error.to_string())),
        &user,
      )
      .await?;
      return Err(error.into());
    }
  };
  let expected_bytes = metadata.total_bytes;
  let expected_hash = metadata.sha256.clone();
  let audit = DownloadAudit {
    operation_id: session.operation_id,
    target: resolved.resource,
    paths,
    user,
    finalized: false,
  };
  let stream = stream::unfold(
    Some((transfer, 0_u64, Sha256::new(), audit)),
    move |state| {
      let expected_hash = expected_hash.clone();
      async move {
        let (mut transfer, mut bytes, mut hasher, mut audit) = state?;
        let message = match transfer.receive().await {
          Ok(message) => message,
          Err(error) => {
            transfer.abort().await;
            let message = error.to_string();
            audit.finish(Err(anyhow!(message.clone()))).await;
            return Some((Err(std::io::Error::other(message)), None));
          }
        };
        match message {
          FileTransferMessage::Chunk(chunk) => {
            bytes += chunk.len() as u64;
            hasher.update(&chunk);
            Some((
              Ok::<Bytes, std::io::Error>(chunk.into()),
              Some((transfer, bytes, hasher, audit)),
            ))
          }
          FileTransferMessage::Complete {
            bytes: sent,
            sha256,
          } if sent == bytes
            && sent == expected_bytes
            && hex::encode(sha256) == expected_hash
            && hex::encode(hasher.finalize()) == expected_hash =>
          {
            transfer.close().await;
            audit.finish(Ok(audit.paths.clone())).await;
            None
          }
          _ => {
            transfer.abort().await;
            audit
              .finish(Err(anyhow!(
                "Download byte count or checksum verification failed"
              )))
              .await;
            Some((
              Err(std::io::Error::other(
                "Download byte count or checksum verification failed",
              )),
              None,
            ))
          }
        }
      }
    },
  );
  Ok(build_download_response(
    Body::from_stream(stream),
    &metadata.file_name,
    metadata.total_bytes,
    &metadata.sha256,
  )?)
}

fn safe_download_name(name: &str) -> String {
  let name = name
    .chars()
    .filter(|character| {
      character.is_ascii_graphic()
        && !matches!(character, '"' | '\\' | '/' | ';')
    })
    .collect::<String>();
  if name.is_empty() {
    "komodo-download".to_string()
  } else {
    name
  }
}

async fn audit_transfer(
  target: ResourceTarget,
  operation: &str,
  result: anyhow::Result<Vec<String>>,
  user: &User,
) -> anyhow::Result<()> {
  let mut update = make_update(target, Operation::FileManager, user);
  match result {
    Ok(paths) => update.push_simple_log(
      operation,
      format!("Affected paths: {}", paths.join(", ")),
    ),
    Err(error) => update.push_error_log(operation, error.to_string()),
  }
  update.finalize();
  add_update(update).await?;
  Ok(())
}
