use axum::{Extension, Router, routing::get};
use komodo_client::entities::user::User;
use mogh_auth_server::middleware::authenticate_request;
use mogh_error::Json;
use mogh_server::{
  cors::cors_layer, session::memory_session_layer,
  ui::serve_static_ui,
};

use crate::{auth::KomodoAuthImpl, config::core_config, ts_client};

pub mod execute;
pub mod read;
pub mod write;

mod file_manager;

mod listener;
mod openapi;
mod terminal;
mod ws;

#[derive(serde::Deserialize)]
struct Variant {
  variant: String,
}

pub async fn app() -> anyhow::Result<Router> {
  let config = core_config();
  let mut vykar_router = Router::new();
  match crate::backup::get_settings().await {
    Ok(settings) => {
      if let komodo_client::entities::backup::BackupRepositoryBackend::CoreLocal {
        path,
      } = &settings.primary.backend
      {
        vykar_router = vykar_router.nest(
          "/vykar/primary",
          crate::backup::embedded_vykar_router(
            std::path::Path::new(path),
            false,
          )?,
        );
      }
      if let Some(mirror) = &settings.mirror
        && let komodo_client::entities::backup::BackupRepositoryBackend::CoreLocal {
          path,
        } = &mirror.backend
      {
        vykar_router = vykar_router.nest(
          "/vykar/mirror",
          crate::backup::embedded_vykar_router(
            std::path::Path::new(path),
            true,
          )?,
        );
      }
    }
    Err(error) => crate::backup::record_configuration_alert(&error),
  }
  Ok(
    Router::new()
      .merge(openapi::serve_docs())
      .route("/version", get(|| async { env!("CARGO_PKG_VERSION") }))
      .nest(
        "/auth",
        mogh_auth_server::api::router::<KomodoAuthImpl>().layer(
          axum::middleware::from_fn(
            crate::auth::middleware::backup_mutation_guard,
          ),
        ),
      )
      .nest("/user", user_router())
      .nest("/read", read::router())
      .nest("/write", write::router())
      .nest("/execute", execute::router())
      .nest("/terminal", terminal::router())
      .nest("/file-manager", file_manager::router())
      .nest("/listener", listener::router())
      .nest("/ws", ws::router())
      .nest("/client", ts_client::router())
      .merge(vykar_router)
      .layer(memory_session_layer(config))
      .fallback_service(serve_static_ui(
        &config.ui_path,
        config.ui_index_force_no_cache,
      ))
      .layer(cors_layer(config)),
  )
}

fn user_router() -> Router {
  Router::new()
    .route(
      "/",
      get(|Extension(user): Extension<User>| async { Json(user) }),
    )
    .layer(axum::middleware::from_fn(
      authenticate_request::<KomodoAuthImpl, false>,
    ))
}
