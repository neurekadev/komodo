use std::str::FromStr;

use anyhow::{Context, anyhow};
use database::mungos::{
  find::find_collect,
  mongodb::bson::{
    Document, doc, oid::ObjectId, to_bson, to_document,
  },
};
use futures_util::future::join_all;
use komodo_client::{
  api::{
    execute::{
      Execution, GlobalAutoUpdate, RotateAllServerKeys, RunAction,
    },
    write::{
      CreateBuilder, CreateProcedure, CreateServer, CreateTag,
      UpdateResourceMeta,
    },
  },
  entities::{
    ResourceTarget,
    builder::{PartialBuilderConfig, PartialServerBuilderConfig},
    config::core::CoreConfig,
    deployment::DeploymentInfo,
    komodo_timestamp,
    procedure::{EnabledExecution, ProcedureConfig, ProcedureStage},
    server::{PartialServerConfig, Server, ServerInfo},
    sync::ResourceSync,
    tag::TagColor,
    update::Log,
    user::{action_user, system_user},
  },
};
use mogh_auth_server::AuthImpl;
use mogh_resolver::Resolve;
use uuid::Uuid;

use crate::{
  api::{
    execute::{ExecuteArgs, ExecuteRequest},
    write::WriteArgs,
  },
  auth::KomodoAuthImpl,
  config::core_config,
  helpers::{
    update::init_execution_update,
    validations::{
      validate_password_length, validate_username,
      validate_webhook_secret,
    },
  },
  network, resource,
  state::db_client,
};

/// Runs the Actions with `run_at_startup: true`
pub async fn run_startup_actions() {
  let startup_actions = match find_collect(
    &db_client().actions,
    doc! { "config.run_at_startup": true },
    None,
  )
  .await
  {
    Ok(actions) => actions,
    Err(e) => {
      error!("Failed to fetch actions for startup | {e:#?}");
      return;
    }
  };

  for action in startup_actions {
    let name = action.name;
    let id = action.id;
    let update = match init_execution_update(
      &ExecuteRequest::RunAction(RunAction {
        action: name.clone(),
        args: Default::default(),
      }),
      action_user(),
    )
    .await
    {
      Ok(update) => update,
      Err(e) => {
        error!(
          "Failed to initialize update for action {name} ({id}) | {e:#?}"
        );
        continue;
      }
    };

    if let Err(e) = (RunAction {
      action: name.clone(),
      args: Default::default(),
    })
    .resolve(&ExecuteArgs {
      user: action_user().to_owned(),
      update,
      task_id: Uuid::new_v4(),
    })
    .await
    {
      error!(
        "Failed to execute startup action {name} ({id}) | {e:#?}"
      );
    }
  }
}

/// This function should be run on startup,
/// after the db client has been initialized
pub async fn on_startup() -> anyhow::Result<()> {
  // Configure manual network interface if specified
  network::configure_internet_gateway().await;

  // Run legacy database fixes
  tokio::join!(
    clean_up_server_templates(),
    v2_init_missing_resource_info(),
  );

  // Validate and establish access before the API begins accepting requests.
  ensure_init_user_and_resources().await?;

  // Standard startup tasks
  tokio::join!(
    in_progress_update_cleanup(),
    open_alert_cleanup(),
    action_api_key_cleanup(),
    ensure_first_server_and_builder(),
    warn_legacy_backup_schedules(),
    backup_run_cleanup(),
  );

  Ok(())
}

async fn backup_run_cleanup() {
  match crate::backup::finalize_interrupted_runs().await {
    Ok(0) => {}
    Ok(count) => info!(
      "Finalized {count} interrupted backup run(s) during startup"
    ),
    Err(error) => {
      error!("Failed to finalize interrupted backup runs: {error:#}")
    }
  }
}

async fn warn_legacy_backup_schedules() {
  let procedures = match find_collect(
    &db_client().procedures,
    doc! { "config.schedule_enabled": true },
    None,
  )
  .await
  {
    Ok(procedures) => procedures,
    Err(error) => {
      warn!("Failed to inspect legacy backup schedules: {error:#}");
      return;
    }
  };
  for procedure in procedures {
    if procedure.config.stages.iter().any(|stage| {
      stage.executions.iter().any(|execution| {
        execution.enabled
          && matches!(
            execution.execution,
            Execution::BackupCoreDatabase(_)
          )
      })
    }) {
      warn!(
        "Legacy Core database backup scheduling remains enabled on Procedure '{}'. Migrate it to /backups to avoid duplicate exports.",
        procedure.name
      );
    }
  }
}

async fn in_progress_update_cleanup() {
  let log = Log::error(
    "Komodo shutdown",
    String::from(
      "Komodo shutdown during execution. If this is a build, the builder may not have been terminated.",
    ),
  );
  let log = match to_document(&log)
    .context("Failed to serialize log to document")
  {
    Ok(log) => log,
    Err(e) => {
      error!("Failed to clean up in progress update | {e:#}");
      return;
    }
  };
  if let Err(e) = db_client()
    .updates
    .update_many(
      doc! {
        "status": "InProgress",
        "$or": [
          { "operation": { "$ne": "DeleteStack" } },
          { "other_data": "" },
        ],
      },
      doc! {
        "$set": {
          "status": "Complete",
          "success": false,
        },
        "$push": {
          "logs": log
        }
      },
    )
    .await
  {
    error!(
      "Failed to clean up in progress updates on startup | {e:?}"
    )
  }
}

/// Run on startup, ensure open alerts pointing to invalid resources are closed.
async fn open_alert_cleanup() {
  let db = db_client();
  let Ok(alerts) =
    find_collect(&db.alerts, doc! { "resolved": false }, None)
      .await
      .inspect_err(|e| {
        error!(
          "failed to list all alerts for startup open alert clean up | {e:?}"
        )
      })
  else {
    return;
  };
  let futures = alerts.into_iter().map(|alert| async move {
    match alert.target {
      ResourceTarget::Server(id) => {
        resource::get::<Server>(&id)
          .await
          .is_err()
          .then(|| ObjectId::from_str(&alert.id).inspect_err(|e| warn!("failed to clean up alert - id is invalid ObjectId | {e:?}")).ok()).flatten()
      }
      ResourceTarget::ResourceSync(id) => {
        resource::get::<ResourceSync>(&id)
          .await
          .is_err()
          .then(|| ObjectId::from_str(&alert.id).inspect_err(|e| warn!("failed to clean up alert - id is invalid ObjectId | {e:?}")).ok()).flatten()
      }
      // No other resources should have open alerts.
      _ => ObjectId::from_str(&alert.id).inspect_err(|e| warn!("failed to clean up alert - id is invalid ObjectId | {e:?}")).ok(),
    }
  });
  let to_update_ids = join_all(futures)
    .await
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
  if let Err(e) = db
    .alerts
    .update_many(
      doc! { "_id": { "$in": to_update_ids } },
      doc! { "$set": {
        "resolved": true,
        "resolved_ts": komodo_timestamp()
      } },
    )
    .await
  {
    error!(
      "failed to clean up invalid open alerts on startup | {e:#}"
    )
  }
}

async fn action_api_key_cleanup() {
  if let Err(e) = db_client()
    .api_keys
    .delete_many(doc! { "user_id": &action_user().id })
    .await
  {
    warn!(
      "Failed to clean up dangling action api keys on startup | {e:?}"
    )
  }
}

/// Ensures a default server / builder exists with the defined address
async fn ensure_first_server_and_builder() {
  let config = core_config();
  if config.first_server_name.is_none()
    && config.first_server_address.is_none()
  {
    // If neither defined, early return
    return;
  }
  // Maybe create first Server / Builder
  let db = db_client();
  // If any Server exists, exit early.
  let Ok(None) =
    db.servers.find_one(Document::new()).await.inspect_err(|e| {
      error!(
        "Failed to initialize first Server. Failed to query db. {e:?}"
      )
    })
  else {
    return;
  };
  // Use the same name for Server and Builder
  let name = config.first_server_name.as_deref().unwrap_or("Local");
  let server = match (CreateServer {
    name: name.to_string(),
    config: PartialServerConfig {
      address: config.first_server_address.clone(),
      enabled: Some(true),
      ..Default::default()
    },
    public_key: None,
  })
  .resolve(&WriteArgs {
    user: system_user().to_owned(),
  })
  .await
  {
    Ok(server) => server,
    Err(e) => {
      error!(
        "Failed to initialize first Server. Failed to CreateServer. {:#}",
        e.error
      );
      return;
    }
  };
  // If any builder exists, exit early.
  let Ok(None) = db.builders
    .find_one(Document::new()).await
    .inspect_err(|e| error!("Failed to initialize 'first_builder' | Failed to query db | {e:?}")) else {
      return;
    };
  if let Err(e) = (CreateBuilder {
    // Same name as Server
    name: name.to_string(),
    config: PartialBuilderConfig::Server(
      PartialServerBuilderConfig {
        server_ids: Some(vec![server.id]),
      },
    ),
  })
  .resolve(&WriteArgs {
    user: system_user().to_owned(),
  })
  .await
  {
    error!(
      "Failed to initialize 'first_builder' | Failed to CreateBuilder | {:#}",
      e.error
    );
  }
}

fn validate_bootstrap_config(
  config: &CoreConfig,
  has_users: bool,
) -> anyhow::Result<()> {
  if !config.webhook_secret.trim().is_empty() {
    validate_webhook_secret(&config.webhook_secret)
      .context("Invalid KOMODO_WEBHOOK_SECRET")?;
  }

  if has_users {
    return Ok(());
  }

  if let Some(username) = &config.init_admin_username {
    anyhow::ensure!(
      config.local_auth,
      "KOMODO_LOCAL_AUTH must be true to create the initial administrator"
    );
    validate_username(username)
      .context("Invalid KOMODO_INIT_ADMIN_USERNAME")?;
    anyhow::ensure!(
      !config.init_admin_password.trim().is_empty(),
      "KOMODO_INIT_ADMIN_PASSWORD must be configured"
    );
    anyhow::ensure!(
      !["changeme", "REPLACE_WITH_PASSWORD"].iter().any(
        |placeholder| config
          .init_admin_password
          .trim()
          .eq_ignore_ascii_case(placeholder)
      ),
      "KOMODO_INIT_ADMIN_PASSWORD must not use an example placeholder"
    );
    validate_password_length(
      &config.init_admin_password,
      config.min_password_length as usize,
    )
    .context("Invalid KOMODO_INIT_ADMIN_PASSWORD")?;
    return Ok(());
  }

  let local_registration_enabled = config.local_auth
    && !config
      .disable_local_user_registration
      .unwrap_or(config.disable_user_registration);
  let oidc_registration_enabled = config.oidc_enabled()
    && !config
      .disable_oidc_user_registration
      .unwrap_or(config.disable_user_registration);
  let named_oauth_registration_enabled = !config
    .disable_user_registration
    && (config.github_oauth.enabled()
      || config.google_oauth.enabled());

  anyhow::ensure!(
    local_registration_enabled
      || oidc_registration_enabled
      || named_oauth_registration_enabled,
    "The user database is empty and registration is disabled. Configure an initial administrator or explicitly enable registration for an enabled login provider"
  );

  Ok(())
}

async fn ensure_init_user_and_resources() -> anyhow::Result<()> {
  let db = db_client();

  // Assumes if there are any existing users, procedures, or tags,
  // the default procedures do not need to be set up.
  let (user, procedures, tags) = tokio::try_join!(
    db.users.find_one(Document::new()),
    db.procedures.find_one(Document::new()),
    db.tags.find_one(Document::new()),
  )
  .context("Failed to query the database during initial setup")?;

  let config = core_config();
  validate_bootstrap_config(config, user.is_some())?;

  if user.is_some() {
    return Ok(());
  }

  // Init admin user if set in config.
  if let Some(username) = &config.init_admin_username {
    info!("Creating init admin user...");
    let hashed_password = bcrypt::hash(
      config.init_admin_password.as_bytes(),
      KomodoAuthImpl.local_auth_bcrypt_cost(),
    )
    .context("Failed to hash the initial administrator password")?;
    AuthImpl::sign_up_local_user(
      &KomodoAuthImpl,
      username.to_string(),
      hashed_password,
      true,
    )
    .await
    .map_err(|e| {
      anyhow!("Failed to create init admin user | {:#}", e.error)
    })?;
    let initialized_user = db
      .users
      .find_one(doc! { "username": username })
      .await
      .context(
        "Failed to query database for init admin user after creation",
      )?;
    anyhow::ensure!(
      initialized_user.is_some(),
      "Failed to find init admin user after creation"
    );
    info!("Successfully created init admin user.");
  }

  if config.disable_init_resources
    || procedures.is_some()
    || tags.is_some()
  {
    info!("Skipping initial system resource creation");
    return Ok(());
  }

  info!("Creating initial system resources...");

  let write_args = WriteArgs {
    user: system_user().to_owned(),
  };

  // Create default 'system' tag
  let default_tags = match (CreateTag {
    name: String::from("system"),
    color: Some(TagColor::Red),
  })
  .resolve(&write_args)
  .await
  {
    Ok(tag) => vec![tag.id],
    Err(e) => {
      warn!("Failed to create default tag | {:#}", e.error);
      Vec::new()
    }
  };

  // GlobalAutoUpdate
  async {
    let Ok(config) = ProcedureConfig::builder()
      .stages(vec![ProcedureStage {
        name: String::from("Stage 1"),
        enabled: true,
        executions: vec![
          EnabledExecution {
            execution: Execution::GlobalAutoUpdate(GlobalAutoUpdate { skip_auto_update: false }),
            enabled: true
          }
        ]
      }])
      .schedule(String::from("Every day at 03:00"))
      .build()
      .inspect_err(|e| error!("Failed to initialize global auto update procedure | Failed to build Procedure | {e:?}")) else {
      return;
    };
    let procedure = match (CreateProcedure {
      name: String::from("Global Auto Update"),
      config: config.into(),
    })
    .resolve(&write_args)
    .await
    {
      Ok(procedure) => procedure,
      Err(e) => {
        error!(
          "Failed to initialize global auto update Procedure | Failed to create Procedure | {:#}",
          e.error
        );
        return;
      }
    };
    if let Err(e) = (UpdateResourceMeta {
      target: ResourceTarget::Procedure(procedure.id),
      tags: Some(default_tags.clone()),
      description: Some(String::from(
        "Pulls and auto updates Stacks and Deployments using 'poll_for_updates' or 'auto_update'.",
      )),
      template: None,
    })
    .resolve(&write_args)
    .await
    {
      warn!(
        "Failed to update global auto update Procedure tags / description | {:#}",
        e.error
      );
    }
  }.await;

  // RotateAllServerKeys
  async {
    let Ok(config) = ProcedureConfig::builder()
      .stages(vec![ProcedureStage {
        name: String::from("Stage 1"),
        enabled: true,
        executions: vec![
          EnabledExecution {
            execution: Execution::RotateAllServerKeys(RotateAllServerKeys {}),
            enabled: true
          }
        ]
      }])
      .schedule(String::from("Every day at 06:00"))
      .build()
      .inspect_err(|e| error!("Failed to initialize Server key rotation Procedure | Failed to build Procedure | {e:?}")) else {
      return;
    };
    let procedure = match (CreateProcedure {
      name: String::from("Rotate Server Keys"),
      config: config.into(),
    })
    .resolve(&write_args)
    .await
    {
      Ok(procedure) => procedure,
      Err(e) => {
        error!(
          "Failed to initialize Server key rotation Procedure | Failed to create Procedure | {:#}",
          e.error
        );
        return;
      }
    };
    if let Err(e) = (UpdateResourceMeta {
      target: ResourceTarget::Procedure(procedure.id),
      tags: Some(default_tags.clone()),
      description: Some(String::from(
        "Rotates all currently connected Server keys.",
      )),
      template: None,
    })
    .resolve(&write_args)
    .await
    {
      warn!(
        "Failed to update Server key rotation Procedure tags / description | {:#}",
        e.error
      );
    }
  }.await;

  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;

  fn configured_core() -> CoreConfig {
    CoreConfig {
      local_auth: true,
      ..Default::default()
    }
  }

  #[test]
  fn existing_installation_does_not_require_bootstrap_credentials() {
    assert!(
      validate_bootstrap_config(&CoreConfig::default(), true).is_ok()
    );
  }

  #[test]
  fn empty_installation_requires_an_access_path() {
    let config = configured_core();
    assert!(validate_bootstrap_config(&config, false).is_err());
  }

  #[test]
  fn initial_admin_requires_a_real_password() {
    let mut config = configured_core();
    config.init_admin_username = Some("admin".into());

    for password in
      ["", "changeme", "REPLACE_WITH_PASSWORD", "too-short"]
    {
      config.init_admin_password = password.into();
      assert!(validate_bootstrap_config(&config, false).is_err());
    }

    config.init_admin_password = "a-secure-password".into();
    assert!(validate_bootstrap_config(&config, false).is_ok());
  }

  #[test]
  fn explicit_registration_override_can_bootstrap() {
    let mut local = configured_core();
    local.disable_local_user_registration = Some(false);
    assert!(validate_bootstrap_config(&local, false).is_ok());

    let mut oidc = CoreConfig::default();
    oidc.oidc_enabled = true;
    oidc.oidc_provider = "https://identity.example.com".into();
    oidc.oidc_client_id = "komodo".into();
    oidc.disable_oidc_user_registration = Some(false);
    assert!(validate_bootstrap_config(&oidc, false).is_ok());
  }

  #[test]
  fn shipped_webhook_placeholder_is_rejected() {
    let mut config = CoreConfig::default();
    config.webhook_secret = "a_random_webhook_secret".into();
    assert!(validate_bootstrap_config(&config, true).is_err());
  }
}

/// v1.17.5 removes the ServerTemplate resource.
/// References to this resource type need to be cleaned up
/// to avoid type errors reading from the database.
async fn clean_up_server_templates() {
  let db = db_client();
  tokio::join!(
    async {
      if let Err(e) = db
        .permissions
        .delete_many(doc! {
          "resource_target.type": "ServerTemplate",
        })
        .await
      {
        error!(
          "Failed to clean up server template permissions on database | {e:#}"
        );
      }
    },
    async {
      if let Err(e) = db
        .updates
        .delete_many(doc! { "target.type": "ServerTemplate" })
        .await
      {
        error!(
          "Failed to clean up server template updates on database | {e:#}"
        );
      }
    },
    async {
      if let Err(e) = db.users
        .update_many(
          Document::new(),
          doc! { "$unset": { "recents.ServerTemplate": 1, "all.ServerTemplate": 1 } }
        )
        .await
      {
        error!(
          "Failed to clean up server template user references on database | {e:#}"
        );
      }
    },
    async {
      if let Err(e) = db
        .user_groups
        .update_many(
          Document::new(),
          doc! { "$unset": { "all.ServerTemplate": 1 } },
        )
        .await
      {
        error!(
          "Failed to clean up server template user group references on database | {e:#}"
        );
      }
    },
  );
}

/// v2 adds ServerInfo to ServerSchema and DeploymentInfo to DeploymentSchema.
/// Need to ensure it is initialized from null to avoid de/serialization issues.
async fn v2_init_missing_resource_info() {
  let default_server_info = match to_bson(&ServerInfo::default()) {
    Ok(info) => info,
    Err(e) => {
      error!("Failed to serialize ServerInfo to bson | {e:?}");
      return;
    }
  };
  if let Err(e) = db_client()
    .servers
    .update_many(
      doc! { "info": null },
      doc! { "$set": { "info": default_server_info } },
    )
    .await
  {
    error!("Failed to migrate ServerInfo to v2 | {e:?}");
  }
  let default_deployment_info =
    match to_bson(&DeploymentInfo::default()) {
      Ok(info) => info,
      Err(e) => {
        error!("Failed to serialize DeploymentInfo to bson | {e:?}");
        return;
      }
    };
  if let Err(e) = db_client()
    .deployments
    .update_many(
      doc! { "info": null },
      doc! { "$set": { "info": default_deployment_info } },
    )
    .await
  {
    error!("Failed to migrate DeploymentInfo to v2 | {e:?}");
  }
}
