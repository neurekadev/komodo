use std::path::Path;

use anyhow::Context;
use database::mungos::{
  by_id::find_one_by_id,
  find::find_collect,
  mongodb::{Collection, bson::doc},
};
use formatting::format_serror;
use indexmap::IndexSet;
use komodo_client::{
  api::write::RefreshStackCache,
  entities::{
    Operation, ResourceTarget, ResourceTargetVariant, SwarmOrServer,
    permission::{PermissionLevel, SpecificPermission},
    repo::Repo,
    resource::Resource,
    server::Server,
    stack::{
      PartialStackConfig, Stack, StackConfig, StackConfigDiff,
      StackInfo, StackListItem, StackListItemInfo,
      StackQuerySpecifics, StackServiceWithUpdate, StackState,
      validate_stack_file_paths,
    },
    swarm::Swarm,
    to_docker_compatible_name,
    update::Update,
    user::{User, stack_user},
  },
};
use mogh_resolver::Resolve;
use periphery_client::api::{
  stack::{
    CommitStackDeletion, PrepareStackDeletion, RollbackStackDeletion,
    StackDeletionMode, ValidateStackDeletion,
  },
  swarm::RemoveSwarmStacks,
};
use serde::{Deserialize, Serialize};

use crate::{
  api::write::WriteArgs,
  config::core_config,
  helpers::{
    periphery_client,
    query::{
      get_cached_stack_state, get_stack_state, get_swarm_or_server,
    },
    repo_link,
    swarm::swarm_request,
    update::update_update,
  },
  monitor::{refresh_server_cache, refresh_swarm_cache},
  state::{
    action_states, all_resources_cache, db_client,
    server_status_cache, stack_status_cache,
  },
};

use super::get_check_permissions;

impl super::KomodoResource for Stack {
  type Config = StackConfig;
  type PartialConfig = PartialStackConfig;
  type ConfigDiff = StackConfigDiff;
  type Info = StackInfo;
  type ListItem = StackListItem;
  type QuerySpecifics = StackQuerySpecifics;

  fn resource_type() -> ResourceTargetVariant {
    ResourceTargetVariant::Stack
  }

  fn resource_target(id: impl Into<String>) -> ResourceTarget {
    ResourceTarget::Stack(id.into())
  }

  fn validated_name(name: &str) -> String {
    to_docker_compatible_name(name)
  }

  fn creator_specific_permissions() -> IndexSet<SpecificPermission> {
    [
      SpecificPermission::FileManager,
      SpecificPermission::Inspect,
      SpecificPermission::Logs,
      SpecificPermission::Terminal,
    ]
    .into_iter()
    .collect()
  }

  fn inherit_specific_permissions_from(
    _self: &Resource<Self::Config, Self::Info>,
  ) -> Option<ResourceTarget> {
    if !_self.config.swarm_id.is_empty() {
      Some(ResourceTarget::Swarm(_self.config.swarm_id.clone()))
    } else if !_self.config.server_id.is_empty() {
      Some(ResourceTarget::Server(_self.config.server_id.clone()))
    } else {
      None
    }
  }

  fn coll() -> &'static Collection<Resource<Self::Config, Self::Info>>
  {
    &db_client().stacks
  }

  async fn to_list_item(
    stack: Resource<Self::Config, Self::Info>,
  ) -> Self::ListItem {
    let status = stack_status_cache().get(&stack.id).await;
    let state = get_cached_stack_state(&stack.id).await;
    let project_name = stack.project_name(false);
    let services = status
      .as_ref()
      .map(|s| {
        s.curr
          .services
          .iter()
          .map(|current_service| {
            let latest_service = stack
              .info
              .latest_services
              .iter()
              .find(|latest_service| {
                current_service.service == latest_service.service_name
              });
            let latest_image = if let Some(latest_image) =
              latest_service.as_ref().map(|s| &s.image)
              && latest_image != &current_service.image
            {
              Some(latest_image.to_string())
            } else {
              None
            };
            let update_available = current_service
              .image_digests
              .as_ref()
              .and_then(|current_digests| {
                latest_service.as_ref().and_then(|latest_service| {
                  latest_service
                    .image_digest
                    .as_ref()?
                    .update_available(current_digests)
                    .into()
                })
              })
              .unwrap_or_default();
            StackServiceWithUpdate {
              service: current_service.service.clone(),
              image: current_service.image.clone(),
              latest_image,
              update_available,
            }
          })
          .collect::<Vec<_>>()
      })
      .unwrap_or_default();

    let default_git = || {
      (
        stack.config.git_provider,
        stack.config.repo,
        stack.config.branch,
        stack.config.git_https,
        String::new(),
      )
    };
    let (git_provider, repo, branch, git_https, linked_repo_name) =
      if stack.config.linked_repo.is_empty() {
        default_git()
      } else {
        all_resources_cache()
          .load()
          .repos
          .get(&stack.config.linked_repo)
          .map(|r| {
            (
              r.config.git_provider.clone(),
              r.config.repo.clone(),
              r.config.branch.clone(),
              r.config.git_https,
              r.name.clone(),
            )
          })
          .unwrap_or_else(default_git)
      };

    // This is only true if it is KNOWN to be true. so other cases are false.
    let (project_missing, status) =
      if matches!(state, StackState::Down | StackState::Unknown) {
        (false, None)
      } else if stack.config.swarm_id.is_empty()
        && !stack.config.server_id.is_empty()
        && let Some(status) = server_status_cache()
          .get(&stack.config.server_id)
          .await
          .as_ref()
      {
        if let Some(docker) = &status.docker {
          if let Some(project) = docker
            .projects
            .iter()
            .find(|project| project.name == project_name)
          {
            (false, project.status.clone())
          } else {
            // The project doesn't exist
            (true, None)
          }
        } else {
          (false, None)
        }
      } else {
        (false, None)
      };

    let all = all_resources_cache().load();
    let server_name = all
      .servers
      .get(&stack.config.server_id)
      .map(|server| server.name.clone())
      .unwrap_or_default();
    let swarm_name = all
      .swarms
      .get(&stack.config.swarm_id)
      .map(|swarm| swarm.name.clone())
      .unwrap_or_default();

    StackListItem {
      name: stack.name,
      id: stack.id,
      template: stack.template,
      tags: stack.tags,
      resource_type: ResourceTargetVariant::Stack,
      info: StackListItemInfo {
        state,
        status,
        services,
        project_missing,
        file_contents: !stack.config.file_contents.is_empty(),
        swarm_id: stack.config.swarm_id,
        swarm_name,
        server_id: stack.config.server_id,
        server_name,
        linked_repo: stack.config.linked_repo,
        linked_repo_name,
        missing_files: stack.info.missing_files,
        files_on_host: stack.config.files_on_host,
        repo_link: repo_link(
          &git_provider,
          &repo,
          &branch,
          git_https,
        ),
        git_provider,
        repo,
        branch,
        latest_hash: stack.info.latest_hash,
        deployed_hash: stack.info.deployed_hash,
        auto_update_all_services: stack
          .config
          .auto_update_all_services,
      },
    }
  }

  async fn busy(id: &String) -> anyhow::Result<bool> {
    action_states()
      .stack
      .get(id)
      .await
      .unwrap_or_default()
      .busy()
  }

  // CREATE

  fn create_operation() -> Operation {
    Operation::CreateStack
  }

  fn user_can_create(user: &User) -> bool {
    user.admin || !core_config().disable_non_admin_create
  }

  async fn validate_create_config(
    config: &mut Self::PartialConfig,
    user: &User,
  ) -> anyhow::Result<()> {
    validate_config(config, user).await?;
    validate_create_file_paths(config)
  }

  async fn post_create(
    created: &Resource<Self::Config, Self::Info>,
    update: &mut Update,
  ) -> anyhow::Result<()> {
    if let Err(e) = (RefreshStackCache {
      stack: created.name.clone(),
    })
    .resolve(&WriteArgs {
      user: stack_user().to_owned(),
    })
    .await
    {
      update.push_error_log(
        "Refresh stack cache",
        format_serror(&e.error.context("The stack cache has failed to refresh. This may be due to a misconfiguration of the Stack").into())
      );
    };
    if created.config.swarm_id.is_empty()
      && created.config.server_id.is_empty()
    {
      return Ok(());
    }
    let Ok(swarm_or_server) = get_swarm_or_server(
      &created.config.swarm_id,
      &created.config.server_id,
    )
    .await
    .inspect_err(|e| {
      warn!(
        "Failed to get Swarm or Server for Stack {} | {e:#}",
        created.name
      )
    }) else {
      return Ok(());
    };
    match swarm_or_server {
      SwarmOrServer::Swarm(swarm) => {
        refresh_swarm_cache(&swarm, true).await;
      }
      SwarmOrServer::Server(server) => {
        refresh_server_cache(&server, true).await;
      }
      SwarmOrServer::None => {}
    }
    Ok(())
  }

  // UPDATE

  fn update_operation() -> Operation {
    Operation::UpdateStack
  }

  async fn validate_update_config(
    id: &str,
    config: &mut Self::PartialConfig,
    user: &User,
  ) -> anyhow::Result<()> {
    validate_config(config, user).await?;
    let current = super::get::<Stack>(id).await?;
    validate_effective_file_paths(&current.config, config)
  }

  async fn post_update(
    updated: &Resource<Self::Config, Self::Info>,
    update: &mut Update,
  ) -> anyhow::Result<()> {
    Self::post_create(updated, update).await
  }

  // RENAME

  fn rename_operation() -> Operation {
    Operation::RenameStack
  }

  // DELETE

  fn delete_operation() -> Operation {
    Operation::DeleteStack
  }

  async fn pre_delete(
    stack: &Resource<Self::Config, Self::Info>,
    update: &mut Update,
  ) -> anyhow::Result<()> {
    Self::pre_delete_transaction(stack, update, false).await
  }

  fn transactional_delete() -> bool {
    true
  }

  async fn delete_transaction_data(
    stack: &Resource<Self::Config, Self::Info>,
    _remove_volumes: bool,
  ) -> anyhow::Result<String> {
    let (_, servers) = stack_delete_servers(stack).await?;
    Ok(serde_json::to_string(&StackDeleteTransaction {
      transaction_id: stack.id.clone(),
      stack_id: stack.id.clone(),
      stack_name: stack.name.clone(),
      server_ids: servers
        .into_iter()
        .map(|server| server.id)
        .collect(),
    })?)
  }

  async fn pre_delete_transaction(
    stack: &Resource<Self::Config, Self::Info>,
    update: &mut Update,
    remove_volumes: bool,
  ) -> anyhow::Result<()> {
    prepare_stack_delete(stack, update, remove_volumes).await
  }

  async fn rollback_delete_transaction(
    _stack: &Resource<Self::Config, Self::Info>,
    update: &mut Update,
  ) -> anyhow::Result<()> {
    let transaction = parse_stack_delete_transaction(update)?;
    reconcile_stack_delete_hosts(&transaction, false).await
  }

  async fn commit_delete_transaction(
    _stack: &Resource<Self::Config, Self::Info>,
    update: &mut Update,
  ) -> anyhow::Result<()> {
    let transaction = parse_stack_delete_transaction(update)?;
    reconcile_stack_delete_hosts(&transaction, true).await
  }

  async fn post_delete(
    resource: &Resource<Self::Config, Self::Info>,
    _update: &mut Update,
  ) -> anyhow::Result<()> {
    stack_status_cache().remove(&resource.id).await;
    Ok(())
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StackDeleteTransaction {
  transaction_id: String,
  stack_id: String,
  stack_name: String,
  server_ids: Vec<String>,
}

fn parse_stack_delete_transaction(
  update: &Update,
) -> anyhow::Result<StackDeleteTransaction> {
  serde_json::from_str(&update.other_data)
    .context("Missing stack deletion transaction state")
}

async fn stack_delete_servers(
  stack: &Stack,
) -> anyhow::Result<(StackDeletionMode, Vec<Server>)> {
  let target = get_swarm_or_server(
    &stack.config.swarm_id,
    &stack.config.server_id,
  )
  .await
  .context("Failed to resolve the stack host for deletion")?;
  let (mode, server_ids) = match target {
    SwarmOrServer::Swarm(swarm) => {
      (StackDeletionMode::Swarm, swarm.config.server_ids)
    }
    SwarmOrServer::Server(server) => {
      (StackDeletionMode::Compose, vec![server.id])
    }
    SwarmOrServer::None => {
      if stack_requires_host_for_deletion(stack) {
        return Err(anyhow::anyhow!(
          "Stack has deployment history but no configured host; refusing deletion because stack files cannot be retired safely"
        ));
      }
      return Ok((StackDeletionMode::Compose, Vec::new()));
    }
  };

  let mut servers = Vec::with_capacity(server_ids.len());
  for server_id in server_ids {
    let server =
      super::get::<Server>(&server_id).await.with_context(|| {
        format!("Failed to load stack host {server_id}")
      })?;
    if !server.config.enabled {
      return Err(anyhow::anyhow!(
        "Stack host {} is disabled; refusing deletion",
        server.name
      ));
    }
    periphery_client(&server)
      .await?
      .health_check()
      .await
      .with_context(|| {
        format!(
          "Stack host {} is not connected; refusing deletion",
          server.name
        )
      })?;
    servers.push(server);
  }
  Ok((mode, servers))
}

fn stack_requires_host_for_deletion(stack: &Stack) -> bool {
  stack.info.deployed_project_name.is_some()
    || stack.info.deployed_contents.is_some()
    || stack.info.remote_contents.is_some()
}

async fn prepare_stack_delete(
  stack: &Stack,
  update: &mut Update,
  remove_volumes: bool,
) -> anyhow::Result<()> {
  let state = get_stack_state(stack)
    .await
    .context("Failed to confirm stack state before deletion")?;
  update.push_simple_log(
    "Confirm stack state",
    format!("Observed stack state: {state}"),
  );
  let (mode, servers) = stack_delete_servers(stack).await?;
  if servers.is_empty() {
    update.push_simple_log(
      "Prepare stack deletion",
      "The stack has no configured host and no host-owned files",
    );
    return Ok(());
  }

  let repo = if stack.config.linked_repo.is_empty() {
    None
  } else {
    Some(
      super::get::<Repo>(&stack.config.linked_repo)
        .await
        .context("Failed to load linked repository for deletion")?,
    )
  };

  if matches!(mode, StackDeletionMode::Swarm) {
    for server in &servers {
      let log = periphery_client(server)
        .await?
        .request(ValidateStackDeletion {
          stack: stack.clone(),
          remove_volumes,
        })
        .await
        .with_context(|| {
          format!(
            "Stack deletion safety checks failed on host {}",
            server.name
          )
        })?;
      update.logs.push(log);
    }
    let log = swarm_request(
      &servers
        .iter()
        .map(|server| server.id.clone())
        .collect::<Vec<_>>(),
      RemoveSwarmStacks {
        stacks: vec![stack.project_name(false)],
        detach: false,
      },
    )
    .await
    .context("Failed to remove the Swarm stack")?;
    let success = log.success;
    update.logs.push(log);
    if !success {
      return Err(anyhow::anyhow!(
        "Swarm stack removal did not complete successfully"
      ));
    }
  }

  for server in servers {
    let response = periphery_client(&server)
      .await?
      .request(PrepareStackDeletion {
        transaction_id: stack.id.clone(),
        stack: stack.clone(),
        repo: repo.clone(),
        mode,
        remove_volumes,
      })
      .await
      .with_context(|| {
        format!(
          "Failed to prepare deletion on stack host {}",
          server.name
        )
      })?;
    update.logs.extend(response);
  }
  Ok(())
}

async fn reconcile_stack_delete_hosts(
  transaction: &StackDeleteTransaction,
  commit: bool,
) -> anyhow::Result<()> {
  let mut errors = Vec::new();
  for server_id in &transaction.server_ids {
    let result = async {
      let server = super::get::<Server>(server_id).await?;
      let periphery = periphery_client(&server).await?;
      if commit {
        periphery
          .request(CommitStackDeletion {
            transaction_id: transaction.transaction_id.clone(),
            stack_name: transaction.stack_name.clone(),
          })
          .await?;
      } else {
        periphery
          .request(RollbackStackDeletion {
            transaction_id: transaction.transaction_id.clone(),
            stack_name: transaction.stack_name.clone(),
          })
          .await?;
      }
      anyhow::Ok(())
    }
    .await;
    if let Err(error) = result {
      errors.push(format!("{server_id}: {error:#}"));
    }
  }
  if errors.is_empty() {
    Ok(())
  } else {
    Err(anyhow::anyhow!(errors.join("; ")))
  }
}

pub fn spawn_stack_delete_reconciliation_loop() {
  tokio::spawn(async {
    loop {
      tokio::time::sleep(std::time::Duration::from_secs(30)).await;
      if let Err(error) = reconcile_stack_delete_transactions().await
      {
        warn!("Failed to reconcile stack deletions: {error:#}");
      }
    }
  });
}

async fn reconcile_stack_delete_transactions() -> anyhow::Result<()> {
  let updates = find_collect(
    &db_client().updates,
    doc! {
      "status": "InProgress",
      "operation": "DeleteStack",
      "other_data": { "$ne": "" },
    },
    None,
  )
  .await
  .context("Failed to load pending stack deletion transactions")?;
  for mut update in updates {
    if super::delete_transaction_is_active(&update.id) {
      continue;
    }
    let transaction = match parse_stack_delete_transaction(&update) {
      Ok(transaction) => transaction,
      Err(error) => {
        warn!(
          "Invalid pending stack deletion update {}: {error:#}",
          update.id
        );
        continue;
      }
    };
    let stack_exists =
      find_one_by_id(&db_client().stacks, &transaction.stack_id)
        .await
        .context("Failed to determine stack deletion outcome")?
        .is_some();
    if let Err(error) =
      reconcile_stack_delete_hosts(&transaction, !stack_exists).await
    {
      warn!(
        "Stack deletion transaction {} is still pending: {error:#}",
        transaction.transaction_id
      );
      continue;
    }
    if stack_exists {
      update.push_error_log(
        "Deletion recovered",
        "Core stopped before database deletion; retired stack files were restored",
      );
    } else {
      update.push_simple_log(
        "Deletion recovered",
        "Database deletion completed; retired stack files were committed to cleanup",
      );
    }
    update.finalize();
    update_update(update).await?;
  }
  Ok(())
}

#[instrument("ValidateStackConfig", skip_all)]
async fn validate_config(
  config: &mut PartialStackConfig,
  user: &User,
) -> anyhow::Result<()> {
  if let Some(swarm_id) = &config.swarm_id
    && !swarm_id.is_empty()
  {
    let swarm = get_check_permissions::<Swarm>(
      swarm_id,
      user,
      PermissionLevel::Read.attach(),
    )
    .await
    .context("Cannot attach Stack to this Swarm")?;
    config.swarm_id = Some(swarm.id);
  }
  if let Some(server_id) = &config.server_id
    && !server_id.is_empty()
  {
    let server = get_check_permissions::<Server>(
      server_id,
      user,
      PermissionLevel::Read.attach(),
    )
    .await
    .context("Cannot attach Stack to this Server")?;
    // in case it comes in as name
    config.server_id = Some(server.id);
  }
  if let Some(linked_repo) = &config.linked_repo
    && !linked_repo.is_empty()
  {
    let repo = get_check_permissions::<Repo>(
      linked_repo,
      user,
      PermissionLevel::Read.attach(),
    )
    .await
    .context("Cannot attach Repo to this Stack")?;
    // in case it comes in as name
    config.linked_repo = Some(repo.id);
  }
  Ok(())
}

fn validate_create_file_paths(
  config: &PartialStackConfig,
) -> anyhow::Result<()> {
  let effective: StackConfig = config.clone().into();
  validate_stack_file_paths(
    &effective.file_paths,
    &effective.env_file_path,
    Path::new(""),
  )
}

fn validate_effective_file_paths(
  current: &StackConfig,
  update: &PartialStackConfig,
) -> anyhow::Result<()> {
  validate_stack_file_paths(
    update.file_paths.as_ref().unwrap_or(&current.file_paths),
    update
      .env_file_path
      .as_deref()
      .unwrap_or(&current.env_file_path),
    Path::new(""),
  )
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn validates_create_and_partial_update_file_paths() {
    let create = PartialStackConfig {
      file_paths: Some(vec!["./.env".into()]),
      env_file_path: Some(".env".into()),
      ..Default::default()
    };
    assert!(validate_create_file_paths(&create).is_err());

    let current = StackConfig {
      file_paths: vec!["compose.yaml".into()],
      env_file_path: ".env".into(),
      ..Default::default()
    };
    let env_update = PartialStackConfig {
      env_file_path: Some("nested/../compose.yaml".into()),
      ..Default::default()
    };
    assert!(
      validate_effective_file_paths(&current, &env_update).is_err()
    );
    let compose_update = PartialStackConfig {
      file_paths: Some(vec!["nested/../.env".into()]),
      ..Default::default()
    };
    assert!(
      validate_effective_file_paths(&current, &compose_update)
        .is_err()
    );
    assert!(
      validate_effective_file_paths(
        &current,
        &PartialStackConfig::default()
      )
      .is_ok()
    );
  }

  #[test]
  fn deployment_history_requires_a_host_for_safe_deletion() {
    let mut stack = Stack {
      info: StackInfo::default(),
      ..Default::default()
    };
    assert!(!stack_requires_host_for_deletion(&stack));

    stack.info.deployed_project_name = Some("sites".into());
    assert!(stack_requires_host_for_deletion(&stack));
  }
}
