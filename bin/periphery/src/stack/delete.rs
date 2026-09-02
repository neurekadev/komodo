use std::{
  borrow::Cow,
  fs::{self, OpenOptions},
  io::Write as _,
  path::{Component, Path, PathBuf},
};

use anyhow::{Context, anyhow};
use command::{
  CommandOptions, run_komodo_standard_command, run_standard_command,
};
use komodo_client::entities::{
  repo::Repo, stack::Stack, to_path_compatible_name, update::Log,
};
use mogh_resolver::Resolve;
use periphery_client::api::stack::{
  CommitStackDeletion, PrepareStackDeletion, RollbackStackDeletion,
  StackDeletionMode, ValidateStackDeletion,
};
use serde::{Deserialize, Serialize};
use shell_escape::unix::escape;
use uuid::Uuid;

use crate::{
  api::{Args, backup},
  config::periphery_config,
  docker::compose::{docker_compose, list_compose_projects},
  stack::write::resolved_run_directory,
};

const DELETION_DIRECTORY: &str = ".komodo-stack-deletions";
const STATE_FILE: &str = "state.json";
const QUARANTINED_ROOT: &str = "root";
const COMPOSE_PROJECT_LABEL: &str = "com.docker.compose.project";
const SWARM_NAMESPACE_LABEL: &str = "com.docker.stack.namespace";

#[derive(
  Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
enum DeletionPhase {
  Preparing,
  Prepared,
  Committed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeletionState {
  transaction_id: String,
  stack_name: String,
  root_present: bool,
  phase: DeletionPhase,
}

impl Resolve<Args> for ValidateStackDeletion {
  #[instrument(
    "ValidateStackDeletion",
    skip_all,
    fields(
      id = args.id.to_string(),
      core = args.core,
      stack = self.stack.name,
    )
  )]
  async fn resolve(self, args: &Args) -> anyhow::Result<Log> {
    validate_owned_stack_root(&self.stack)?;
    if self.remove_volumes {
      let volumes = exact_labeled_volumes(
        SWARM_NAMESPACE_LABEL,
        &self.stack.project_name(false),
      )
      .await?;
      ensure_volumes_unreferenced(&volumes).await?;
    }
    Ok(Log::simple(
      "Validate stack deletion",
      "Stack files and volumes passed non-mutating safety checks"
        .to_string(),
    ))
  }
}

impl Resolve<Args> for PrepareStackDeletion {
  #[instrument(
    "PrepareStackDeletion",
    skip_all,
    fields(
      id = args.id.to_string(),
      core = args.core,
      transaction = self.transaction_id,
      stack = self.stack.name,
    )
  )]
  async fn resolve(self, args: &Args) -> anyhow::Result<Vec<Log>> {
    let _filesystem = backup::filesystem_mutation_guard()?;
    let PrepareStackDeletion {
      transaction_id,
      stack,
      repo,
      mode,
      remove_volumes,
    } = self;
    validate_identifier(&transaction_id)?;

    if let Some(log) =
      recover_prepared_transaction(&transaction_id, &stack.name)?
    {
      return Ok(vec![log]);
    }

    backup::ensure_no_pending_recovery()?;

    let mut logs = Vec::new();
    match mode {
      StackDeletionMode::Compose => {
        logs.push(
          prepare_compose_teardown(
            &stack,
            repo.as_ref(),
            remove_volumes,
          )
          .await?,
        );
      }
      StackDeletionMode::Swarm => {
        let project = stack.project_name(false);
        wait_for_swarm_detach(&project).await?;
        logs.push(Log::simple(
          "Wait for stack detach",
          "All local Swarm task containers have detached".to_string(),
        ));
        if remove_volumes {
          let removed = remove_exact_labeled_volumes(
            SWARM_NAMESPACE_LABEL,
            &project,
          )
          .await?;
          logs.push(Log::simple(
            "Remove stack volumes",
            if removed.is_empty() {
              "No stack-owned local volumes were present".to_string()
            } else {
              format!("Removed volumes: {}", removed.join(", "))
            },
          ));
        }
      }
    }

    retire_stack_root(&transaction_id, &stack)?;
    logs.push(Log::simple(
      "Retire stack files",
      if stack.config.linked_repo.is_empty() {
        "Komodo-owned stack files were moved into protected quarantine"
          .to_string()
      } else {
        "Linked repository files were retained".to_string()
      },
    ));
    Ok(logs)
  }
}

impl Resolve<Args> for RollbackStackDeletion {
  #[instrument(
    "RollbackStackDeletion",
    skip_all,
    fields(
      id = args.id.to_string(),
      core = args.core,
      transaction = self.transaction_id,
      stack = self.stack_name,
    )
  )]
  async fn resolve(self, args: &Args) -> anyhow::Result<Log> {
    let _filesystem = backup::filesystem_mutation_guard()?;
    rollback_stack_root(&self.transaction_id, &self.stack_name)?;
    Ok(Log::simple(
      "Restore stack files",
      "The retired stack files were restored".to_string(),
    ))
  }
}

impl Resolve<Args> for CommitStackDeletion {
  #[instrument(
    "CommitStackDeletion",
    skip_all,
    fields(
      id = args.id.to_string(),
      core = args.core,
      transaction = self.transaction_id,
      stack = self.stack_name,
    )
  )]
  async fn resolve(self, args: &Args) -> anyhow::Result<Log> {
    let filesystem = backup::filesystem_mutation_guard()?;
    let cleanup =
      commit_stack_root(&self.transaction_id, &self.stack_name)?;
    if let Some(cleanup) = cleanup {
      tokio::spawn(async move {
        let _filesystem = filesystem;
        if let Err(error) = tokio::fs::remove_dir_all(&cleanup).await
        {
          warn!(
            "Failed to clean committed stack deletion at {}: {error:#}",
            cleanup.display()
          );
        } else if let Some(parent) = cleanup.parent() {
          let _ = sync_directory(parent);
        }
      });
    }
    Ok(Log::simple(
      "Delete stack files",
      "Retired stack files were committed to protected cleanup"
        .to_string(),
    ))
  }
}

pub(crate) fn ensure_no_pending_deletions() -> anyhow::Result<()> {
  ensure_no_pending_deletions_at(&deletion_base())
}

fn ensure_no_pending_deletions_at(base: &Path) -> anyhow::Result<()> {
  let metadata = match fs::symlink_metadata(base) {
    Ok(metadata) => metadata,
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
      return Ok(());
    }
    Err(error) => return Err(error.into()),
  };
  validate_protected_directory(base, &metadata)?;
  for entry in fs::read_dir(base)? {
    let path = entry?.path();
    validate_protected_directory(
      &path,
      &fs::symlink_metadata(&path)?,
    )?;
    let state = read_state_at(&path).with_context(|| format!(
      "Backup/restore blocked by unreadable Stack deletion state at {}",
      path.display()
    ))?;
    if state.phase != DeletionPhase::Committed {
      return Err(anyhow!(
        "Backup/restore blocked by pending Stack deletion '{}'; let Core finish deletion reconciliation before retrying",
        state.transaction_id
      ));
    }
  }
  Ok(())
}

pub async fn initialize() -> anyhow::Result<()> {
  let base = deletion_base();
  let metadata = match fs::symlink_metadata(&base) {
    Ok(metadata) => metadata,
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
      return Ok(());
    }
    Err(error) => return Err(error.into()),
  };
  ensure_protected_directory(&base, &metadata)?;
  for entry in fs::read_dir(&base)? {
    let entry = entry?;
    let path = entry.path();
    let metadata = fs::symlink_metadata(&path)?;
    if let Err(error) = ensure_protected_directory(&path, &metadata) {
      warn!(
        "Ignoring invalid stack deletion recovery entry at {}: {error:#}",
        path.display(),
      );
      continue;
    }
    match read_state_at(&path) {
      Ok(state) if state.phase == DeletionPhase::Committed => {
        tokio::fs::remove_dir_all(&path).await.with_context(
          || {
            format!(
              "Failed to resume committed stack deletion at {}",
              path.display()
            )
          },
        )?;
      }
      Ok(state) => warn!(
        "Retaining prepared stack deletion transaction {} for Core reconciliation",
        state.transaction_id
      ),
      Err(error) => warn!(
        "Unable to read stack deletion recovery entry at {}: {error:#}",
        path.display()
      ),
    }
  }
  sync_directory(&base)
}

async fn prepare_compose_teardown(
  stack: &Stack,
  repo: Option<&Repo>,
  remove_volumes: bool,
) -> anyhow::Result<Log> {
  validate_relative_path(&stack.config.run_directory, true)?;
  for path in stack.compose_file_paths() {
    validate_relative_path(path, false)?;
  }
  validate_relative_path(&stack.config.env_file_path, false)?;
  for file in &stack.config.additional_env_files {
    validate_relative_path(&file.path, false)?;
  }

  let run_directory = resolved_run_directory(stack, repo);
  let project = stack.project_name(false);
  let project_exists = list_compose_projects()
    .await
    .context("Failed to confirm Compose project state")?
    .iter()
    .any(|candidate| candidate.name == project);
  let compose_files = stack
    .compose_file_paths()
    .iter()
    .map(|path| run_directory.join(path))
    .collect::<Vec<_>>();
  let config_ready = run_directory.is_dir()
    && compose_files.iter().all(|path| path.is_file());

  if !config_ready {
    if project_exists {
      return Err(anyhow!(
        "Compose project {project} still exists, but its exact stack configuration is unavailable; refusing unsafe teardown"
      ));
    }
    if remove_volumes {
      let candidates =
        exact_labeled_volumes(COMPOSE_PROJECT_LABEL, &project)
          .await?;
      if !candidates.is_empty() {
        return Err(anyhow!(
          "Stack-owned volumes exist, but the exact Compose configuration is unavailable; refusing to guess which volumes are external"
        ));
      }
    }
    return Ok(Log::simple(
      "Compose Down",
      "The Compose project and stack configuration are both absent"
        .to_string(),
    ));
  }

  let run_directory =
    run_directory.canonicalize().with_context(|| {
      format!(
        "Failed to resolve stack run directory {}",
        run_directory.display()
      )
    })?;
  validate_resolved_run_directory(stack, repo, &run_directory)?;

  let mut command =
    format!("{} -p {}", docker_compose(), escaped(&project));
  for path in stack.compose_file_paths() {
    command.push_str(" -f ");
    command.push_str(&escaped(path));
  }
  for file in &stack.config.additional_env_files {
    command.push_str(" --env-file ");
    command.push_str(&escaped(&file.path));
  }
  let managed_env = run_directory.join(&stack.config.env_file_path);
  if managed_env.is_file()
    && !stack
      .config
      .additional_env_files
      .iter()
      .any(|file| file.path == stack.config.env_file_path)
  {
    command.push_str(" --env-file ");
    command.push_str(&escaped(&stack.config.env_file_path));
  }
  command.push_str(" down --remove-orphans");
  if remove_volumes {
    command.push_str(" --volumes");
  }

  let log = run_komodo_standard_command(
    "Compose Down",
    command,
    CommandOptions::default().path(run_directory.as_path()),
  )
  .await;
  if !log.success {
    return Err(anyhow!(
      "Compose teardown failed: {}",
      log.combined()
    ));
  }
  Ok(log)
}

fn validate_resolved_run_directory(
  stack: &Stack,
  repo: Option<&Repo>,
  run_directory: &Path,
) -> anyhow::Result<()> {
  let expected_root = if let Some(repo) = repo {
    periphery_config()
      .repo_dir()
      .join(to_path_compatible_name(&repo.name))
      .join(&repo.config.path)
  } else {
    owned_stack_root(&periphery_config().stack_dir(), stack)?
      .context("Linked stack is missing its linked repository")?
  };
  let expected_root =
    expected_root.canonicalize().with_context(|| {
      format!(
        "Failed to resolve expected stack root {}",
        expected_root.display()
      )
    })?;
  if !run_directory.starts_with(&expected_root) {
    return Err(anyhow!(
      "Resolved stack run directory escapes its configured root"
    ));
  }
  Ok(())
}

async fn exact_labeled_volumes(
  label: &str,
  value: &str,
) -> anyhow::Result<Vec<String>> {
  let filter = format!("label={label}={value}");
  let output = run_standard_command(
    &format!(
      "docker volume ls --quiet --filter {}",
      escaped(&filter)
    ),
    CommandOptions::default(),
  )
  .await;
  if !output.success() {
    return Err(anyhow!(
      "Failed to list stack-owned volumes: {}",
      output.stderr
    ));
  }
  let mut volumes = output
    .stdout
    .lines()
    .map(str::trim)
    .filter(|line| !line.is_empty())
    .map(str::to_string)
    .collect::<Vec<_>>();
  volumes.sort();
  volumes.dedup();

  for volume in &volumes {
    let template = format!("{{{{ index .Labels {label:?} }}}}");
    let inspect = run_standard_command(
      &format!(
        "docker volume inspect --format {} -- {}",
        escaped(&template),
        escaped(volume)
      ),
      CommandOptions::default(),
    )
    .await;
    if !inspect.success() || inspect.stdout.trim() != value {
      return Err(anyhow!(
        "Volume {volume} failed exact ownership verification"
      ));
    }
  }
  Ok(volumes)
}

async fn remove_exact_labeled_volumes(
  label: &str,
  value: &str,
) -> anyhow::Result<Vec<String>> {
  let volumes = exact_labeled_volumes(label, value).await?;
  ensure_volumes_unreferenced(&volumes).await?;
  for volume in &volumes {
    let remove = run_standard_command(
      &format!("docker volume rm -- {}", escaped(volume)),
      CommandOptions::default(),
    )
    .await;
    if !remove.success() {
      return Err(anyhow!(
        "Failed to remove verified stack-owned volume {volume}: {}",
        remove.stderr
      ));
    }
  }
  Ok(volumes)
}

async fn ensure_volumes_unreferenced(
  volumes: &[String],
) -> anyhow::Result<()> {
  for volume in volumes {
    let references = run_standard_command(
      &format!(
        "docker ps --all --quiet --filter {}",
        escaped(&format!("volume={volume}"))
      ),
      CommandOptions::default(),
    )
    .await;
    if !references.success() {
      return Err(anyhow!(
        "Failed to verify references for volume {volume}: {}",
        references.stderr
      ));
    }
    if !references.stdout.trim().is_empty() {
      return Err(anyhow!(
        "Stack-owned volume {volume} is still referenced by a container"
      ));
    }
  }
  Ok(())
}

async fn wait_for_swarm_detach(project: &str) -> anyhow::Result<()> {
  let filter =
    escaped(&format!("label={SWARM_NAMESPACE_LABEL}={project}"));
  for attempt in 0..60 {
    let containers = run_standard_command(
      &format!("docker ps --all --quiet --filter {filter}"),
      CommandOptions::default(),
    )
    .await;
    if !containers.success() {
      return Err(anyhow!(
        "Failed to confirm Swarm task detachment: {}",
        containers.stderr
      ));
    }
    if containers.stdout.trim().is_empty() {
      return Ok(());
    }
    if attempt == 59 {
      return Err(anyhow!(
        "Swarm task containers are still attached after 60 seconds"
      ));
    }
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
  }
  unreachable!()
}

fn recover_prepared_transaction(
  transaction_id: &str,
  stack_name: &str,
) -> anyhow::Result<Option<Log>> {
  let transaction = transaction_directory(transaction_id)?;
  let Some(metadata) = path_entry_metadata(&transaction)? else {
    return Ok(None);
  };
  ensure_protected_directory(&transaction, &metadata)?;
  let mut state = read_state_at(&transaction)?;
  validate_state(&state, transaction_id, stack_name)?;
  match state.phase {
    DeletionPhase::Prepared => Ok(Some(Log::simple(
      "Retire stack files",
      "Stack deletion was already prepared".to_string(),
    ))),
    DeletionPhase::Committed => {
      Err(anyhow!("Stack deletion transaction is already committed"))
    }
    DeletionPhase::Preparing => {
      let visible = owned_root_for_name(stack_name)?;
      let quarantined = transaction.join(QUARANTINED_ROOT);
      let visible_exists = path_entry_exists(&visible)?;
      let quarantined_exists = path_entry_exists(&quarantined)?;
      match (state.root_present, visible_exists, quarantined_exists) {
        (true, true, false) => return Ok(None),
        (true, false, true) | (false, _, false) => {}
        _ => {
          return Err(anyhow!(
            "Stack deletion recovery state is ambiguous"
          ));
        }
      }
      state.phase = DeletionPhase::Prepared;
      write_state_at(&transaction, &state)?;
      Ok(Some(Log::simple(
        "Retire stack files",
        "Recovered the prepared stack deletion transaction"
          .to_string(),
      )))
    }
  }
}

fn retire_stack_root(
  transaction_id: &str,
  stack: &Stack,
) -> anyhow::Result<()> {
  let transaction = create_transaction_directory(transaction_id)?;
  let Some(root) =
    owned_stack_root(&periphery_config().stack_dir(), stack)?
  else {
    let state = DeletionState {
      transaction_id: transaction_id.to_string(),
      stack_name: stack.name.clone(),
      root_present: false,
      phase: DeletionPhase::Prepared,
    };
    return write_state_at(&transaction, &state);
  };
  let root_present = match fs::symlink_metadata(&root) {
    Ok(metadata) => {
      ensure_real_directory(&root, &metadata)?;
      true
    }
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
      false
    }
    Err(error) => return Err(error.into()),
  };
  let mut state = DeletionState {
    transaction_id: transaction_id.to_string(),
    stack_name: stack.name.clone(),
    root_present,
    phase: DeletionPhase::Preparing,
  };
  write_state_at(&transaction, &state)?;
  retire_visible_root(&root, &transaction, &mut state)
}

fn rollback_stack_root(
  transaction_id: &str,
  stack_name: &str,
) -> anyhow::Result<()> {
  validate_identifier(transaction_id)?;
  let transaction = transaction_directory(transaction_id)?;
  let Some(metadata) = path_entry_metadata(&transaction)? else {
    return Ok(());
  };
  ensure_protected_directory(&transaction, &metadata)?;
  let state = read_state_at(&transaction)?;
  validate_state(&state, transaction_id, stack_name)?;
  if state.phase == DeletionPhase::Committed {
    return Err(anyhow!("Committed stack files cannot be restored"));
  }
  if state.root_present {
    let visible = owned_root_for_name(stack_name)?;
    restore_retired_root(&visible, &transaction, &state)?;
  }
  fs::remove_dir_all(&transaction).with_context(|| {
    format!(
      "Failed to retire rolled back transaction {}",
      transaction.display()
    )
  })?;
  sync_directory(&deletion_base())
}

fn commit_stack_root(
  transaction_id: &str,
  stack_name: &str,
) -> anyhow::Result<Option<PathBuf>> {
  validate_identifier(transaction_id)?;
  let transaction = transaction_directory(transaction_id)?;
  let Some(metadata) = path_entry_metadata(&transaction)? else {
    return Ok(None);
  };
  ensure_protected_directory(&transaction, &metadata)?;
  let mut state = read_state_at(&transaction)?;
  validate_state(&state, transaction_id, stack_name)?;
  commit_state_at(&transaction, &mut state)?;
  Ok(Some(transaction))
}

fn retire_visible_root(
  root: &Path,
  transaction: &Path,
  state: &mut DeletionState,
) -> anyhow::Result<()> {
  if state.root_present {
    let quarantined = transaction.join(QUARANTINED_ROOT);
    if path_entry_exists(&quarantined)? {
      return Err(anyhow!(
        "Stack deletion quarantine already contains a root"
      ));
    }
    fs::rename(root, &quarantined).with_context(|| {
      format!(
        "Failed to atomically retire stack root {}",
        root.display()
      )
    })?;
    sync_directory(
      root.parent().context("Stack root has no parent")?,
    )?;
    sync_directory(transaction)?;
  }
  state.phase = DeletionPhase::Prepared;
  write_state_at(transaction, state)
}

fn restore_retired_root(
  visible: &Path,
  transaction: &Path,
  state: &DeletionState,
) -> anyhow::Result<()> {
  if !state.root_present {
    return Ok(());
  }
  let quarantined = transaction.join(QUARANTINED_ROOT);
  match (
    path_entry_exists(visible)?,
    path_entry_exists(&quarantined)?,
  ) {
    (false, true) => {
      fs::rename(&quarantined, visible).with_context(|| {
        format!("Failed to restore stack root {}", visible.display())
      })?;
      sync_directory(
        visible.parent().context("Stack root has no parent")?,
      )
    }
    (true, false) => Ok(()),
    _ => Err(anyhow!("Stack deletion rollback state is ambiguous")),
  }
}

fn commit_state_at(
  transaction: &Path,
  state: &mut DeletionState,
) -> anyhow::Result<()> {
  state.phase = DeletionPhase::Committed;
  write_state_at(transaction, state)
}

fn validate_state(
  state: &DeletionState,
  transaction_id: &str,
  stack_name: &str,
) -> anyhow::Result<()> {
  if state.transaction_id != transaction_id
    || state.stack_name != stack_name
  {
    return Err(anyhow!(
      "Stack deletion transaction identity does not match"
    ));
  }
  Ok(())
}

fn deletion_base() -> PathBuf {
  periphery_config().stack_dir().join(DELETION_DIRECTORY)
}

fn transaction_directory(
  transaction_id: &str,
) -> anyhow::Result<PathBuf> {
  validate_identifier(transaction_id)?;
  Ok(deletion_base().join(transaction_id))
}

fn create_transaction_directory(
  transaction_id: &str,
) -> anyhow::Result<PathBuf> {
  let stack_dir = periphery_config().stack_dir();
  fs::create_dir_all(&stack_dir).with_context(|| {
    format!(
      "Failed to create stack directory {}",
      stack_dir.display()
    )
  })?;
  let stack_metadata = fs::symlink_metadata(&stack_dir)?;
  ensure_real_directory(&stack_dir, &stack_metadata)?;
  let base = deletion_base();
  match fs::create_dir(&base) {
    Ok(()) => set_private_permissions(&base)?,
    Err(error)
      if error.kind() == std::io::ErrorKind::AlreadyExists =>
    {
      let metadata = fs::symlink_metadata(&base)?;
      ensure_protected_directory(&base, &metadata)?;
    }
    Err(error) => return Err(error.into()),
  }
  let transaction = transaction_directory(transaction_id)?;
  match fs::create_dir(&transaction) {
    Ok(()) => set_private_permissions(&transaction)?,
    Err(error)
      if error.kind() == std::io::ErrorKind::AlreadyExists =>
    {
      let metadata = fs::symlink_metadata(&transaction)?;
      ensure_protected_directory(&transaction, &metadata)?;
    }
    Err(error) => return Err(error.into()),
  }
  sync_directory(&stack_dir)?;
  sync_directory(&base)?;
  Ok(transaction)
}

fn read_state_at(
  transaction: &Path,
) -> anyhow::Result<DeletionState> {
  let state_path = transaction.join(STATE_FILE);
  let metadata =
    fs::symlink_metadata(&state_path).with_context(|| {
      format!(
        "Stack deletion state is missing at {}",
        state_path.display()
      )
    })?;
  if !metadata.is_file() || metadata.file_type().is_symlink() {
    return Err(anyhow!(
      "Stack deletion state is not a regular file"
    ));
  }
  #[cfg(unix)]
  {
    use std::os::unix::fs::MetadataExt as _;
    if metadata.uid() != unsafe { libc::geteuid() } {
      return Err(anyhow!(
        "Stack deletion state is not owned by Periphery"
      ));
    }
  }
  if metadata.len() > 64 * 1024 {
    return Err(anyhow!(
      "Stack deletion state is unexpectedly large"
    ));
  }
  serde_json::from_slice(&fs::read(&state_path)?)
    .context("Failed to parse stack deletion state")
}

fn write_state_at(
  transaction: &Path,
  state: &DeletionState,
) -> anyhow::Result<()> {
  let temporary =
    transaction.join(format!(".state-{}.tmp", Uuid::new_v4()));
  let mut options = OpenOptions::new();
  options.write(true).create_new(true);
  let mut file = options.open(&temporary)?;
  set_private_file_permissions(&file)?;
  file.write_all(&serde_json::to_vec(state)?)?;
  file.sync_all()?;
  fs::rename(&temporary, transaction.join(STATE_FILE))?;
  sync_directory(transaction)
}

fn owned_stack_root(
  stack_dir: &Path,
  stack: &Stack,
) -> anyhow::Result<Option<PathBuf>> {
  if !stack.config.linked_repo.is_empty() {
    return Ok(None);
  }
  Ok(Some(stack_dir.join(safe_stack_root_name(&stack.name)?)))
}

fn owned_root_for_name(stack_name: &str) -> anyhow::Result<PathBuf> {
  Ok(
    periphery_config()
      .stack_dir()
      .join(safe_stack_root_name(stack_name)?),
  )
}

fn safe_stack_root_name(stack_name: &str) -> anyhow::Result<String> {
  let name = to_path_compatible_name(stack_name);
  validate_identifier(&name)?;
  if name == DELETION_DIRECTORY {
    return Err(anyhow!(
      "Stack name conflicts with protected deletion storage"
    ));
  }
  Ok(name)
}

fn validate_owned_stack_root(stack: &Stack) -> anyhow::Result<()> {
  let Some(root) =
    owned_stack_root(&periphery_config().stack_dir(), stack)?
  else {
    return Ok(());
  };
  match fs::symlink_metadata(&root) {
    Ok(metadata) => ensure_real_directory(&root, &metadata),
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
      Ok(())
    }
    Err(error) => Err(error.into()),
  }
}

fn validate_identifier(value: &str) -> anyhow::Result<()> {
  if value.is_empty()
    || value.len() > 128
    || value == "."
    || value == ".."
    || !value.bytes().all(|byte| {
      byte.is_ascii_alphanumeric() || b"-_.".contains(&byte)
    })
  {
    return Err(anyhow!("Invalid stack deletion identifier"));
  }
  Ok(())
}

fn validate_relative_path(
  value: &str,
  allow_empty: bool,
) -> anyhow::Result<()> {
  if value.is_empty() && allow_empty {
    return Ok(());
  }
  let path = Path::new(value);
  if value.is_empty()
    || path.is_absolute()
    || path.components().any(|component| {
      matches!(
        component,
        Component::ParentDir
          | Component::RootDir
          | Component::Prefix(_)
      )
    })
  {
    return Err(anyhow!(
      "Stack deletion requires safe relative stack paths"
    ));
  }
  Ok(())
}

fn ensure_real_directory(
  path: &Path,
  metadata: &fs::Metadata,
) -> anyhow::Result<()> {
  if !metadata.is_dir() || metadata.file_type().is_symlink() {
    return Err(anyhow!(
      "Protected stack deletion path is not a real directory: {}",
      path.display()
    ));
  }
  Ok(())
}

fn path_entry_metadata(
  path: &Path,
) -> anyhow::Result<Option<fs::Metadata>> {
  match fs::symlink_metadata(path) {
    Ok(metadata) => Ok(Some(metadata)),
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
      Ok(None)
    }
    Err(error) => Err(error.into()),
  }
}

fn path_entry_exists(path: &Path) -> anyhow::Result<bool> {
  Ok(path_entry_metadata(path)?.is_some())
}

fn ensure_protected_directory(
  path: &Path,
  metadata: &fs::Metadata,
) -> anyhow::Result<()> {
  validate_protected_directory(path, metadata)?;
  set_private_permissions(path)
}

fn validate_protected_directory(
  path: &Path,
  metadata: &fs::Metadata,
) -> anyhow::Result<()> {
  ensure_real_directory(path, metadata)?;
  #[cfg(unix)]
  {
    use std::os::unix::fs::MetadataExt as _;
    if metadata.uid() != unsafe { libc::geteuid() } {
      return Err(anyhow!(
        "Protected stack deletion path is not owned by Periphery: {}",
        path.display()
      ));
    }
  }
  Ok(())
}

fn escaped(value: &str) -> String {
  escape(Cow::Borrowed(value)).into_owned()
}

fn sync_directory(path: &Path) -> anyhow::Result<()> {
  #[cfg(unix)]
  fs::File::open(path)?.sync_all()?;
  Ok(())
}

fn set_private_permissions(path: &Path) -> anyhow::Result<()> {
  #[cfg(unix)]
  {
    use std::os::unix::fs::PermissionsExt as _;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
  }
  Ok(())
}

fn set_private_file_permissions(
  file: &fs::File,
) -> anyhow::Result<()> {
  #[cfg(unix)]
  {
    use std::os::unix::fs::PermissionsExt as _;
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
  }
  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;

  fn test_directory(name: &str) -> PathBuf {
    std::env::temp_dir()
      .join(format!("komodo-stack-delete-{name}-{}", Uuid::new_v4()))
  }

  fn deletion_state() -> DeletionState {
    DeletionState {
      transaction_id: "stack-id".into(),
      stack_name: "sites".into(),
      root_present: true,
      phase: DeletionPhase::Preparing,
    }
  }

  #[test]
  fn pending_deletion_gate_allows_only_committed_or_absent_state() {
    let root = tempfile::tempdir().unwrap();
    let base = root.path().join(DELETION_DIRECTORY);
    ensure_no_pending_deletions_at(&base).unwrap();
    fs::create_dir(&base).unwrap();
    ensure_no_pending_deletions_at(&base).unwrap();
    let transaction = base.join("stack-id");
    fs::create_dir(&transaction).unwrap();
    assert!(ensure_no_pending_deletions_at(&base).is_err());
    let mut state = deletion_state();
    for phase in [DeletionPhase::Preparing, DeletionPhase::Prepared] {
      state.phase = phase;
      write_state_at(&transaction, &state).unwrap();
      assert!(ensure_no_pending_deletions_at(&base).is_err());
    }
    state.phase = DeletionPhase::Committed;
    write_state_at(&transaction, &state).unwrap();
    ensure_no_pending_deletions_at(&base).unwrap();
    fs::write(transaction.join(STATE_FILE), "invalid state").unwrap();
    assert!(ensure_no_pending_deletions_at(&base).is_err());
  }

  #[cfg(unix)]
  #[test]
  fn pending_deletion_gate_rejects_symlinked_transactions() {
    let root = tempfile::tempdir().unwrap();
    let base = root.path().join(DELETION_DIRECTORY);
    let outside = root.path().join("outside");
    fs::create_dir(&base).unwrap();
    fs::create_dir(&outside).unwrap();
    let mut state = deletion_state();
    state.phase = DeletionPhase::Committed;
    write_state_at(&outside, &state).unwrap();
    std::os::unix::fs::symlink(&outside, base.join("stack-id"))
      .unwrap();
    assert!(ensure_no_pending_deletions_at(&base).is_err());
  }

  #[test]
  fn owned_stack_root_is_exact_and_linked_repos_are_retained() {
    let stack_dir = Path::new("/var/lib/komodo/stacks");
    let mut stack = Stack {
      name: "My Stack".into(),
      ..Default::default()
    };
    assert_eq!(
      owned_stack_root(stack_dir, &stack).unwrap(),
      Some(stack_dir.join(to_path_compatible_name(&stack.name)))
    );

    stack.config.linked_repo = "repo-id".into();
    assert_eq!(owned_stack_root(stack_dir, &stack).unwrap(), None);
  }

  #[test]
  fn deletion_identifiers_and_relative_paths_fail_closed() {
    for invalid in ["", ".", "..", "../escape", "with/slash"] {
      assert!(validate_identifier(invalid).is_err());
    }
    for invalid in ["/absolute", "../escape", "safe/../../escape"] {
      assert!(validate_relative_path(invalid, false).is_err());
    }
    assert!(
      validate_relative_path("nested/compose.yaml", false).is_ok()
    );
    assert!(validate_relative_path("", true).is_ok());
  }

  #[test]
  fn retired_stack_root_can_be_restored_exactly() {
    let base = test_directory("rollback");
    let visible = base.join("sites");
    let transaction = base.join("transaction");
    fs::create_dir_all(visible.join("nested")).unwrap();
    fs::create_dir(&transaction).unwrap();
    fs::write(visible.join("nested/compose.yaml"), "old").unwrap();

    let mut state = deletion_state();
    write_state_at(&transaction, &state).unwrap();
    retire_visible_root(&visible, &transaction, &mut state).unwrap();

    assert_eq!(state.phase, DeletionPhase::Prepared);
    assert!(!visible.exists());
    assert_eq!(
      fs::read_to_string(
        transaction
          .join(QUARANTINED_ROOT)
          .join("nested/compose.yaml")
      )
      .unwrap(),
      "old"
    );

    restore_retired_root(&visible, &transaction, &state).unwrap();
    assert_eq!(
      fs::read_to_string(visible.join("nested/compose.yaml"))
        .unwrap(),
      "old"
    );
    assert!(!transaction.join(QUARANTINED_ROOT).exists());

    fs::remove_dir_all(&base).unwrap();
  }

  #[test]
  fn committed_cleanup_preserves_same_name_recreation() {
    let base = test_directory("recreate");
    let visible = base.join("sites");
    let transaction = base.join("transaction");
    fs::create_dir_all(&visible).unwrap();
    fs::create_dir(&transaction).unwrap();
    fs::write(visible.join("compose.yaml"), "old").unwrap();

    let mut state = deletion_state();
    write_state_at(&transaction, &state).unwrap();
    retire_visible_root(&visible, &transaction, &mut state).unwrap();
    fs::create_dir(&visible).unwrap();
    fs::write(visible.join("compose.yaml"), "new").unwrap();

    assert!(
      restore_retired_root(&visible, &transaction, &state).is_err()
    );
    assert_eq!(
      fs::read_to_string(visible.join("compose.yaml")).unwrap(),
      "new"
    );
    assert_eq!(
      fs::read_to_string(
        transaction.join(QUARANTINED_ROOT).join("compose.yaml")
      )
      .unwrap(),
      "old"
    );

    commit_state_at(&transaction, &mut state).unwrap();
    assert_eq!(
      read_state_at(&transaction).unwrap().phase,
      DeletionPhase::Committed
    );
    fs::remove_dir_all(&transaction).unwrap();

    assert_eq!(
      fs::read_to_string(visible.join("compose.yaml")).unwrap(),
      "new"
    );
    fs::remove_dir_all(&base).unwrap();
  }

  #[cfg(unix)]
  #[test]
  fn symlink_stack_root_is_rejected() {
    use std::os::unix::fs::symlink;

    let base = test_directory("symlink");
    let target = base.join("target");
    let visible = base.join("sites");
    fs::create_dir_all(&target).unwrap();
    symlink(&target, &visible).unwrap();

    let metadata = fs::symlink_metadata(&visible).unwrap();
    assert!(ensure_real_directory(&visible, &metadata).is_err());

    fs::remove_dir_all(&base).unwrap();
  }

  #[cfg(unix)]
  #[test]
  fn rollback_never_overwrites_a_broken_symlink() {
    use std::os::unix::fs::symlink;

    let base = test_directory("broken-symlink");
    let visible = base.join("sites");
    let transaction = base.join("transaction");
    fs::create_dir_all(transaction.join(QUARANTINED_ROOT)).unwrap();
    symlink(base.join("missing-target"), &visible).unwrap();

    let state = deletion_state();
    assert!(
      restore_retired_root(&visible, &transaction, &state).is_err()
    );
    assert!(fs::symlink_metadata(&visible).is_ok());
    assert!(transaction.join(QUARANTINED_ROOT).is_dir());

    fs::remove_dir_all(&base).unwrap();
  }
}
