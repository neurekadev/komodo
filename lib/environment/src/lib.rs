use std::path::{Path, PathBuf};

use anyhow::Context;
use formatting::format_serror;
use komodo_client::entities::{
  EnvironmentVar, render_environment_file, update::Log,
};

/// If the environment was written and needs to be passed to the compose command,
/// will return the env file PathBuf.
/// Should ensure all logs are successful after calling.
pub async fn write_env_file(
  environment: &[EnvironmentVar],
  folder: &Path,
  env_file_path: &str,
  logs: &mut Vec<Log>,
) -> Option<PathBuf> {
  let env_file_path =
    folder.join(env_file_path).components().collect::<PathBuf>();

  if environment.is_empty() {
    // Still want to return Some(env_file_path) if the path
    // already exists on the host and is a file.
    // This is for "Files on Server" mode when user writes the env file themself.
    if env_file_path.is_file() {
      return Some(env_file_path);
    }
    return None;
  }

  let contents = render_environment_file(environment);

  write_rendered_env_file(env_file_path, contents, logs).await
}

/// Write a UI-managed environment file even when the source is empty, so a
/// cleared database value cannot leave stale variables on the host.
pub async fn write_managed_env_file(
  environment: &[EnvironmentVar],
  folder: &Path,
  env_file_path: &str,
  logs: &mut Vec<Log>,
) -> Option<PathBuf> {
  let env_file_path =
    folder.join(env_file_path).components().collect::<PathBuf>();
  write_rendered_env_file(
    env_file_path,
    render_environment_file(environment),
    logs,
  )
  .await
}

async fn write_rendered_env_file(
  env_file_path: PathBuf,
  contents: String,
  logs: &mut Vec<Log>,
) -> Option<PathBuf> {
  if let Err(e) =
    mogh_secret_file::write_async(&env_file_path, contents)
      .await
      .with_context(|| {
        format!(
          "Failed to write environment file to {env_file_path:?}"
        )
      })
  {
    logs.push(Log::error(
      "Write Environment File",
      format_serror(&e.into()),
    ));
    return None;
  }

  logs.push(Log::simple(
    "Write Environment File",
    format!("Environment file written to {env_file_path:?}"),
  ));

  Some(env_file_path)
}
