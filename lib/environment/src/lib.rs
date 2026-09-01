use std::path::{Path, PathBuf};

use anyhow::Context;
use formatting::format_serror;
use komodo_client::entities::{EnvironmentVar, update::Log};

/// Render environment variables exactly as stack deployment writes them.
pub fn render_env_file(environment: &[EnvironmentVar]) -> String {
  let contents = environment
    .iter()
    .map(|env| format!("{}={}", env.variable, env.value))
    .collect::<Vec<_>>()
    .join("\n");

  if contents.is_empty() || contents.ends_with('\n') {
    contents
  } else {
    contents + "\n"
  }
}

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

  let contents = render_env_file(environment);

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
    render_env_file(environment),
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

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn deployment_formatter_handles_values_and_empty_sources() {
    assert_eq!(render_env_file(&[]), "");
    assert_eq!(
      render_env_file(&[
        EnvironmentVar {
          variable: "FIRST".into(),
          value: "one".into(),
        },
        EnvironmentVar {
          variable: "QUOTED".into(),
          value: "\"two words\"".into(),
        },
      ]),
      "FIRST=one\nQUOTED=\"two words\"\n"
    );
  }

  #[tokio::test]
  async fn managed_empty_environment_retires_stale_host_values() {
    let directory = std::env::temp_dir().join(format!(
      "komodo-environment-test-{}-{}",
      std::process::id(),
      std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
    ));
    tokio::fs::create_dir_all(&directory).await.unwrap();
    let path = directory.join(".env");
    tokio::fs::write(&path, "STALE=value\n").await.unwrap();
    let mut logs = Vec::new();

    assert_eq!(
      write_managed_env_file(&[], &directory, ".env", &mut logs)
        .await,
      Some(path.clone())
    );
    assert_eq!(tokio::fs::read_to_string(&path).await.unwrap(), "");
    assert!(logs.iter().all(|log| log.success));
    tokio::fs::remove_dir_all(directory).await.unwrap();
  }
}
