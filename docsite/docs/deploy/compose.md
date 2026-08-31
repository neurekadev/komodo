# Docker Compose

Komodo can deploy Docker Compose projects through the `Stack` resource.

## Configuration

```toml
[[stack]]
name = "my-stack"
[stack.config]
server = "server-prod"
run_directory = "/opt/stacks/my-stack"
file_paths = ["compose.yaml"]
env_file_path = ".env"
git_account = "my-user"
repo = "myorg/stacks"
environment = """
DB_HOST = db.example.com
LOG_LEVEL = info
"""
```

### Config fields

| Field | Description | Default |
|---|---|---|
| `server` | The Server to deploy on. | — |
| `file_paths` | List of compose files. Supports composing multiple files via `docker compose -f ... -f ...`. | `[]` |
| `env_file_path` | Path, relative to the run directory, where Komodo writes `environment` before running Compose. It must resolve to a different path than every entry in `file_paths`. | `.env` |
| `run_directory` | Working directory for compose commands. | — |
| `project_name` | Override the compose project name. Defaults to the Stack name. | Stack name |
| `environment` | Environment variables written to `env_file_path` and passed via `--env-file`. Supports [variable interpolation](../configuration/variables.md). | `""` |
| `extra_args` | Additional flags passed to `docker compose up`. | `""` |
| `ignore_services` | Services to exclude from health checks (e.g. init containers that exit after startup). | `[]` |
| `git_provider` | Git provider domain. | `github.com` |
| `git_account` | Git provider account for private repos. | — |
| `repo` | Repository in `owner/repo` format. | — |
| `branch` | Branch to clone. | `main` |
| `auto_update` | Automatically redeploy when newer image digests are available. | `false` |
| `poll_for_updates` | Check for newer images and show an update indicator. | `false` |
| `send_alerts` | Send alerts on stack state changes. | `true` |
| `links` | Quick links displayed in the resource header. | `[]` |

Komodo rejects Stack configurations where a Compose file and `env_file_path` resolve to the same lexical location, including aliases such as `./.env` and `config/../.env`. Keep the Compose and generated environment files at distinct paths so one cannot overwrite the other.

## Defining Compose Files

Stacks support three ways to provide compose files:

1. **Write in the UI** — Komodo writes the files to the host at deploy time.
2. **Files on the host** — Point to existing files on the server.
3. **Git repo** — Komodo clones the repo onto the host to deploy. Changes are tracked in git and you can use [webhooks](../automate/webhooks.md) to auto-redeploy on push.

The Stack **Files** tab opens the directory containing the primary Compose file. UI-managed and host-file Stacks can be changed by users with Write and `FileManager` permission; repository-backed Stacks are read-only, and Swarm Stacks do not expose File Manager. See [File Manager](../file-manager.md) for supported operations and permissions.

## Importing Existing Projects

To import a running compose project, create a Stack in Komodo with access to the same compose files and attach the correct Server. Komodo matches projects by compose project name — if the running project name differs from the Stack name, set a custom `project_name` in the config. Run `docker compose ls` on the host to find existing project names.

## Deploying to a Swarm

A Stack can target a **Swarm** instead of a single Server to deploy via `docker stack deploy`. See [Swarm](../swarm.md) for details.
