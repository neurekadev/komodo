#!/bin/bash
# Image-owned defaults for the repository's Compose deployment only.
# Ordinary Core, Periphery, and AWS builder commands do not use these profiles.

komodo_core_compose_defaults() {
  export KOMODO_DATABASE_ADDRESS="${KOMODO_DATABASE_ADDRESS:-komodo-database:27017}"
  export KOMODO_PRIVATE_KEY="${KOMODO_PRIVATE_KEY:-file:/data/keys/core.key}"
  export KOMODO_REPORTING_PRIVATE_KEY="${KOMODO_REPORTING_PRIVATE_KEY:-file:/data/keys/reporting.key}"
  export KOMODO_PERIPHERY_PUBLIC_KEY="${KOMODO_PERIPHERY_PUBLIC_KEY:-file:/data/keys/periphery.pub}"
  export KOMODO_CLI_BACKUPS_FOLDER="${KOMODO_CLI_BACKUPS_FOLDER:-/data/backups}"
  export KOMODO_SYNC_DIRECTORY="${KOMODO_SYNC_DIRECTORY:-/data/syncs}"
  export KOMODO_REPO_DIRECTORY="${KOMODO_REPO_DIRECTORY:-/data/repo-cache}"
  export KOMODO_LOCAL_AUTH="${KOMODO_LOCAL_AUTH-true}"
  export KOMODO_FIRST_SERVER_NAME="${KOMODO_FIRST_SERVER_NAME-Local}"
}

komodo_periphery_compose_defaults() {
  # The main installation shares Core's env file. Standalone agents supply their
  # own Core address. Preserve explicit empty addresses for inbound connections.
  if [[ ! -v PERIPHERY_CORE_ADDRESSES && ! -v PERIPHERY_CORE_ADDRESS ]]; then
    if [[ -n "${KOMODO_HOST:-}" ]]; then
      export PERIPHERY_CORE_ADDRESS=ws://komodo:9120
    else
      printf '%s\n' 'Set PERIPHERY_CORE_ADDRESS in .env for standalone Periphery.' >&2
      return 1
    fi
  fi

  export PERIPHERY_CONNECT_AS="${PERIPHERY_CONNECT_AS:-${KOMODO_FIRST_SERVER_NAME:-Local}}"
  export PERIPHERY_PRIVATE_KEY="${PERIPHERY_PRIVATE_KEY:-file:/data/keys/periphery.key}"
  export PERIPHERY_CORE_PUBLIC_KEYS="${PERIPHERY_CORE_PUBLIC_KEYS:-file:/data/keys/core.pub}"
  export PERIPHERY_ROOT_DIRECTORY="${PERIPHERY_ROOT_DIRECTORY:-/data}"

  # Keep workspaces in the host Docker daemon's path namespace. Existing custom
  # project names, Docker roots, and per-directory overrides remain supported.
  local host_data="${PERIPHERY_DOCKER_VOLUME_ROOT:-/var/lib/docker/volumes}/${COMPOSE_PROJECT_NAME:-komodo}_data/_data"
  export PERIPHERY_REPO_DIR="${PERIPHERY_REPO_DIR:-$host_data/repos}"
  export PERIPHERY_STACK_DIR="${PERIPHERY_STACK_DIR:-$host_data/stacks}"
  export PERIPHERY_BUILD_DIR="${PERIPHERY_BUILD_DIR:-$host_data/builds}"
  export PERIPHERY_INCLUDE_DISK_MOUNTS="${PERIPHERY_INCLUDE_DISK_MOUNTS-/etc/hostname}"
}
