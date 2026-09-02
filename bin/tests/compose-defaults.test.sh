#!/bin/bash
# Isolated unit tests: shell environment only; no Docker or application processes.
set -euo pipefail
# Run with only a PATH so an operator's shell cannot supply deployment settings.
if [[ "${COMPOSE_DEFAULTS_TEST_ISOLATED:-}" != 1 ]]; then
  exec env -i PATH="$PATH" COMPOSE_DEFAULTS_TEST_ISOLATED=1 bash "$0"
fi
source "$(dirname "${BASH_SOURCE[0]}")/../compose-defaults.sh"

assert_equal() {
  if [[ "$1" != "$2" ]]; then
    printf 'Expected <%s>, got <%s>\n' "$2" "$1" >&2
    exit 1
  fi
}

(
  komodo_core_compose_defaults
  assert_equal "$KOMODO_PRIVATE_KEY" file:/data/keys/core.key
  assert_equal "$KOMODO_REPORTING_PRIVATE_KEY" file:/data/keys/reporting.key
  assert_equal "$KOMODO_PERIPHERY_PUBLIC_KEY" file:/data/keys/periphery.pub
  assert_equal "$KOMODO_CLI_BACKUPS_FOLDER" /data/backups
  assert_equal "$KOMODO_SYNC_DIRECTORY" /data/syncs
  assert_equal "$KOMODO_REPO_DIRECTORY" /data/repo-cache
  assert_equal "$KOMODO_DATABASE_ADDRESS" komodo-database:27017
  assert_equal "$KOMODO_FIRST_SERVER_NAME" Local
  assert_equal "$KOMODO_LOCAL_AUTH" true
)

(
  export KOMODO_HOST=https://core.test KOMODO_FIRST_SERVER_NAME='Existing Server'
  komodo_core_compose_defaults
  komodo_periphery_compose_defaults
  assert_equal "$PERIPHERY_CONNECT_AS" 'Existing Server'
  assert_equal "$PERIPHERY_CORE_ADDRESS" ws://komodo:9120
  assert_equal "$PERIPHERY_PRIVATE_KEY" file:/data/keys/periphery.key
  assert_equal "$PERIPHERY_CORE_PUBLIC_KEYS" file:/data/keys/core.pub
  assert_equal "$PERIPHERY_ROOT_DIRECTORY" /data
  assert_equal "$PERIPHERY_STACK_DIR" /var/lib/docker/volumes/komodo_data/_data/stacks
  assert_equal "$PERIPHERY_INCLUDE_DISK_MOUNTS" /etc/hostname
)

(
  export PERIPHERY_CORE_ADDRESS=https://remote.test PERIPHERY_CONNECT_AS=Remote
  export COMPOSE_PROJECT_NAME=custom PERIPHERY_DOCKER_VOLUME_ROOT='/custom docker/volumes'
  komodo_periphery_compose_defaults
  assert_equal "$PERIPHERY_CORE_ADDRESS" https://remote.test
  assert_equal "$PERIPHERY_CONNECT_AS" Remote
  assert_equal "$PERIPHERY_REPO_DIR" '/custom docker/volumes/custom_data/_data/repos'
  assert_equal "$PERIPHERY_STACK_DIR" '/custom docker/volumes/custom_data/_data/stacks'
  assert_equal "$PERIPHERY_BUILD_DIR" '/custom docker/volumes/custom_data/_data/builds'
)

(
  export KOMODO_HOST=https://core.test PERIPHERY_CORE_ADDRESS=''
  export PERIPHERY_PRIVATE_KEY=file:/custom/key PERIPHERY_CORE_PUBLIC_KEYS=file:/custom/core.pub
  export PERIPHERY_ROOT_DIRECTORY=/custom PERIPHERY_REPO_DIR=/custom/repos
  export PERIPHERY_STACK_DIR=/custom/stacks PERIPHERY_BUILD_DIR=/custom/builds
  export PERIPHERY_INCLUDE_DISK_MOUNTS=''
  komodo_periphery_compose_defaults
  assert_equal "$PERIPHERY_CORE_ADDRESS" ''
  assert_equal "$PERIPHERY_PRIVATE_KEY" file:/custom/key
  assert_equal "$PERIPHERY_CORE_PUBLIC_KEYS" file:/custom/core.pub
  assert_equal "$PERIPHERY_ROOT_DIRECTORY" /custom
  assert_equal "$PERIPHERY_REPO_DIR" /custom/repos
  assert_equal "$PERIPHERY_STACK_DIR" /custom/stacks
  assert_equal "$PERIPHERY_BUILD_DIR" /custom/builds
  assert_equal "$PERIPHERY_INCLUDE_DISK_MOUNTS" ''
)

(
  export PERIPHERY_CORE_ADDRESSES=https://one.test,https://two.test
  komodo_periphery_compose_defaults
  [[ ! -v PERIPHERY_CORE_ADDRESS ]]
)

(
  export KOMODO_FIRST_SERVER_NAME='' KOMODO_LOCAL_AUTH=false
  export KOMODO_DATABASE_ADDRESS=external:27017 KOMODO_PRIVATE_KEY=file:/custom/core.key
  export KOMODO_CLI_BACKUPS_FOLDER=/custom/backups
  komodo_core_compose_defaults
  assert_equal "$KOMODO_FIRST_SERVER_NAME" ''
  assert_equal "$KOMODO_LOCAL_AUTH" false
  assert_equal "$KOMODO_DATABASE_ADDRESS" external:27017
  assert_equal "$KOMODO_PRIVATE_KEY" file:/custom/core.key
  assert_equal "$KOMODO_CLI_BACKUPS_FOLDER" /custom/backups
)

if (komodo_periphery_compose_defaults 2>/dev/null); then
  printf '%s\n' 'Standalone Periphery accepted a missing Core address.' >&2
  exit 1
fi

# Exercise entrypoint dispatch with shell stubs; never start services or modify
# certificates/the filesystem. Preserve arguments containing spaces verbatim.
test_entrypoint() (
  local profile="$1" expected_command="$2" marker=''
  local defaults="$(dirname "${BASH_SOURCE[0]}")/../compose-defaults.sh"
  source() {
    assert_equal "$1" /app/bin/compose-defaults.sh
    builtin source "$defaults"
  }
  touch() { assert_equal "$1" /run/komodo-compose; marker=1; }
  update-ca-certificates() { :; }
  exec() {
    assert_equal "$1" "$expected_command"
    assert_equal "$2" 'argument with spaces'
    case "$profile" in
      core-compose)
        assert_equal "$marker" 1
        assert_equal "$KOMODO_CLI_BACKUPS_FOLDER" /data/backups
        ;;
      periphery-compose)
        assert_equal "$PERIPHERY_PRIVATE_KEY" file:/data/keys/periphery.key
        ;;
      core)
        [[ ! -v KOMODO_DATABASE_ADDRESS && ! -v KOMODO_PRIVATE_KEY ]]
        ;;
      periphery)
        assert_equal "$PERIPHERY_PRIVATE_KEY" "${EXPECTED_KEY:-file:/config/keys/periphery.key}"
        [[ ! -v PERIPHERY_CORE_ADDRESS && ! -v PERIPHERY_ROOT_DIRECTORY ]]
        ;;
    esac
  }
  export KOMODO_HOST=https://core.test
  builtin source "$(dirname "${BASH_SOURCE[0]}")/../entrypoint.sh" "$profile" 'argument with spaces'
)

test_entrypoint core-compose core
test_entrypoint periphery-compose periphery
test_entrypoint core core
test_entrypoint periphery periphery
(export PERIPHERY_PRIVATE_KEY=file:/etc/komodo/keys/periphery.key EXPECTED_KEY=file:/etc/komodo/keys/periphery.key; test_entrypoint periphery periphery)

printf '%s\n' 'Compose default unit tests passed.'
