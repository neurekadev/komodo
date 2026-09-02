#!/bin/bash

# Only the explicit Compose commands apply the shared-volume defaults.
case "${1:-}" in
  core-compose)
    source /app/bin/compose-defaults.sh
    komodo_core_compose_defaults || exit 1
    # docker exec starts with the image environment, not PID 1's exports.
    # Let the bundled km wrapper select the same non-secret defaults.
    touch /run/komodo-compose || exit 1
    shift
    set -- core "$@"
    ;;
  periphery-compose)
    source /app/bin/compose-defaults.sh
    komodo_periphery_compose_defaults || exit 1
    shift
    set -- periphery "$@"
    ;;
  periphery)
    # Preserve the non-Compose image default, including explicit env overrides.
    export PERIPHERY_PRIVATE_KEY="${PERIPHERY_PRIVATE_KEY-file:/config/keys/periphery.key}"
    ;;
esac

## Update certificates.
update-ca-certificates

## Let the actual command take over
exec "$@"
