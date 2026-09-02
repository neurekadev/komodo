#!/bin/bash
# docker exec does not inherit defaults exported by the container's entrypoint.
if [[ -f /run/komodo-compose ]]; then
  source /app/bin/compose-defaults.sh
  komodo_core_compose_defaults || exit 1
fi
exec /app/bin/km "$@"
