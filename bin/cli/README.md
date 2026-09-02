# Komodo CLI

The `km` CLI executes actions, manages resources, opens terminal sessions, and performs database maintenance against Komodo Core.

## Supported usage

Run the CLI included in the Core container:

```sh
docker exec -it komodo km --help
```

Or run the standalone container image with your configuration directory mounted:

```sh
docker run --rm -it \
  -v "$HOME/.config/komodo:/config" \
  ghcr.io/neurekadev/komodo-cli:3 km --help
```

Keep `-it` when replacing `--help` with an interactive command such as `connect`, `exec`, or `attach`. For unattended commands, omit `-it` and pass `-y` where the command requires confirmation.

Komodo does not publish native CLI binaries or crates. Developers may build `komodo_cli` from this repository for self-supported use.
