# File Manager

Komodo's File Manager provides browser-based access to files belonging to a Stack or a Docker named volume. Open the **Files** tab on a Stack or on a named volume beneath a Server.

File Manager clients select a logical Stack or Volume target. Core and Periphery resolve that target to a trusted filesystem root; clients cannot submit arbitrary absolute host paths. Paths within the target are root-relative, and directory traversal through parent components or symlinked directories is rejected.

## Supported Targets

| Target | Filesystem root | Write behavior |
| --- | --- | --- |
| Stack | The directory containing the Stack's primary Compose file inside its resolved run directory. | UI-managed and host-file Stacks can be changed. Repository-backed Stacks are read-only. Swarm Stacks are not supported. |
| Volume | The mountpoint returned by Docker for the selected named volume. Roots that overlap the private File Manager journal are excluded. | Read and write access depend on the user's Server permissions and Periphery's host access. |

For a UI-managed Stack, the primary Compose file is marked **managed**. It can be changed only in the text editor so Komodo can update the Stack configuration and roll back the host write if that update fails. It cannot be renamed, moved, deleted, or replaced by an upload.

## Permissions

File Manager requires both a base permission level and the `FileManager` specific permission:

- **Read** or **Execute**, plus `FileManager`, allows browsing, reading text files, and downloading files.
- **Write**, plus `FileManager`, allows mutating operations when the target itself is writable.
- Admins satisfy both permission checks automatically, although intrinsic read-only target constraints still apply. `KOMODO_TRANSPARENT_MODE=true` supplies only the base Read level; it does not grant `FileManager`.
- `KOMODO_UI_WRITE_DISABLED=true` makes File Manager read-only even when the user has Write permission.

A Stack accepts `FileManager` directly on the Stack or inherits it from its attached Server; the user still needs the required base level on the Stack. A Volume uses the base level and `FileManager` permission on its Server.

:::warning Server-wide volume access
A Server-level `FileManager` grant covers **every eligible Docker named volume on that Server**. Permissions cannot currently be narrowed to an individual volume. Volumes can contain application configuration, credentials, or database files, so grant this permission only to users and groups trusted with all named-volume contents on that host.

Periphery refuses a Volume root whose directory tree equals, contains, or is contained by `${PERIPHERY_ROOT_DIRECTORY}/file-manager-journal`. The check follows filesystem directory identity, so bind mounts, named-volume aliases, and symlink aliases do not bypass it. The official `${COMPOSE_PROJECT_NAME}_data` volume—normally `komodo_data`—is still listed with other Docker volumes, but its Files tab reports File Manager unavailable because the volume contains Periphery's private journal and credentials.
:::

See [Permissioning](/docs/configuration/permissioning#specific-permissions) for permission examples.

## File Operations

Depending on the target and permission level, File Manager supports:

- Creating files and folders, editing UTF-8 text files, and uploading or downloading files.
- Multi-selection, drag-and-drop moves, cut, copy, paste, rename, and delete.
- Creating and extracting common archive formats.
- Conflict review with an explicit choice to overwrite or skip each collision.
- Progress reporting and cancellation for long-running operations.

Create, create-folder, rename, move, copy, and delete operations have per-user, per-target undo and redo history. Recovery data is stored beneath `${PERIPHERY_ROOT_DIRECTORY}/file-manager-journal` and expires after 24 hours. Text edits, uploads, and archive operations are not added to the undo history.

### Keyboard Shortcuts

When focus is within the file explorer, including on a selection checkbox, these shortcuts are available:

| Shortcut | Action |
| --- | --- |
| <kbd>Ctrl</kbd>/<kbd>Cmd</kbd> + <kbd>A</kbd> | Select every entry in the current directory. |
| <kbd>Ctrl</kbd>/<kbd>Cmd</kbd> + <kbd>C</kbd>, <kbd>X</kbd>, or <kbd>V</kbd> | Copy, cut, or paste using the File Manager clipboard. |
| <kbd>Ctrl</kbd>/<kbd>Cmd</kbd> + <kbd>Z</kbd> | Undo the latest supported operation. |
| <kbd>Ctrl</kbd>/<kbd>Cmd</kbd> + <kbd>Y</kbd>, or <kbd>Ctrl</kbd>/<kbd>Cmd</kbd> + <kbd>Shift</kbd> + <kbd>Z</kbd> | Redo. |
| <kbd>Arrow Up</kbd>/<kbd>Arrow Down</kbd> | Move the selection. |
| <kbd>Arrow Left</kbd> | Open the parent directory. |
| <kbd>Arrow Right</kbd> or <kbd>Enter</kbd> | Open the selected directory or text file. |
| <kbd>Delete</kbd> | Delete the selected entries after confirmation. |
| <kbd>Escape</kbd> | Clear the selection or close the editor. |

Text inputs and the text editor keep their native editing shortcuts instead of triggering explorer actions.

## Limits and Capacity

Text files larger than 4 MiB cannot be opened in the editor, but they can still be downloaded. File operations also enforce path-depth, archive-expansion, entry-count, and free-space limits before changing data.

The entry limit defaults to `1000000` entries per operation. Set a positive value in Periphery configuration when a legitimate directory tree needs a different ceiling:

```toml
file_manager_max_entries = 1000000
```

The equivalent environment variable is `PERIPHERY_FILE_MANAGER_MAX_ENTRIES`.

## Containerized Periphery

Docker reports named-volume mountpoints in the host's path namespace. When Periphery runs in a container, Docker-volume access therefore requires Docker's volume root to be mounted read/write at the **same absolute path** inside the container. The official Compose templates use `PERIPHERY_DOCKER_VOLUME_ROOT` for this mapping and also place Periphery's repository, Stack, and build workspace aliases beneath it.

Changing that bind mount to read-only, or mounting only `komodo_data`, disables writable Docker-volume management and breaks the official templates' host-visible workspace paths. This makes the Periphery container part of the Docker host's trusted computing boundary; it already has equivalent host control through the Docker socket. Protect the Core-to-Periphery connection and apply the Server permission warning above.

See [Connect More Servers](/docs/setup/connect-servers#container) for the template and rootless-Docker configuration.
