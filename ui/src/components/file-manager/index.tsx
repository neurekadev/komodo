import { KOMODO_BASE_URL } from "@/main";
import { useRead, useWrite } from "@/lib/hooks";
import { ICONS } from "@/lib/icons";
import {
  ActionIcon,
  Alert,
  Badge,
  Box,
  Button,
  Checkbox,
  Divider,
  Group,
  Loader,
  Menu,
  Modal,
  Progress,
  ScrollArea,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
  Tooltip,
} from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import { notifications } from "@mantine/notifications";
import { useQueryClient } from "@tanstack/react-query";
import { MoghAuth, Types } from "komodo_client";
import {
  Archive,
  ArrowDownAZ,
  ArrowUpAZ,
  Check,
  ChevronDown,
  ChevronRight,
  Clipboard,
  Copy,
  File,
  FileArchive,
  Folder,
  FolderOpen,
  FolderPlus,
  Pencil,
  Redo2,
  Scissors,
  Undo2,
  Upload,
} from "lucide-react";
import { languageFromPath, MonacoEditor, Section } from "mogh_ui";
import {
  KeyboardEvent as ReactKeyboardEvent,
  ReactNode,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

type SortKey = "name" | "size" | "modified_at";
type ClipboardState = {
  mode: "copy" | "move";
  paths: string[];
};
type ActionDialog =
  | "create-file"
  | "create-directory"
  | "rename"
  | "archive"
  | null;
type PendingCommit = {
  operation: Types.FileManagerOperation;
  preflight: Types.FileManagerPreflight;
  clearClipboardOnSuccess?: boolean;
};

const joinPath = (...parts: string[]) =>
  parts
    .filter(Boolean)
    .join("/")
    .replaceAll(/\/{2,}/g, "/")
    .replace(/^\//, "")
    .replace(/\/$/, "");

const parentPath = (path: string) => {
  const index = path.lastIndexOf("/");
  return index < 0 ? "" : path.slice(0, index);
};

const fileName = (path: string) => path.split("/").at(-1) ?? path;

const formatBytes = (bytes: number) => {
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KiB", "MiB", "GiB", "TiB"];
  let value = bytes / 1024;
  let unit = units[0];
  for (let index = 1; value >= 1024 && index < units.length; index += 1) {
    value /= 1024;
    unit = units[index];
  }
  return `${value.toFixed(value >= 10 ? 1 : 2)} ${unit}`;
};

const isEditingElement = (target: EventTarget | null) => {
  const element = target instanceof HTMLElement ? target : null;
  return !!element?.closest("input, textarea, [contenteditable=true], .monaco-editor");
};

const reportUpdateFailure = (update: Types.Update) => {
  if (update.success) return false;
  const message =
    update.logs.findLast((log) => !log.success)?.stderr ||
    "The file operation failed. Open its update for details.";
  notifications.show({ title: "File operation failed", message, color: "red" });
  return true;
};

export default function FileManager({
  target,
  titleOther,
}: {
  target: Types.FileManagerTarget;
  titleOther?: ReactNode;
}) {
  const queryClient = useQueryClient();
  const desktop = useMediaQuery("(min-width: 62em)");
  const inputRef = useRef<HTMLInputElement>(null);
  const explorerRef = useRef<HTMLDivElement>(null);
  const [path, setPath] = useState("");
  const [selected, setSelected] = useState<string[]>([]);
  const [selectionAnchor, setSelectionAnchor] = useState<string>();
  const [clipboard, setClipboard] = useState<ClipboardState>();
  const [sortKey, setSortKey] = useState<SortKey>("name");
  const [sortAscending, setSortAscending] = useState(true);
  const [editorPath, setEditorPath] = useState<string>();
  const [draft, setDraft] = useState("");
  const [action, setAction] = useState<ActionDialog>(null);
  const [actionValue, setActionValue] = useState("");
  const [archiveFormat, setArchiveFormat] =
    useState<Types.FileManagerArchiveFormat>(Types.FileManagerArchiveFormat.Zip);
  const [pendingCommit, setPendingCommit] = useState<PendingCommit>();
  const [decisions, setDecisions] = useState<
    Record<string, Types.FileManagerConflictAction>
  >({});
  const [transfer, setTransfer] = useState<{
    label: string;
    percent?: number;
    cancel?: () => void;
  }>();

  const capabilities = useRead("GetFileManagerCapabilities", { target }).data;
  const directory = useRead(
    "ListFileManagerDirectory",
    { target, path },
    { enabled: capabilities?.available === true },
  );
  const journal = useRead(
    "GetFileManagerJournalStatus",
    { target },
    { enabled: capabilities?.available === true },
  );
  const textFile = useRead(
    "ReadFileManagerText",
    { target, path: editorPath ?? "" },
    { enabled: !!editorPath && capabilities?.available === true },
  );

  const { mutateAsync: preflight, isPending: preflightPending } = useWrite(
    "PreflightFileManagerOperation",
  );
  const { mutateAsync: commit, isPending: commitPending } = useWrite(
    "CommitFileManagerOperation",
  );
  const { mutateAsync: prepareUpload } = useWrite("PrepareFileManagerUpload");
  const { mutateAsync: prepareDownload } = useWrite(
    "PrepareFileManagerDownload",
  );
  const { mutateAsync: undo, isPending: undoPending } = useWrite(
    "UndoFileManagerOperation",
  );
  const { mutateAsync: redo, isPending: redoPending } = useWrite(
    "RedoFileManagerOperation",
  );

  const readOnly = capabilities?.read_only ?? true;
  const busy =
    preflightPending || commitPending || undoPending || redoPending || !!transfer;

  const entries = useMemo(() => {
    const result = [...(directory.data?.entries ?? [])];
    result.sort((left, right) => {
      if (
        left.kind === Types.FileManagerEntryKind.Directory &&
        right.kind !== Types.FileManagerEntryKind.Directory
      )
        return -1;
      if (
        right.kind === Types.FileManagerEntryKind.Directory &&
        left.kind !== Types.FileManagerEntryKind.Directory
      )
        return 1;
      const comparison =
        sortKey === "name"
          ? left.name.localeCompare(right.name, undefined, {
              numeric: true,
              sensitivity: "base",
            })
          : left[sortKey] - right[sortKey];
      return sortAscending ? comparison : -comparison;
    });
    return result;
  }, [directory.data?.entries, sortAscending, sortKey]);

  const refresh = useCallback(async () => {
    await Promise.all([
      queryClient.invalidateQueries({
        predicate: ({ queryKey }) => queryKey[0] === "ListFileManagerDirectory",
      }),
      queryClient.invalidateQueries({
        queryKey: ["GetFileManagerJournalStatus", { target }],
      }),
      editorPath
        ? queryClient.invalidateQueries({
            queryKey: ["ReadFileManagerText", { target, path: editorPath }],
          })
        : Promise.resolve(),
    ]);
  }, [editorPath, queryClient, target]);

  useEffect(() => {
    if (textFile.data) setDraft(textFile.data.contents);
  }, [textFile.data]);

  useEffect(() => {
    setSelected([]);
    setSelectionAnchor(undefined);
    setEditorPath(undefined);
  }, [path]);

  const completeCommit = useCallback(
    async (
      plan: PendingCommit,
      conflictDecisions: Types.FileManagerConflictDecision[] = [],
    ) => {
      const update = await commit({
        target,
        plan_id: plan.preflight.plan_id,
        decisions: conflictDecisions,
        confirmed: true,
      });
      setPendingCommit(undefined);
      setDecisions({});
      await refresh();
      if (reportUpdateFailure(update)) return;
      if (plan.clearClipboardOnSuccess) setClipboard(undefined);
      setSelected([]);
      notifications.show({ message: "File operation completed.", color: "green" });
    },
    [commit, refresh, target],
  );

  const runOperation = useCallback(
    async (
      operation: Types.FileManagerOperation,
      options: { clearClipboardOnSuccess?: boolean } = {},
    ) => {
      const result = await preflight({ target, operation });
      const plan = { operation, preflight: result, ...options };
      if (result.confirmation_required || result.conflicts.length > 0) {
        setDecisions(
          Object.fromEntries(
            result.conflicts.map((conflict) => [
              conflict.path,
              Types.FileManagerConflictAction.Overwrite,
            ]),
          ),
        );
        setPendingCommit(plan);
      } else {
        await completeCommit(plan);
      }
    },
    [completeCommit, preflight, target],
  );

  const selectEntry = (
    entry: Types.FileManagerEntry,
    event: React.MouseEvent,
  ) => {
    if (event.shiftKey && selectionAnchor) {
      const start = entries.findIndex((item) => item.path === selectionAnchor);
      const end = entries.findIndex((item) => item.path === entry.path);
      if (start >= 0 && end >= 0) {
        setSelected(
          entries
            .slice(Math.min(start, end), Math.max(start, end) + 1)
            .map((item) => item.path),
        );
        return;
      }
    }
    if (event.ctrlKey || event.metaKey) {
      setSelected((current) =>
        current.includes(entry.path)
          ? current.filter((item) => item !== entry.path)
          : [...current, entry.path],
      );
    } else {
      setSelected([entry.path]);
    }
    setSelectionAnchor(entry.path);
  };

  const openEntry = (entry: Types.FileManagerEntry) => {
    if (entry.kind === Types.FileManagerEntryKind.Directory) {
      setPath(entry.path);
    } else if (entry.kind === Types.FileManagerEntryKind.File) {
      setEditorPath(entry.path);
    }
  };

  const selectedEntries = entries.filter((entry) =>
    selected.includes(entry.path),
  );
  const selectionContainsManaged = selectedEntries.some((entry) => entry.managed);
  const canChangeSelection =
    !readOnly && selected.length > 0 && !selectionContainsManaged;

  const paste = useCallback(
    async (destination = path) => {
      if (!clipboard || readOnly) return;
      await runOperation(
        {
          type: clipboard.mode === "copy" ? "Copy" : "Move",
          params: { paths: clipboard.paths, destination },
        },
        { clearClipboardOnSuccess: clipboard.mode === "move" },
      );
    },
    [clipboard, path, readOnly, runOperation],
  );

  const onKeyboard = async (event: ReactKeyboardEvent<HTMLDivElement>) => {
    if (isEditingElement(event.target)) return;
    const modifier = event.ctrlKey || event.metaKey;
    const index = entries.findIndex((entry) => entry.path === selected.at(-1));
    if (modifier && event.key.toLowerCase() === "a") {
      event.preventDefault();
      setSelected(entries.map((entry) => entry.path));
    } else if (modifier && event.key.toLowerCase() === "c" && selected.length) {
      event.preventDefault();
      setClipboard({ mode: "copy", paths: selected });
    } else if (
      modifier &&
      event.key.toLowerCase() === "x" &&
      canChangeSelection
    ) {
      event.preventDefault();
      setClipboard({ mode: "move", paths: selected });
    } else if (modifier && event.key.toLowerCase() === "v") {
      event.preventDefault();
      await paste();
    } else if (modifier && event.key.toLowerCase() === "z" && !event.shiftKey) {
      event.preventDefault();
      if (journal.data?.can_undo && !readOnly) {
        reportUpdateFailure(await undo({ target, confirmed: true }));
        await refresh();
      }
    } else if (
      modifier &&
      (event.key.toLowerCase() === "y" ||
        (event.shiftKey && event.key.toLowerCase() === "z"))
    ) {
      event.preventDefault();
      if (journal.data?.can_redo && !readOnly) {
        reportUpdateFailure(await redo({ target, confirmed: true }));
        await refresh();
      }
    } else if (event.key === "ArrowLeft") {
      event.preventDefault();
      if (path) setPath(parentPath(path));
    } else if (event.key === "ArrowRight" || event.key === "Enter") {
      const entry = entries[index];
      if (entry) {
        event.preventDefault();
        openEntry(entry);
      }
    } else if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      const next = Math.max(
        0,
        Math.min(
          entries.length - 1,
          index + (event.key === "ArrowDown" ? 1 : -1),
        ),
      );
      if (entries[next]) setSelected([entries[next].path]);
    } else if (event.key === "Delete" && canChangeSelection) {
      event.preventDefault();
      await runOperation({ type: "Delete", params: { paths: selected } });
    } else if (event.key === "Escape") {
      setSelected([]);
      setEditorPath(undefined);
    }
  };

  const openAction = (next: Exclude<ActionDialog, null>) => {
    setActionValue(next === "rename" ? fileName(selected[0] ?? "") : "");
    setAction(next);
  };

  const submitAction = async () => {
    const value = actionValue.trim();
    if (!value || !action) return;
    if (action === "create-file") {
      await runOperation({
        type: "CreateFile",
        params: { path: joinPath(path, value) },
      });
    } else if (action === "create-directory") {
      await runOperation({
        type: "CreateDirectory",
        params: { path: joinPath(path, value) },
      });
    } else if (action === "rename") {
      await runOperation({
        type: "Rename",
        params: { path: selected[0], new_name: value },
      });
    } else {
      await runOperation({
        type: "CreateArchive",
        params: {
          paths: selected,
          destination: joinPath(path, value),
          format: archiveFormat,
        },
      });
    }
    setAction(null);
  };

  const uploadFiles = async (files: File[], destination = path) => {
    if (!files.length || readOnly) return;
    let uploadedCount = 0;
    try {
      for (const file of files) {
        const collisionEntry =
          destination === path
            ? entries.find((entry) => entry.name === file.name)
            : undefined;
        const collision = !!collisionEntry;
        if (collisionEntry?.managed) {
          notifications.show({
            title: "Upload blocked",
            message: `${file.name} is managed by the stack editor and cannot be replaced by upload.`,
            color: "red",
          });
          continue;
        }
        if (
          collision &&
          !globalThis.confirm(
            `${file.name} already exists. Overwrite it with the uploaded file?`,
          )
        ) {
          continue;
        }
        setTransfer({ label: `Uploading ${file.name}`, percent: 0 });
        const ticket = await prepareUpload({
          target,
          destination,
          file_names: [file.name],
          total_bytes: file.size,
          overwrite: collision,
          confirmed: collision,
          expected_revision: collisionEntry?.revision,
        });
        await new Promise<void>((resolve, reject) => {
          const request = new XMLHttpRequest();
          request.open("POST", KOMODO_BASE_URL + ticket.url);
          const jwt = MoghAuth.LOGIN_TOKENS.jwt();
          if (jwt) request.setRequestHeader("authorization", jwt);
          request.withCredentials = true;
          setTransfer({
            label: `Uploading ${file.name}`,
            percent: 0,
            cancel: () => request.abort(),
          });
          request.upload.onprogress = (event) =>
            setTransfer({
              label: `Uploading ${file.name}`,
              percent: event.lengthComputable
                ? Math.round((event.loaded / event.total) * 100)
                : undefined,
              cancel: () => request.abort(),
            });
          request.onload = () =>
            request.status >= 200 && request.status < 300
              ? resolve()
              : reject(new Error(request.responseText || "Upload failed"));
          request.onerror = () => reject(new Error("Upload connection failed"));
          request.onabort = () =>
            reject(new DOMException("Upload cancelled", "AbortError"));
          request.send(file);
        });
        uploadedCount += 1;
      }
      if (uploadedCount > 0) {
        notifications.show({ message: "Upload completed.", color: "green" });
        await refresh();
      }
    } catch (error) {
      if ((error as DOMException)?.name !== "AbortError") {
        notifications.show({
          title: "Upload failed",
          message: error instanceof Error ? error.message : String(error),
          color: "red",
        });
      }
    } finally {
      setTransfer(undefined);
      if (inputRef.current) inputRef.current.value = "";
    }
  };

  const download = async () => {
    if (!selected.length) return;
    const controller = new AbortController();
    try {
      setTransfer({
        label: "Preparing download",
        cancel: () => controller.abort(),
      });
      const ticket = await prepareDownload({ target, paths: selected });
      const jwt = MoghAuth.LOGIN_TOKENS.jwt();
      const response = await fetch(KOMODO_BASE_URL + ticket.url, {
        headers: jwt ? { authorization: jwt } : {},
        credentials: "include",
        signal: controller.signal,
      });
      if (!response.ok) throw new Error(await response.text());
      const disposition = response.headers.get("content-disposition") ?? "";
      const downloadName =
        disposition.match(/filename="?([^";]+)"?/i)?.[1] ??
        (selected.length === 1 ? fileName(selected[0]) : "komodo-files.zip");
      const pickerWindow = window as Window & {
        showSaveFilePicker?: (options: unknown) => Promise<{
          createWritable: () => Promise<WritableStream>;
        }>;
      };
      if (pickerWindow.showSaveFilePicker && response.body) {
        const handle = await pickerWindow.showSaveFilePicker({
          suggestedName: downloadName,
        });
        await response.body.pipeTo(await handle.createWritable());
      } else {
        const blob = await response.blob();
        const link = document.createElement("a");
        link.href = URL.createObjectURL(blob);
        link.download = downloadName;
        link.click();
        URL.revokeObjectURL(link.href);
      }
      notifications.show({ message: "Download completed.", color: "green" });
    } catch (error) {
      if ((error as DOMException)?.name !== "AbortError") {
        notifications.show({
          title: "Download failed",
          message: error instanceof Error ? error.message : String(error),
          color: "red",
        });
      }
    } finally {
      setTransfer(undefined);
    }
  };

  const onDrop = async (event: React.DragEvent, destination = path) => {
    event.preventDefault();
    const droppedFiles = Array.from(event.dataTransfer.files);
    if (droppedFiles.length) {
      await uploadFiles(droppedFiles, destination);
      return;
    }
    const paths = JSON.parse(
      event.dataTransfer.getData("application/x-komodo-file-paths") || "[]",
    ) as string[];
    if (paths.length && !readOnly) {
      await runOperation({ type: "Move", params: { paths, destination } });
    }
  };

  const sortHeader = (label: string, key: SortKey) => (
    <Table.Th
      onClick={() => {
        if (sortKey === key) setSortAscending((current) => !current);
        else {
          setSortKey(key);
          setSortAscending(true);
        }
      }}
      style={{ cursor: "pointer", userSelect: "none" }}
    >
      <Group gap="xs" wrap="nowrap">
        {label}
        {sortKey === key &&
          (sortAscending ? <ArrowDownAZ size={14} /> : <ArrowUpAZ size={14} />)}
      </Group>
    </Table.Th>
  );

  const breadcrumbs = path ? path.split("/") : [];

  return (
    <Section
      title={titleOther ? undefined : "File Manager"}
      icon={titleOther ? undefined : <ICONS.FileManager size="1.3rem" />}
      titleOther={titleOther}
      gap="md"
    >
      {!capabilities ? (
        <Loader />
      ) : !capabilities.available ? (
        <Alert color="yellow" title="File Manager unavailable">
          {capabilities.reason ?? "This target cannot expose a filesystem root."}
        </Alert>
      ) : (
        <Stack gap="sm">
          {readOnly && (
            <Alert color="blue" title="Read-only source">
              {capabilities.reason ??
                "This linked source can be browsed and downloaded, but it cannot be changed here."}
            </Alert>
          )}

          <Group justify="space-between" align="center">
            <Group gap={4}>
              <Button variant="subtle" size="compact-sm" onClick={() => setPath("")}>
                Root
              </Button>
              {breadcrumbs.map((part, index) => {
                const crumbPath = breadcrumbs.slice(0, index + 1).join("/");
                return (
                  <Group gap={4} key={crumbPath}>
                    <ChevronRight size={14} />
                    <Button
                      variant="subtle"
                      size="compact-sm"
                      onClick={() => setPath(crumbPath)}
                    >
                      {part}
                    </Button>
                  </Group>
                );
              })}
            </Group>
            <Group gap={4}>
              <ToolbarButton
                label="New file"
                icon={<File size={17} />}
                disabled={readOnly || busy}
                onClick={() => openAction("create-file")}
              />
              <ToolbarButton
                label="New folder"
                icon={<FolderPlus size={17} />}
                disabled={readOnly || busy}
                onClick={() => openAction("create-directory")}
              />
              <ToolbarButton
                label="Upload"
                icon={<Upload size={17} />}
                disabled={readOnly || busy}
                onClick={() => inputRef.current?.click()}
              />
              <ToolbarButton
                label="Download"
                icon={<ICONS.Download size={17} />}
                disabled={!selected.length || selectionContainsManaged || busy}
                onClick={download}
              />
              <Divider orientation="vertical" />
              <ToolbarButton
                label="Cut"
                icon={<Scissors size={17} />}
                disabled={!canChangeSelection || busy}
                onClick={() => setClipboard({ mode: "move", paths: selected })}
              />
              <ToolbarButton
                label="Copy"
                icon={<Copy size={17} />}
                disabled={!canChangeSelection || busy}
                onClick={() => setClipboard({ mode: "copy", paths: selected })}
              />
              <ToolbarButton
                label="Paste"
                icon={<Clipboard size={17} />}
                disabled={!clipboard || readOnly || busy}
                onClick={() => paste()}
              />
              <Menu shadow="md" position="bottom-end">
                <Menu.Target>
                  <ActionIcon
                    variant="subtle"
                    aria-label="More file actions"
                    disabled={busy}
                  >
                    <Archive size={17} />
                  </ActionIcon>
                </Menu.Target>
                <Menu.Dropdown>
                  <Menu.Item
                    leftSection={<Pencil size={15} />}
                    disabled={selected.length !== 1 || !canChangeSelection}
                    onClick={() => openAction("rename")}
                  >
                    Rename
                  </Menu.Item>
                  <Menu.Item
                    leftSection={<FileArchive size={15} />}
                    disabled={!canChangeSelection}
                    onClick={() => openAction("archive")}
                  >
                    Create archive
                  </Menu.Item>
                  <Menu.Item
                    leftSection={<FolderOpen size={15} />}
                    disabled={
                      selected.length !== 1 ||
                      readOnly ||
                      selectedEntries[0]?.kind !== Types.FileManagerEntryKind.File
                    }
                    onClick={() =>
                      runOperation({
                        type: "ExtractArchive",
                        params: {
                          path: selected[0],
                          destination: joinPath(
                            path,
                            fileName(selected[0])
                              .replace(/\.tar\.gz$/i, "")
                              .replace(/\.(zip|tar|7z|rar)$/i, "") || "extracted",
                          ),
                        },
                      })
                    }
                  >
                    Extract here
                  </Menu.Item>
                  <Menu.Divider />
                  <Menu.Item
                    color="red"
                    leftSection={<ICONS.Delete size={15} />}
                    disabled={!canChangeSelection}
                    onClick={() =>
                      runOperation({ type: "Delete", params: { paths: selected } })
                    }
                  >
                    Delete
                  </Menu.Item>
                </Menu.Dropdown>
              </Menu>
              <Divider orientation="vertical" />
              <ToolbarButton
                label={journal.data?.undo_description ?? "Undo"}
                icon={<Undo2 size={17} />}
                disabled={readOnly || !journal.data?.can_undo || busy}
                onClick={async () => {
                  reportUpdateFailure(await undo({ target, confirmed: true }));
                  await refresh();
                }}
              />
              <ToolbarButton
                label={journal.data?.redo_description ?? "Redo"}
                icon={<Redo2 size={17} />}
                disabled={readOnly || !journal.data?.can_redo || busy}
                onClick={async () => {
                  reportUpdateFailure(await redo({ target, confirmed: true }));
                  await refresh();
                }}
              />
            </Group>
          </Group>

          {clipboard && (
            <Text size="xs" c="dimmed">
              {clipboard.paths.length} item{clipboard.paths.length === 1 ? "" : "s"}{" "}
              ready to {clipboard.mode === "copy" ? "copy" : "move"}.
            </Text>
          )}
          {transfer && (
            <Stack gap={4}>
              <Group justify="space-between">
                <Text size="sm">{transfer.label}</Text>
                {transfer.cancel && (
                  <Button size="compact-xs" variant="subtle" onClick={transfer.cancel}>
                    Cancel
                  </Button>
                )}
              </Group>
              <Progress value={transfer.percent ?? 100} animated={transfer.percent == null} />
            </Stack>
          )}

          <Box
            ref={explorerRef}
            tabIndex={0}
            onKeyDown={onKeyboard}
            onDragOver={(event) => event.preventDefault()}
            onDrop={onDrop}
            style={{ outline: "none" }}
          >
            <Group align="stretch" gap="sm" wrap="nowrap">
              {desktop && (
                <ScrollArea w={250} h={520} className="bordered-light" p="xs">
                  <DirectoryTree
                    target={target}
                    currentPath={path}
                    onSelect={setPath}
                  />
                </ScrollArea>
              )}
              <ScrollArea h={520} style={{ flex: 1 }} className="bordered-light">
                {directory.isPending ? (
                  <Group justify="center" p="xl">
                    <Loader />
                  </Group>
                ) : directory.isError ? (
                  <Alert color="red" title="Unable to read directory" m="md">
                    The path could not be listed. It may have changed or no longer be
                    accessible.
                  </Alert>
                ) : (
                  <Table highlightOnHover stickyHeader verticalSpacing="xs">
                    <Table.Thead>
                      <Table.Tr>
                        <Table.Th w={42}>
                          <Checkbox
                            aria-label="Select all entries"
                            checked={entries.length > 0 && selected.length === entries.length}
                            indeterminate={
                              selected.length > 0 && selected.length < entries.length
                            }
                            onChange={(event) =>
                              setSelected(
                                event.currentTarget.checked
                                  ? entries.map((entry) => entry.path)
                                  : [],
                              )
                            }
                          />
                        </Table.Th>
                        {sortHeader("Name", "name")}
                        {sortHeader("Size", "size")}
                        {sortHeader("Modified", "modified_at")}
                      </Table.Tr>
                    </Table.Thead>
                    <Table.Tbody>
                      {entries.map((entry) => (
                        <Table.Tr
                          key={entry.path}
                          bg={selected.includes(entry.path) ? "accent.1" : undefined}
                          draggable={!readOnly && !entry.managed}
                          onDragStart={(event) =>
                            event.dataTransfer.setData(
                              "application/x-komodo-file-paths",
                              JSON.stringify(
                                selected.includes(entry.path) ? selected : [entry.path],
                              ),
                            )
                          }
                          onDragOver={(event) => {
                            if (entry.kind === Types.FileManagerEntryKind.Directory)
                              event.preventDefault();
                          }}
                          onDrop={(event) => {
                            if (entry.kind === Types.FileManagerEntryKind.Directory)
                              void onDrop(event, entry.path);
                          }}
                          onClick={(event) => selectEntry(entry, event)}
                          onDoubleClick={() => openEntry(entry)}
                          style={{ cursor: "default" }}
                        >
                          <Table.Td>
                            <Checkbox
                              aria-label={`Select ${entry.name}`}
                              checked={selected.includes(entry.path)}
                              onChange={() =>
                                setSelected((current) =>
                                  current.includes(entry.path)
                                    ? current.filter((item) => item !== entry.path)
                                    : [...current, entry.path],
                                )
                              }
                              onClick={(event) => event.stopPropagation()}
                            />
                          </Table.Td>
                          <Table.Td>
                            <Group gap="xs" wrap="nowrap">
                              {entry.kind === Types.FileManagerEntryKind.Directory ? (
                                <Folder size={18} />
                              ) : entry.kind === Types.FileManagerEntryKind.File ? (
                                <File size={18} />
                              ) : (
                                <Badge size="xs" color="gray">
                                  {entry.kind}
                                </Badge>
                              )}
                              <Text size="sm" ff="monospace" truncate>
                                {entry.name}
                              </Text>
                              {entry.managed && (
                                <Badge size="xs" variant="light">
                                  managed
                                </Badge>
                              )}
                            </Group>
                          </Table.Td>
                          <Table.Td>
                            <Text size="sm" c="dimmed">
                              {entry.kind === Types.FileManagerEntryKind.Directory
                                ? "—"
                                : formatBytes(entry.size)}
                            </Text>
                          </Table.Td>
                          <Table.Td>
                            <Text size="sm" c="dimmed">
                              {new Date(entry.modified_at).toLocaleString()}
                            </Text>
                          </Table.Td>
                        </Table.Tr>
                      ))}
                      {entries.length === 0 && (
                        <Table.Tr>
                          <Table.Td colSpan={4}>
                            <Text ta="center" c="dimmed" py="xl">
                              This directory is empty. Drop files here to upload them.
                            </Text>
                          </Table.Td>
                        </Table.Tr>
                      )}
                    </Table.Tbody>
                  </Table>
                )}
              </ScrollArea>
            </Group>
          </Box>
        </Stack>
      )}

      <input
        ref={inputRef}
        type="file"
        multiple
        hidden
        onChange={(event) => void uploadFiles(Array.from(event.target.files ?? []))}
      />

      <Modal
        opened={!!editorPath}
        onClose={() => setEditorPath(undefined)}
        title={editorPath ? fileName(editorPath) : "File editor"}
        size="xl"
      >
        {textFile.isPending ? (
          <Loader />
        ) : textFile.data ? (
          <Stack>
            <Text size="xs" c="dimmed" ff="monospace">
              {textFile.data.path}
            </Text>
            <MonacoEditor
              value={draft}
              onValueChange={setDraft}
              filename={textFile.data.path}
              language={languageFromPath(textFile.data.path)}
              readOnly={readOnly}
              maxHeightProportion={0.65}
            />
            {!readOnly && (
              <Group justify="end">
                <Button
                  leftSection={<ICONS.Save size={16} />}
                  loading={busy}
                  disabled={draft === textFile.data.contents}
                  onClick={async () => {
                    await runOperation({
                      type: "WriteText",
                      params: {
                        path: textFile.data.path,
                        contents: draft,
                        expected_revision: textFile.data.revision,
                      },
                    });
                  }}
                >
                  Save
                </Button>
              </Group>
            )}
          </Stack>
        ) : (
          <Alert color="red">This file cannot be opened as editable text.</Alert>
        )}
      </Modal>

      <Modal
        opened={!!action}
        onClose={() => setAction(null)}
        title={
          action === "create-file"
            ? "Create file"
            : action === "create-directory"
              ? "Create folder"
              : action === "rename"
                ? "Rename item"
                : "Create archive"
        }
      >
        <Stack>
          <TextInput
            autoFocus
            label={action === "archive" ? "Archive name" : "Name"}
            value={actionValue}
            onChange={(event) => setActionValue(event.currentTarget.value)}
            onKeyDown={(event) => {
              if (event.key === "Enter") void submitAction();
            }}
          />
          {action === "archive" && (
            <Select
              label="Format"
              value={archiveFormat}
              onChange={(value) =>
                setArchiveFormat(value as Types.FileManagerArchiveFormat)
              }
              data={[
                { value: Types.FileManagerArchiveFormat.Zip, label: "ZIP" },
                { value: Types.FileManagerArchiveFormat.Tar, label: "TAR" },
                { value: Types.FileManagerArchiveFormat.TarGz, label: "TAR.GZ" },
                { value: Types.FileManagerArchiveFormat.SevenZip, label: "7z" },
              ]}
            />
          )}
          <Group justify="end">
            <Button variant="default" onClick={() => setAction(null)}>
              Cancel
            </Button>
            <Button
              leftSection={<Check size={16} />}
              loading={busy}
              disabled={!actionValue.trim()}
              onClick={() => void submitAction()}
            >
              Continue
            </Button>
          </Group>
        </Stack>
      </Modal>

      <Modal
        opened={!!pendingCommit}
        onClose={() => setPendingCommit(undefined)}
        title="Confirm file operation"
        size="lg"
      >
        <Stack>
          <Alert color="yellow" title="Review required">
            This operation can replace or remove existing data. Review every conflict
            before continuing.
          </Alert>
          {pendingCommit?.preflight.conflicts.map((conflict) => (
            <Group key={conflict.path} justify="space-between" wrap="nowrap">
              <Stack gap={0} style={{ minWidth: 0 }}>
                <Text ff="monospace" size="sm" truncate>
                  {conflict.path}
                </Text>
                <Text size="xs" c="dimmed">
                  Existing {conflict.existing_kind}; incoming {conflict.incoming_kind}
                </Text>
              </Stack>
              <Select
                w={130}
                value={decisions[conflict.path]}
                onChange={(value) =>
                  setDecisions((current) => ({
                    ...current,
                    [conflict.path]: value as Types.FileManagerConflictAction,
                  }))
                }
                data={[
                  {
                    value: Types.FileManagerConflictAction.Overwrite,
                    label: "Overwrite",
                  },
                  { value: Types.FileManagerConflictAction.Skip, label: "Skip" },
                ]}
              />
            </Group>
          ))}
          <Group justify="end">
            <Button variant="default" onClick={() => setPendingCommit(undefined)}>
              Cancel
            </Button>
            <Button
              color="red"
              loading={commitPending}
              onClick={() =>
                pendingCommit &&
                void completeCommit(
                  pendingCommit,
                  pendingCommit.preflight.conflicts.map((conflict) => ({
                    path: conflict.path,
                    action:
                      decisions[conflict.path] ??
                      Types.FileManagerConflictAction.Skip,
                  })),
                )
              }
            >
              Confirm operation
            </Button>
          </Group>
        </Stack>
      </Modal>
    </Section>
  );
}

function ToolbarButton({
  label,
  icon,
  disabled,
  onClick,
}: {
  label: string;
  icon: ReactNode;
  disabled?: boolean;
  onClick: () => void;
}) {
  return (
    <Tooltip label={label} openDelay={400}>
      <ActionIcon
        variant="subtle"
        aria-label={label}
        disabled={disabled}
        onClick={onClick}
      >
        {icon}
      </ActionIcon>
    </Tooltip>
  );
}

function DirectoryTree({
  target,
  currentPath,
  onSelect,
}: {
  target: Types.FileManagerTarget;
  currentPath: string;
  onSelect: (path: string) => void;
}) {
  const [expanded, setExpanded] = useState(true);
  return (
    <Stack gap={2}>
      <Button
        variant={currentPath === "" ? "light" : "subtle"}
        justify="start"
        size="compact-sm"
        leftSection={
          <ActionIcon
            component="span"
            variant="transparent"
            size="xs"
            onClick={(event) => {
              event.stopPropagation();
              setExpanded((value) => !value);
            }}
          >
            {expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
          </ActionIcon>
        }
        onClick={() => onSelect("")}
      >
        Root
      </Button>
      {expanded && (
        <TreeChildren
          target={target}
          path=""
          currentPath={currentPath}
          onSelect={onSelect}
          depth={1}
        />
      )}
    </Stack>
  );
}

function TreeChildren({
  target,
  path,
  currentPath,
  onSelect,
  depth,
}: {
  target: Types.FileManagerTarget;
  path: string;
  currentPath: string;
  onSelect: (path: string) => void;
  depth: number;
}) {
  const directory = useRead("ListFileManagerDirectory", { target, path });
  return (
    <Stack gap={2}>
      {directory.data?.entries
        .filter((entry) => entry.kind === Types.FileManagerEntryKind.Directory)
        .sort((left, right) => left.name.localeCompare(right.name))
        .map((entry) => (
          <TreeDirectory
            key={entry.path}
            target={target}
            entry={entry}
            currentPath={currentPath}
            onSelect={onSelect}
            depth={depth}
          />
        ))}
    </Stack>
  );
}

function TreeDirectory({
  target,
  entry,
  currentPath,
  onSelect,
  depth,
}: {
  target: Types.FileManagerTarget;
  entry: Types.FileManagerEntry;
  currentPath: string;
  onSelect: (path: string) => void;
  depth: number;
}) {
  const [expanded, setExpanded] = useState(currentPath.startsWith(`${entry.path}/`));
  return (
    <>
      <Button
        variant={currentPath === entry.path ? "light" : "subtle"}
        justify="start"
        size="compact-sm"
        pl={`calc(${depth} * 0.65rem)`}
        leftSection={
          <ActionIcon
            component="span"
            variant="transparent"
            size="xs"
            onClick={(event) => {
              event.stopPropagation();
              setExpanded((value) => !value);
            }}
          >
            {expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
          </ActionIcon>
        }
        onClick={() => onSelect(entry.path)}
      >
        <Text size="sm" truncate>
          {entry.name}
        </Text>
      </Button>
      {expanded && (
        <TreeChildren
          target={target}
          path={entry.path}
          currentPath={currentPath}
          onSelect={onSelect}
          depth={depth + 1}
        />
      )}
    </>
  );
}
