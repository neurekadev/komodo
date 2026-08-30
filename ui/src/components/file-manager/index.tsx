import { KOMODO_BASE_URL } from "@/main";
import { useRead, useWrite } from "@/lib/hooks";
import { ICONS } from "@/lib/icons";
import {
  fileManagerTargetKey,
  useFileOperations,
} from "@/components/file-manager/operations";
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
  ArrowDownAZ,
  ArrowUpAZ,
  Check,
  ChevronDown,
  ChevronRight,
  Clipboard,
  Copy,
  EllipsisVertical,
  File as FileIcon,
  FileArchive,
  FileCode,
  FileCog,
  FileImage,
  FileMusic,
  FileSpreadsheet,
  FileSymlink,
  FileTerminal,
  FileText,
  FileType,
  FileVideoCamera,
  Folder,
  FolderOpen,
  FolderPlus,
  FolderUp,
  FilePlus,
  Pencil,
  Redo2,
  Scissors,
  Undo2,
  Upload,
} from "lucide-react";
import type { LucideIcon } from "lucide-react";
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
  notificationId: string;
  label: string;
};
type FileVisual = {
  icon: LucideIcon;
  color: string;
  opacity?: number;
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

const ARCHIVE_EXTENSIONS = [
  ".tar.gz",
  ".tar.bz2",
  ".tar.xz",
  ".tar.zst",
  ".zip",
  ".tar",
  ".tgz",
  ".7z",
  ".rar",
  ".gz",
  ".bz2",
  ".xz",
  ".zst",
];
const IMAGE_EXTENSIONS = [
  ".png",
  ".jpg",
  ".jpeg",
  ".gif",
  ".webp",
  ".svg",
  ".bmp",
  ".ico",
  ".tif",
  ".tiff",
  ".avif",
  ".heic",
];
const VIDEO_EXTENSIONS = [".mp4", ".mkv", ".mov", ".avi", ".webm", ".m4v"];
const AUDIO_EXTENSIONS = [
  ".mp3",
  ".wav",
  ".flac",
  ".ogg",
  ".m4a",
  ".aac",
  ".opus",
];
const SPREADSHEET_EXTENSIONS = [".csv", ".tsv", ".xls", ".xlsx", ".ods"];
const DOCUMENT_EXTENSIONS = [".pdf", ".doc", ".docx", ".odt", ".rtf"];
const SCRIPT_EXTENSIONS = [
  ".sh",
  ".bash",
  ".zsh",
  ".fish",
  ".ps1",
  ".bat",
  ".cmd",
];
const CONFIG_EXTENSIONS = [
  ".yaml",
  ".yml",
  ".toml",
  ".ini",
  ".conf",
  ".cfg",
  ".properties",
  ".xml",
  ".json",
];
const CODE_EXTENSIONS = [
  ".c",
  ".h",
  ".cc",
  ".cpp",
  ".hpp",
  ".cs",
  ".fs",
  ".fsx",
  ".go",
  ".rs",
  ".py",
  ".rb",
  ".java",
  ".kt",
  ".kts",
  ".php",
  ".swift",
  ".dart",
  ".lua",
  ".ex",
  ".exs",
  ".erl",
  ".clj",
  ".cljs",
  ".scala",
  ".sql",
  ".html",
  ".htm",
  ".css",
  ".scss",
  ".sass",
  ".less",
  ".js",
  ".jsx",
  ".mjs",
  ".cjs",
  ".ts",
  ".tsx",
  ".vue",
  ".svelte",
  ".astro",
];
const TEXT_EXTENSIONS = [
  ".txt",
  ".md",
  ".mdx",
  ".rst",
  ".adoc",
  ".log",
  ".nfo",
  ".text",
];
const TEXT_BASENAMES = ["readme", "license", "changelog", "notice"];
const CONFIG_BASENAMES = [
  "dockerfile",
  "containerfile",
  "makefile",
  "cmakelists.txt",
  ".dockerignore",
  ".editorconfig",
  ".gitattributes",
  ".gitignore",
  ".npmrc",
  ".yarnrc",
];

const hasExtension = (name: string, extensions: string[]) =>
  extensions.some((extension) => name.endsWith(extension));

const fileVisual = (entry: Types.FileManagerEntry): FileVisual => {
  if (entry.kind === Types.FileManagerEntryKind.Directory) {
    return {
      icon: Folder,
      color: "var(--mantine-color-yellow-text)",
      opacity: 0.9,
    };
  }
  if (entry.kind === Types.FileManagerEntryKind.Symlink) {
    return { icon: FileSymlink, color: "var(--mantine-color-gray-text)" };
  }
  if (entry.kind !== Types.FileManagerEntryKind.File) {
    return { icon: FileIcon, color: "var(--mantine-color-dimmed)" };
  }

  const name = entry.name.toLowerCase();
  if (hasExtension(name, ARCHIVE_EXTENSIONS))
    return { icon: FileArchive, color: "var(--mantine-color-green-text)" };
  if (hasExtension(name, IMAGE_EXTENSIONS))
    return { icon: FileImage, color: "var(--mantine-color-grape-text)" };
  if (hasExtension(name, VIDEO_EXTENSIONS))
    return { icon: FileVideoCamera, color: "var(--mantine-color-red-text)" };
  if (hasExtension(name, AUDIO_EXTENSIONS))
    return { icon: FileMusic, color: "var(--mantine-color-pink-text)" };
  if (hasExtension(name, SPREADSHEET_EXTENSIONS))
    return { icon: FileSpreadsheet, color: "var(--mantine-color-teal-text)" };
  if (hasExtension(name, DOCUMENT_EXTENSIONS))
    return { icon: FileType, color: "var(--mantine-color-indigo-text)" };
  if (hasExtension(name, SCRIPT_EXTENSIONS))
    return { icon: FileTerminal, color: "var(--mantine-color-cyan-text)" };
  if (
    CONFIG_BASENAMES.includes(name) ||
    name === ".env" ||
    name.startsWith(".env.") ||
    hasExtension(name, CONFIG_EXTENSIONS)
  )
    return { icon: FileCog, color: "var(--mantine-color-orange-text)" };
  if (hasExtension(name, CODE_EXTENSIONS))
    return { icon: FileCode, color: "var(--mantine-color-blue-text)" };
  if (
    TEXT_BASENAMES.includes(name) ||
    TEXT_BASENAMES.some((basename) => name.startsWith(`${basename}.`)) ||
    TEXT_BASENAMES.some((basename) => name.startsWith(`${basename}-`)) ||
    hasExtension(name, TEXT_EXTENSIONS)
  )
    return {
      icon: FileText,
      color: "var(--mantine-color-text)",
      opacity: 1,
    };
  return { icon: FileIcon, color: "var(--mantine-color-dimmed)" };
};

function EntryIcon({
  entry,
  size = 18,
}: {
  entry: Types.FileManagerEntry;
  size?: number;
}) {
  const visual = fileVisual(entry);
  const Icon = visual.icon;
  return (
    <Icon size={size} color={visual.color} opacity={visual.opacity ?? 0.85} />
  );
}

const archiveExtension = (format: Types.FileManagerArchiveFormat) => {
  switch (format) {
    case Types.FileManagerArchiveFormat.Tar:
      return ".tar";
    case Types.FileManagerArchiveFormat.TarGz:
      return ".tar.gz";
    case Types.FileManagerArchiveFormat.SevenZip:
      return ".7z";
    default:
      return ".zip";
  }
};

const ensureArchiveExtension = (
  name: string,
  format: Types.FileManagerArchiveFormat,
) => {
  const extension = archiveExtension(format);
  return name.toLowerCase().endsWith(extension) ? name : `${name}${extension}`;
};

const archiveBaseName = (path: string) => {
  const name = fileName(path);
  const lower = name.toLowerCase();
  const extension = ARCHIVE_EXTENSIONS.find((candidate) =>
    lower.endsWith(candidate),
  );
  return extension ? name.slice(0, -extension.length) || "extracted" : name;
};

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
  return !!element?.closest(
    "input, textarea, [contenteditable=true], .monaco-editor",
  );
};

const operationLabel = (operation: Types.FileManagerOperation) => {
  switch (operation.type) {
    case "CreateFile":
      return "Create file";
    case "CreateDirectory":
      return "Create folder";
    case "Rename":
      return "Rename";
    case "Move":
      return "Move";
    case "Copy":
      return "Copy";
    case "Delete":
      return "Delete";
    case "WriteText":
      return "Save text file";
    case "CreateArchive":
      return "Create archive";
    case "ExtractArchive":
      return "Extract archive";
  }
};

export default function FileManager({
  target,
  titleOther,
}: {
  target: Types.FileManagerTarget;
  titleOther?: ReactNode;
}) {
  const queryClient = useQueryClient();
  const operations = useFileOperations();
  const desktop = useMediaQuery("(min-width: 62em)");
  const inputRef = useRef<HTMLInputElement>(null);
  const uploadDestinationRef = useRef<string | undefined>(undefined);
  const explorerRef = useRef<HTMLDivElement>(null);
  const [path, setPath] = useState("");
  const [selected, setSelected] = useState<string[]>([]);
  const [selectionAnchor, setSelectionAnchor] = useState<string>();
  const [clipboard, setClipboard] = useState<ClipboardState>();
  const [sortKey, setSortKey] = useState<SortKey>("name");
  const [sortAscending, setSortAscending] = useState(true);
  const [editorPath, setEditorPath] = useState<string>();
  const [draft, setDraft] = useState("");
  const editorSource = useRef<{ path: string; contents: string } | undefined>(
    undefined,
  );
  const [discardEditorOpen, setDiscardEditorOpen] = useState(false);
  const [action, setAction] = useState<ActionDialog>(null);
  const [actionValue, setActionValue] = useState("");
  const [actionPaths, setActionPaths] = useState<string[]>([]);
  const [actionDestination, setActionDestination] = useState("");
  const [archiveFormat, setArchiveFormat] =
    useState<Types.FileManagerArchiveFormat>(
      Types.FileManagerArchiveFormat.Zip,
    );
  const [pendingCommit, setPendingCommit] = useState<PendingCommit>();
  const [decisions, setDecisions] = useState<
    Record<string, Types.FileManagerConflictAction>
  >({});

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
    { onError: () => undefined },
  );
  const { mutateAsync: commit, isPending: commitPending } = useWrite(
    "CommitFileManagerOperation",
    {
      onError: () => undefined,
    },
  );
  const { mutateAsync: prepareUpload } = useWrite("PrepareFileManagerUpload", {
    onError: () => undefined,
  });
  const { mutateAsync: prepareDownload } = useWrite(
    "PrepareFileManagerDownload",
    {
      onError: () => undefined,
    },
  );
  const { mutateAsync: undo, isPending: undoPending } = useWrite(
    "UndoFileManagerOperation",
    {
      onError: () => undefined,
    },
  );
  const { mutateAsync: redo, isPending: redoPending } = useWrite(
    "RedoFileManagerOperation",
    {
      onError: () => undefined,
    },
  );

  const readOnly = capabilities?.read_only ?? true;
  const busy =
    preflightPending ||
    commitPending ||
    undoPending ||
    redoPending ||
    operations.isWriteActive(target);

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

  const editorDirty = !!textFile.data && draft !== textFile.data.contents;

  const refresh = useCallback(
    async (forceEditor = false) => {
      const matchesTarget = (queryKey: readonly unknown[]) => {
        const params = queryKey[1];
        return (
          !!params &&
          typeof params === "object" &&
          "target" in params &&
          fileManagerTargetKey(
            (params as { target: Types.FileManagerTarget }).target,
          ) === fileManagerTargetKey(target)
        );
      };
      await Promise.all([
        queryClient.invalidateQueries({
          predicate: ({ queryKey }) =>
            queryKey[0] === "ListFileManagerDirectory" &&
            matchesTarget(queryKey),
        }),
        queryClient.invalidateQueries({
          queryKey: ["GetFileManagerJournalStatus", { target }],
        }),
        editorPath && (!editorDirty || forceEditor)
          ? queryClient.invalidateQueries({
              queryKey: ["ReadFileManagerText", { target, path: editorPath }],
            })
          : Promise.resolve(),
      ]);
    },
    [editorDirty, editorPath, queryClient, target],
  );

  useEffect(() => {
    if (!editorPath) {
      editorSource.current = undefined;
      return;
    }
    if (!textFile.data || textFile.data.path !== editorPath) return;
    const previous = editorSource.current;
    setDraft((current) =>
      previous?.path !== textFile.data.path || current === previous.contents
        ? textFile.data.contents
        : current,
    );
    editorSource.current = {
      path: textFile.data.path,
      contents: textFile.data.contents,
    };
  }, [editorPath, textFile.data]);

  useEffect(() => {
    setSelected([]);
    setSelectionAnchor(undefined);
  }, [path]);

  const requestEditorClose = useCallback(() => {
    if (editorDirty) setDiscardEditorOpen(true);
    else setEditorPath(undefined);
  }, [editorDirty]);

  useEffect(() => {
    if (!editorDirty) return;
    const warn = (event: BeforeUnloadEvent) => event.preventDefault();
    window.addEventListener("beforeunload", warn);
    return () => window.removeEventListener("beforeunload", warn);
  }, [editorDirty]);

  const completeCommit = useCallback(
    async (
      plan: PendingCommit,
      conflictDecisions: Types.FileManagerConflictDecision[] = [],
    ) => {
      setPendingCommit(undefined);
      setDecisions({});
      operations.submitting(plan.notificationId, plan.label);
      try {
        const ticket = await commit({
          target,
          plan_id: plan.preflight.plan_id,
          decisions: conflictDecisions,
          confirmed: true,
        });
        const status = await operations.track(
          ticket.operation_id,
          target,
          plan.label,
          true,
          plan.notificationId,
        );
        if (status.state !== Types.FileManagerOperationState.Complete) return;
        await refresh(plan.operation.type === "WriteText");
        if (plan.clearClipboardOnSuccess) setClipboard(undefined);
        setSelected([]);
      } catch (error) {
        operations.failPending(plan.notificationId, plan.label, error);
      }
    },
    [commit, operations, refresh, target],
  );

  const runOperation = useCallback(
    async (
      operation: Types.FileManagerOperation,
      options: { clearClipboardOnSuccess?: boolean } = {},
    ) => {
      const label = operationLabel(operation);
      const notificationId = operations.begin(label);
      try {
        const result = await preflight({ target, operation });
        const plan = {
          operation,
          preflight: result,
          notificationId,
          label,
          ...options,
        };
        if (result.confirmation_required || result.conflicts.length > 0) {
          operations.waiting(notificationId, label);
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
      } catch (error) {
        operations.failPending(notificationId, label, error);
      }
    },
    [completeCommit, operations, preflight, target],
  );

  const runHistoryOperation = useCallback(
    async (kind: "undo" | "redo") => {
      const label =
        kind === "undo" ? "Undo file operation" : "Redo file operation";
      const notificationId = operations.begin(label);
      try {
        const ticket =
          kind === "undo"
            ? await undo({ target, confirmed: true })
            : await redo({ target, confirmed: true });
        const status = await operations.track(
          ticket.operation_id,
          target,
          label,
          true,
          notificationId,
        );
        if (status.state === Types.FileManagerOperationState.Complete) {
          await refresh();
          setSelected([]);
        }
      } catch (error) {
        operations.failPending(notificationId, label, error);
      }
    },
    [operations, redo, refresh, target, undo],
  );

  const cancelPendingCommit = useCallback(() => {
    if (pendingCommit) {
      operations.cancelPending(
        pendingCommit.notificationId,
        pendingCommit.label,
      );
    }
    setPendingCommit(undefined);
    setDecisions({});
  }, [operations, pendingCommit]);

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

  const selectedPathSet = useMemo(() => new Set(selected), [selected]);
  const selectedEntries = entries.filter((entry) =>
    selectedPathSet.has(entry.path),
  );
  const selectionContainsManaged = selectedEntries.some(
    (entry) => entry.managed,
  );
  const canChangeSelection =
    !readOnly && selected.length > 0 && !selectionContainsManaged;
  const canRenameSelection = canChangeSelection && selected.length === 1;
  const canArchiveSelection = canChangeSelection;
  const canExtractSelection =
    canChangeSelection &&
    selected.length === 1 &&
    selectedEntries[0]?.kind === Types.FileManagerEntryKind.File;
  const hasMoreAction =
    canRenameSelection ||
    canArchiveSelection ||
    canExtractSelection ||
    canChangeSelection;

  const paste = useCallback(
    async (destination = path) => {
      if (!clipboard || readOnly) return;
      const paths = clipboard.paths.filter(
        (source) =>
          source !== destination &&
          !destination.startsWith(`${source}/`) &&
          (clipboard.mode === "copy" || parentPath(source) !== destination),
      );
      if (!paths.length) return;
      await runOperation(
        {
          type: clipboard.mode === "copy" ? "Copy" : "Move",
          params: { paths, destination },
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
        await runHistoryOperation("undo");
      }
    } else if (
      modifier &&
      (event.key.toLowerCase() === "y" ||
        (event.shiftKey && event.key.toLowerCase() === "z"))
    ) {
      event.preventDefault();
      if (journal.data?.can_redo && !readOnly) {
        await runHistoryOperation("redo");
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
      if (editorPath) requestEditorClose();
      else setSelected([]);
    }
  };

  const openAction = (
    next: Exclude<ActionDialog, null>,
    options: { paths?: string[]; destination?: string } = {},
  ) => {
    const paths = options.paths ?? selected;
    setActionPaths(paths);
    setActionDestination(options.destination ?? path);
    setActionValue(next === "rename" ? fileName(paths[0] ?? "") : "");
    setAction(next);
  };

  const submitAction = async () => {
    const value = actionValue.trim();
    if (!value || !action) return;
    if (action === "create-file") {
      await runOperation({
        type: "CreateFile",
        params: { path: joinPath(actionDestination, value) },
      });
    } else if (action === "create-directory") {
      await runOperation({
        type: "CreateDirectory",
        params: { path: joinPath(actionDestination, value) },
      });
    } else if (action === "rename") {
      await runOperation({
        type: "Rename",
        params: { path: actionPaths[0], new_name: value },
      });
    } else {
      await runOperation({
        type: "CreateArchive",
        params: {
          paths: actionPaths,
          destination: joinPath(
            actionDestination,
            ensureArchiveExtension(value, archiveFormat),
          ),
          format: archiveFormat,
        },
      });
    }
    setAction(null);
  };

  const uploadFiles = async (files: File[], destination = path) => {
    if (!files.length || readOnly) return;
    let uploadedCount = 0;
    for (const file of files) {
      let operationId: string | undefined;
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
      const label = `Upload ${file.name}`;
      const notificationId = operations.begin(label);
      try {
        const ticket = await prepareUpload({
          target,
          destination,
          file_names: [file.name],
          total_bytes: file.size,
          overwrite: collision,
          confirmed: collision,
          expected_revision: collisionEntry?.revision,
        });
        operationId = ticket.operation_id;
        const statusPromise = operations.track(
          ticket.operation_id,
          target,
          label,
          true,
          notificationId,
        );
        await new Promise<void>((resolve, reject) => {
          const request = new XMLHttpRequest();
          request.open("POST", KOMODO_BASE_URL + ticket.url);
          const jwt = MoghAuth.LOGIN_TOKENS.jwt();
          if (jwt) request.setRequestHeader("authorization", jwt);
          request.withCredentials = true;
          operations.setCancel(notificationId, () => request.abort());
          request.onload = () =>
            request.status >= 200 && request.status < 300
              ? resolve()
              : reject(new Error(request.responseText || "Upload failed"));
          request.onerror = () => reject(new Error("Upload connection failed"));
          request.onabort = () =>
            reject(new DOMException("Upload cancelled", "AbortError"));
          request.send(file);
        });
        const status = await statusPromise;
        if (status.state === Types.FileManagerOperationState.Complete) {
          uploadedCount += 1;
        }
      } catch (error) {
        if (operationId) operations.untrack(operationId);
        if ((error as DOMException)?.name === "AbortError") {
          operations.cancelPending(notificationId, label);
        } else {
          operations.failPending(notificationId, label, error);
        }
      } finally {
        operations.setCancel(notificationId);
      }
    }
    if (uploadedCount > 0) {
      await refresh();
    }
    if (inputRef.current) inputRef.current.value = "";
  };

  const openUpload = (destination = path) => {
    uploadDestinationRef.current = destination;
    inputRef.current?.click();
  };

  const download = async (paths = selected) => {
    if (!paths.length) return;
    const controller = new AbortController();
    const label =
      paths.length === 1 ? `Download ${fileName(paths[0])}` : "Download files";
    const notificationId = operations.begin(label);
    let operationId: string | undefined;
    try {
      const ticket = await prepareDownload({ target, paths });
      operationId = ticket.operation_id;
      const statusPromise = operations.track(
        ticket.operation_id,
        target,
        label,
        false,
        notificationId,
      );
      operations.setCancel(notificationId, () => controller.abort());
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
        (paths.length === 1 ? fileName(paths[0]) : "komodo-files.zip");
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
      await statusPromise;
    } catch (error) {
      if (operationId) operations.untrack(operationId);
      if ((error as DOMException)?.name === "AbortError")
        operations.cancelPending(notificationId, label);
      else operations.failPending(notificationId, label, error);
    } finally {
      operations.setCancel(notificationId);
    }
  };

  const onDrop = async (event: React.DragEvent, destination = path) => {
    event.preventDefault();
    event.stopPropagation();
    const droppedFiles = Array.from(event.dataTransfer.files);
    if (droppedFiles.length) {
      await uploadFiles(droppedFiles, destination);
      return;
    }
    let droppedPaths: string[] = [];
    try {
      const parsed = JSON.parse(
        event.dataTransfer.getData("application/x-komodo-file-paths") || "[]",
      );
      if (Array.isArray(parsed)) {
        droppedPaths = parsed.filter(
          (candidate): candidate is string => typeof candidate === "string",
        );
      }
    } catch {
      return;
    }
    if (!droppedPaths.length || readOnly) return;
    const movablePaths = [...new Set(droppedPaths)].filter(
      (source) =>
        source !== destination &&
        !destination.startsWith(`${source}/`) &&
        parentPath(source) !== destination,
    );
    if (!movablePaths.length) return;
    await runOperation({
      type: "Move",
      params: { paths: movablePaths, destination },
    });
  };

  const ignoreDrop = (event: React.DragEvent) => {
    event.preventDefault();
    event.stopPropagation();
    event.dataTransfer.dropEffect = "none";
  };

  const extractArchive = (archivePath: string, destinationDirectory: string) =>
    runOperation({
      type: "ExtractArchive",
      params: {
        path: archivePath,
        destination: joinPath(
          destinationDirectory,
          archiveBaseName(archivePath),
        ),
      },
    });

  const contextMenuItems = ({
    entry,
    paths,
    destination,
    archiveDestination,
    containsManaged = !!entry?.managed,
    showOpen = true,
  }: {
    entry?: Types.FileManagerEntry;
    paths: string[];
    destination?: string;
    archiveDestination: string;
    containsManaged?: boolean;
    showOpen?: boolean;
  }) => {
    const canMutate =
      !readOnly && paths.length > 0 && !containsManaged && !busy;
    const canOpen =
      paths.length <= 1 &&
      (!entry ||
        entry.kind === Types.FileManagerEntryKind.Directory ||
        entry.kind === Types.FileManagerEntryKind.File);

    return (
      <>
        {showOpen && (
          <Menu.Item
            leftSection={<FolderOpen size={15} />}
            disabled={!canOpen}
            onClick={() =>
              entry ? openEntry(entry) : setPath(destination ?? "")
            }
          >
            Open
          </Menu.Item>
        )}
        {paths.length > 0 && (
          <Menu.Item
            leftSection={<ICONS.Download size={15} />}
            disabled={!!containsManaged || busy}
            onClick={() => void download(paths)}
          >
            Download
          </Menu.Item>
        )}
        {destination !== undefined && (
          <>
            <Menu.Sub>
              <Menu.Sub.Target>
                <Menu.Sub.Item
                  leftSection={<FolderPlus size={15} />}
                  disabled={readOnly || busy}
                >
                  New
                </Menu.Sub.Item>
              </Menu.Sub.Target>
              <Menu.Sub.Dropdown>
                <Menu.Item
                  leftSection={<FilePlus size={15} />}
                  onClick={() => openAction("create-file", { destination })}
                >
                  File
                </Menu.Item>
                <Menu.Item
                  leftSection={<FolderPlus size={15} />}
                  onClick={() =>
                    openAction("create-directory", { destination })
                  }
                >
                  Folder
                </Menu.Item>
              </Menu.Sub.Dropdown>
            </Menu.Sub>
            <Menu.Item
              leftSection={<Upload size={15} />}
              disabled={readOnly || busy}
              onClick={() => openUpload(destination)}
            >
              Upload
            </Menu.Item>
            <Menu.Item
              leftSection={<Clipboard size={15} />}
              disabled={!clipboard || readOnly || busy}
              onClick={() => void paste(destination)}
            >
              Paste
            </Menu.Item>
          </>
        )}
        {paths.length > 0 && (
          <>
            <Menu.Divider />
            <Menu.Item
              leftSection={<Scissors size={15} />}
              disabled={!canMutate}
              onClick={() => setClipboard({ mode: "move", paths })}
            >
              Cut
            </Menu.Item>
            <Menu.Item
              leftSection={<Copy size={15} />}
              disabled={!canMutate}
              onClick={() => setClipboard({ mode: "copy", paths })}
            >
              Copy
            </Menu.Item>
            <Menu.Item
              leftSection={<Pencil size={15} />}
              disabled={!canMutate || paths.length !== 1}
              onClick={() => openAction("rename", { paths })}
            >
              Rename
            </Menu.Item>
            <Menu.Sub>
              <Menu.Sub.Target>
                <Menu.Sub.Item
                  leftSection={<FileArchive size={15} />}
                  disabled={!canMutate}
                >
                  Archive
                </Menu.Sub.Item>
              </Menu.Sub.Target>
              <Menu.Sub.Dropdown>
                <Menu.Item
                  leftSection={<FileArchive size={15} />}
                  onClick={() =>
                    openAction("archive", {
                      paths,
                      destination: archiveDestination,
                    })
                  }
                >
                  Create archive
                </Menu.Item>
                <Menu.Item
                  leftSection={<FolderOpen size={15} />}
                  disabled={
                    paths.length !== 1 ||
                    entry?.kind !== Types.FileManagerEntryKind.File
                  }
                  onClick={() =>
                    void extractArchive(paths[0], archiveDestination)
                  }
                >
                  Extract here
                </Menu.Item>
              </Menu.Sub.Dropdown>
            </Menu.Sub>
            <Menu.Divider />
            <Menu.Item
              color="red"
              leftSection={<ICONS.Delete size={15} />}
              disabled={!canMutate}
              onClick={() =>
                void runOperation({ type: "Delete", params: { paths } })
              }
            >
              Delete
            </Menu.Item>
          </>
        )}
      </>
    );
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
          {capabilities.reason ??
            "This target cannot expose a filesystem root."}
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
              <Button
                variant="subtle"
                size="compact-sm"
                onClick={() => setPath("")}
              >
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
                icon={<FilePlus size={17} />}
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
                onClick={() => openUpload()}
              />
              <ToolbarButton
                label="Download"
                icon={<ICONS.Download size={17} />}
                disabled={!selected.length || selectionContainsManaged || busy}
                onClick={() => void download()}
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
                    disabled={busy || !hasMoreAction}
                  >
                    <EllipsisVertical size={17} />
                  </ActionIcon>
                </Menu.Target>
                <Menu.Dropdown>
                  <Menu.Item
                    leftSection={<Pencil size={15} />}
                    disabled={!canRenameSelection}
                    onClick={() => openAction("rename")}
                  >
                    Rename
                  </Menu.Item>
                  <Menu.Item
                    leftSection={<FileArchive size={15} />}
                    disabled={!canArchiveSelection}
                    onClick={() => openAction("archive")}
                  >
                    Create archive
                  </Menu.Item>
                  <Menu.Item
                    leftSection={<FolderOpen size={15} />}
                    disabled={!canExtractSelection}
                    onClick={() => void extractArchive(selected[0], path)}
                  >
                    Extract here
                  </Menu.Item>
                  <Menu.Divider />
                  <Menu.Item
                    color="red"
                    leftSection={<ICONS.Delete size={15} />}
                    disabled={!canChangeSelection}
                    onClick={() =>
                      runOperation({
                        type: "Delete",
                        params: { paths: selected },
                      })
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
                onClick={() => void runHistoryOperation("undo")}
              />
              <ToolbarButton
                label={journal.data?.redo_description ?? "Redo"}
                icon={<Redo2 size={17} />}
                disabled={readOnly || !journal.data?.can_redo || busy}
                onClick={() => void runHistoryOperation("redo")}
              />
            </Group>
          </Group>

          {clipboard && (
            <Text size="xs" c="dimmed">
              {clipboard.paths.length} item
              {clipboard.paths.length === 1 ? "" : "s"} ready to{" "}
              {clipboard.mode === "copy" ? "copy" : "move"}.
            </Text>
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
                    onDrop={(event, destination) =>
                      void onDrop(event, destination)
                    }
                    ignoreDrop={ignoreDrop}
                    backgroundMenu={contextMenuItems({
                      paths: [],
                      destination: path,
                      archiveDestination: path,
                      showOpen: false,
                    })}
                    contextMenu={(entry) =>
                      contextMenuItems({
                        entry,
                        paths: entry ? [entry.path] : [],
                        destination: entry?.path ?? "",
                        archiveDestination: entry ? parentPath(entry.path) : "",
                      })
                    }
                  />
                </ScrollArea>
              )}
              <ScrollArea
                h={520}
                style={{ flex: 1 }}
                className="bordered-light"
              >
                {directory.isPending ? (
                  <Group justify="center" p="xl">
                    <Loader />
                  </Group>
                ) : directory.isError ? (
                  <Alert color="red" title="Unable to read directory" m="md">
                    The path could not be listed. It may have changed or no
                    longer be accessible.
                  </Alert>
                ) : (
                  <Table highlightOnHover stickyHeader verticalSpacing="xs">
                    <Table.Thead>
                      <Table.Tr>
                        <Table.Th w={42}>
                          <Checkbox
                            aria-label="Select all entries"
                            checked={
                              entries.length > 0 &&
                              selected.length === entries.length
                            }
                            indeterminate={
                              selected.length > 0 &&
                              selected.length < entries.length
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
                      {path && (
                        <Table.Tr
                          onClick={() => setPath(parentPath(path))}
                          style={{ cursor: "pointer" }}
                        >
                          <Table.Td />
                          <Table.Td>
                            <Group gap="xs" wrap="nowrap">
                              <FolderUp
                                size={18}
                                color="var(--mantine-color-yellow-text)"
                                opacity={0.9}
                              />
                              <Text size="sm" ff="monospace">
                                ..
                              </Text>
                            </Group>
                          </Table.Td>
                          <Table.Td>
                            <Text size="sm" c="dimmed">
                              —
                            </Text>
                          </Table.Td>
                          <Table.Td>
                            <Text size="sm" c="dimmed">
                              —
                            </Text>
                          </Table.Td>
                        </Table.Tr>
                      )}
                      {entries.map((entry) => {
                        const entryIsSelected = selectedPathSet.has(entry.path);
                        const contextPaths = entryIsSelected
                          ? selected
                          : [entry.path];
                        return (
                          <Menu key={entry.path} shadow="md">
                            <Menu.ContextMenu>
                              <Table.Tr
                                bg={entryIsSelected ? "accent.1" : undefined}
                                draggable={!readOnly && !entry.managed}
                                onDragStart={(event) =>
                                  event.dataTransfer.setData(
                                    "application/x-komodo-file-paths",
                                    JSON.stringify(
                                      entryIsSelected ? selected : [entry.path],
                                    ),
                                  )
                                }
                                onDragOver={(event) => {
                                  event.stopPropagation();
                                  if (
                                    entry.kind ===
                                    Types.FileManagerEntryKind.Directory
                                  ) {
                                    event.preventDefault();
                                    event.dataTransfer.dropEffect = "move";
                                  }
                                }}
                                onDrop={(event) => {
                                  if (
                                    entry.kind ===
                                    Types.FileManagerEntryKind.Directory
                                  ) {
                                    void onDrop(event, entry.path);
                                  } else {
                                    ignoreDrop(event);
                                  }
                                }}
                                onClick={(event) => selectEntry(entry, event)}
                                onContextMenu={() => {
                                  if (!entryIsSelected) {
                                    setSelected([entry.path]);
                                    setSelectionAnchor(entry.path);
                                  }
                                }}
                                onDoubleClick={() => openEntry(entry)}
                                style={{ cursor: "default" }}
                              >
                                <Table.Td>
                                  <Checkbox
                                    aria-label={`Select ${entry.name}`}
                                    checked={entryIsSelected}
                                    onChange={() =>
                                      setSelected((current) =>
                                        current.includes(entry.path)
                                          ? current.filter(
                                              (item) => item !== entry.path,
                                            )
                                          : [...current, entry.path],
                                      )
                                    }
                                    onClick={(event) => event.stopPropagation()}
                                  />
                                </Table.Td>
                                <Table.Td>
                                  <Group gap="xs" wrap="nowrap">
                                    <EntryIcon entry={entry} />
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
                                    {entry.kind ===
                                    Types.FileManagerEntryKind.Directory
                                      ? "—"
                                      : formatBytes(entry.size)}
                                  </Text>
                                </Table.Td>
                                <Table.Td>
                                  <Text size="sm" c="dimmed">
                                    {new Date(
                                      entry.modified_at,
                                    ).toLocaleString()}
                                  </Text>
                                </Table.Td>
                              </Table.Tr>
                            </Menu.ContextMenu>
                            <Menu.Dropdown>
                              {contextMenuItems({
                                entry,
                                paths: contextPaths,
                                destination:
                                  entry.kind ===
                                  Types.FileManagerEntryKind.Directory
                                    ? entry.path
                                    : undefined,
                                archiveDestination: path,
                                containsManaged: entryIsSelected
                                  ? selectionContainsManaged
                                  : entry.managed,
                              })}
                            </Menu.Dropdown>
                          </Menu>
                        );
                      })}
                      {entries.length === 0 && (
                        <Table.Tr>
                          <Table.Td colSpan={4}>
                            <Text ta="center" c="dimmed" py="xl">
                              This directory is empty. Drop files here to upload
                              them.
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
        onChange={(event) =>
          void uploadFiles(
            Array.from(event.target.files ?? []),
            uploadDestinationRef.current ?? path,
          )
        }
      />

      <Modal
        opened={!!editorPath}
        onClose={requestEditorClose}
        title={editorPath ? fileName(editorPath) : "File editor"}
        size="min(92vw, 1600px)"
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
          <Alert color="red">
            This file cannot be opened as editable text.
          </Alert>
        )}
      </Modal>

      <Modal
        opened={discardEditorOpen}
        onClose={() => setDiscardEditorOpen(false)}
        title="Discard unsaved changes?"
        size="sm"
        centered
      >
        <Stack>
          <Text size="sm">
            This file has unsaved changes. Closing it will permanently discard
            your draft.
          </Text>
          <Group justify="end">
            <Button
              variant="default"
              onClick={() => setDiscardEditorOpen(false)}
            >
              Keep editing
            </Button>
            <Button
              color="red"
              onClick={() => {
                setDiscardEditorOpen(false);
                setEditorPath(undefined);
              }}
            >
              Discard changes
            </Button>
          </Group>
        </Stack>
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
                {
                  value: Types.FileManagerArchiveFormat.TarGz,
                  label: "TAR.GZ",
                },
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
        onClose={cancelPendingCommit}
        title="Confirm file operation"
        size="lg"
      >
        <Stack>
          <Alert color="yellow" title="Review required">
            This operation can replace or remove existing data. Review every
            conflict before continuing.
          </Alert>
          {pendingCommit?.preflight.conflicts.map((conflict) => (
            <Group key={conflict.path} justify="space-between" wrap="nowrap">
              <Stack gap={0} style={{ minWidth: 0 }}>
                <Text ff="monospace" size="sm" truncate>
                  {conflict.path}
                </Text>
                <Text size="xs" c="dimmed">
                  Existing {conflict.existing_kind}; incoming{" "}
                  {conflict.incoming_kind}
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
                  {
                    value: Types.FileManagerConflictAction.Skip,
                    label: "Skip",
                  },
                ]}
              />
            </Group>
          ))}
          <Group justify="end">
            <Button variant="default" onClick={cancelPendingCommit}>
              Cancel
            </Button>
            <Button
              color="red"
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
  contextMenu,
  backgroundMenu,
  onDrop,
  ignoreDrop,
}: {
  target: Types.FileManagerTarget;
  currentPath: string;
  onSelect: (path: string) => void;
  contextMenu: (entry?: Types.FileManagerEntry) => ReactNode;
  backgroundMenu: ReactNode;
  onDrop: (event: React.DragEvent, destination: string) => void;
  ignoreDrop: (event: React.DragEvent) => void;
}) {
  const [expanded, setExpanded] = useState(true);
  return (
    <Menu shadow="md">
      <Menu.ContextMenu>
        <Stack gap={2} mih={500} onDragOver={ignoreDrop} onDrop={ignoreDrop}>
          <Menu shadow="md">
            <Menu.ContextMenu>
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
                    {expanded ? (
                      <ChevronDown size={14} />
                    ) : (
                      <ChevronRight size={14} />
                    )}
                  </ActionIcon>
                }
                onClick={() => onSelect("")}
                onContextMenu={(event) => event.stopPropagation()}
                onDragOver={(event) => {
                  event.preventDefault();
                  event.stopPropagation();
                  event.dataTransfer.dropEffect = "move";
                }}
                onDrop={(event) => onDrop(event, "")}
              >
                <Group gap="xs" wrap="nowrap">
                  <Folder
                    size={16}
                    color="var(--mantine-color-yellow-text)"
                    opacity={0.9}
                  />
                  <Text size="sm">Root</Text>
                </Group>
              </Button>
            </Menu.ContextMenu>
            <Menu.Dropdown>{contextMenu()}</Menu.Dropdown>
          </Menu>
          {expanded && (
            <TreeChildren
              target={target}
              path=""
              currentPath={currentPath}
              onSelect={onSelect}
              contextMenu={contextMenu}
              onDrop={onDrop}
              ignoreDrop={ignoreDrop}
              depth={1}
            />
          )}
        </Stack>
      </Menu.ContextMenu>
      <Menu.Dropdown>{backgroundMenu}</Menu.Dropdown>
    </Menu>
  );
}

function TreeChildren({
  target,
  path,
  currentPath,
  onSelect,
  contextMenu,
  onDrop,
  ignoreDrop,
  depth,
}: {
  target: Types.FileManagerTarget;
  path: string;
  currentPath: string;
  onSelect: (path: string) => void;
  contextMenu: (entry?: Types.FileManagerEntry) => ReactNode;
  onDrop: (event: React.DragEvent, destination: string) => void;
  ignoreDrop: (event: React.DragEvent) => void;
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
            contextMenu={contextMenu}
            onDrop={onDrop}
            ignoreDrop={ignoreDrop}
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
  contextMenu,
  onDrop,
  ignoreDrop,
  depth,
}: {
  target: Types.FileManagerTarget;
  entry: Types.FileManagerEntry;
  currentPath: string;
  onSelect: (path: string) => void;
  contextMenu: (entry?: Types.FileManagerEntry) => ReactNode;
  onDrop: (event: React.DragEvent, destination: string) => void;
  ignoreDrop: (event: React.DragEvent) => void;
  depth: number;
}) {
  const [expanded, setExpanded] = useState(
    currentPath.startsWith(`${entry.path}/`),
  );
  return (
    <>
      <Menu shadow="md">
        <Menu.ContextMenu>
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
                {expanded ? (
                  <ChevronDown size={14} />
                ) : (
                  <ChevronRight size={14} />
                )}
              </ActionIcon>
            }
            onClick={() => onSelect(entry.path)}
            onContextMenu={(event) => event.stopPropagation()}
            onDragOver={(event) => {
              event.preventDefault();
              event.stopPropagation();
              event.dataTransfer.dropEffect = "move";
            }}
            onDrop={(event) => onDrop(event, entry.path)}
          >
            <Group gap="xs" wrap="nowrap" style={{ minWidth: 0 }}>
              <Folder
                size={16}
                color="var(--mantine-color-yellow-text)"
                opacity={0.9}
              />
              <Text size="sm" truncate>
                {entry.name}
              </Text>
            </Group>
          </Button>
        </Menu.ContextMenu>
        <Menu.Dropdown>{contextMenu(entry)}</Menu.Dropdown>
      </Menu>
      {expanded && (
        <TreeChildren
          target={target}
          path={entry.path}
          currentPath={currentPath}
          onSelect={onSelect}
          contextMenu={contextMenu}
          onDrop={onDrop}
          ignoreDrop={ignoreDrop}
          depth={depth + 1}
        />
      )}
    </>
  );
}
