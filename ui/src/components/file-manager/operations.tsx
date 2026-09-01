import { komodo_client } from "@/lib/hooks";
import { updateLogToText } from "@/lib/utils";
import {
  Button,
  Checkbox,
  Group,
  Modal,
  Progress,
  Stack,
  Text,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { useQueryClient } from "@tanstack/react-query";
import { Types } from "komodo_client";
import { Check, CircleX } from "lucide-react";
import {
  createContext,
  ReactNode,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

const STORAGE_KEY = "komodo-file-manager-operations-v1";
const STATUS_REQUEST_TIMEOUT_MS = 10_000;
const STATUS_TIMEOUT_WARNING_THRESHOLD = 30;
const OPERATION_NOT_FOUND_ERROR = "File Manager operation was not found";

type TrackedOperation = {
  operationId: string;
  notificationId: string;
  target: Types.FileManagerTarget;
  label: string;
  write: boolean;
  startedAt: number;
};

type OperationContextValue = {
  begin: (label: string) => string;
  waiting: (notificationId: string, label: string) => void;
  submitting: (notificationId: string, label: string) => void;
  cancelPending: (notificationId: string, label: string) => void;
  failPending: (notificationId: string, label: string, error: unknown) => void;
  setCancel: (notificationId: string, cancel?: () => void) => void;
  track: (
    operationId: string,
    target: Types.FileManagerTarget,
    label: string,
    write: boolean,
    notificationId?: string,
  ) => Promise<Types.FileManagerOperationStatus>;
  discover: (target: Types.FileManagerTarget) => Promise<void>;
  untrack: (operationId: string) => void;
  isWriteActive: (target: Types.FileManagerTarget) => boolean;
};

const OperationContext = createContext<OperationContextValue | null>(null);

export const fileManagerTargetKey = (target: Types.FileManagerTarget) =>
  JSON.stringify(
    target.type === "Stack"
      ? [target.type, target.params.stack]
      : [target.type, target.params.server, target.params.volume],
  );

const queryTargets = (
  queryKey: readonly unknown[],
  target: Types.FileManagerTarget,
) => {
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

class FileOperationStatusTimeoutError extends Error {
  constructor() {
    super("File operation status request timed out");
    this.name = "FileOperationStatusTimeoutError";
  }
}

const withTimeout = async <T,>(promise: Promise<T>, timeoutMs: number) => {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timeout = setTimeout(
          () => reject(new FileOperationStatusTimeoutError()),
          timeoutMs,
        );
      }),
    ]);
  } finally {
    if (timeout !== undefined) clearTimeout(timeout);
  }
};

const loadOperations = (): TrackedOperation[] => {
  try {
    const value = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? "[]");
    return Array.isArray(value) ? value : [];
  } catch {
    return [];
  }
};

const errorParts = (value: unknown): string[] => {
  if (!value || typeof value !== "object") return [];
  const result = (value as { result?: { error?: unknown; trace?: unknown } })
    .result;
  if (!result) return [];
  return [result.error, ...(Array.isArray(result.trace) ? result.trace : [])]
    .filter((part): part is string => typeof part === "string" && !!part)
    .map(updateLogToText);
};

const apiErrorParts = (error: unknown) => {
  const structured = errorParts(error);
  if (structured.length) return structured;
  if (!(error instanceof Error)) return [];
  try {
    return errorParts({ result: JSON.parse(error.message) });
  } catch {
    return [];
  }
};

const isOperationNotFoundError = (error: unknown) =>
  apiErrorParts(error).some((part) => part.includes(OPERATION_NOT_FOUND_ERROR));

const errorText = (error: unknown) => {
  const structured = apiErrorParts(error);
  if (structured.length) return structured.join(" | ");
  const message = error instanceof Error ? error.message : String(error);
  return updateLogToText(message);
};

const phaseLabel = (phase: Types.FileManagerOperationPhase) =>
  ({
    [Types.FileManagerOperationPhase.Queued]: "Queued",
    [Types.FileManagerOperationPhase.Preparing]: "Preparing",
    [Types.FileManagerOperationPhase.Snapshotting]:
      "Creating recovery snapshot",
    [Types.FileManagerOperationPhase.Applying]: "Applying changes",
    [Types.FileManagerOperationPhase.Verifying]: "Verifying",
    [Types.FileManagerOperationPhase.Transferring]: "Transferring",
    [Types.FileManagerOperationPhase.Finalizing]: "Finalizing",
    [Types.FileManagerOperationPhase.RollingBack]: "Rolling back",
  })[phase] ?? "Working";

const progressValue = (status: Types.FileManagerOperationStatus) => {
  if (status.total_bytes > 0) {
    return Math.min(100, (status.completed_bytes / status.total_bytes) * 100);
  }
  if (status.total_entries > 0) {
    return Math.min(
      100,
      (status.completed_entries / status.total_entries) * 100,
    );
  }
  return undefined;
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

const formatDuration = (milliseconds: number) => {
  const seconds = Math.max(0, Math.floor(milliseconds / 1_000));
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const remainder = seconds % 60;
  if (minutes < 60) return `${minutes}m ${remainder}s`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m`;
};

function StatusMessage({
  status,
  cancel,
}: {
  status: Types.FileManagerOperationStatus;
  cancel?: () => void;
}) {
  const percent = progressValue(status);
  const elapsed = Math.max(1, Date.now() - (status.started_at ?? Date.now()));
  const phaseElapsed = Math.max(
    1,
    Date.now() - (status.phase_started_at ?? status.started_at ?? Date.now()),
  );
  const bytesPerSecond =
    status.completed_bytes > 0
      ? status.completed_bytes / (phaseElapsed / 1_000)
      : undefined;
  const entriesPerSecond =
    status.completed_entries > 0
      ? status.completed_entries / (phaseElapsed / 1_000)
      : undefined;
  const eta =
    status.total_bytes > status.completed_bytes && bytesPerSecond
      ? ((status.total_bytes - status.completed_bytes) / bytesPerSecond) * 1_000
      : status.total_entries > status.completed_entries && entriesPerSecond
        ? ((status.total_entries - status.completed_entries) /
            entriesPerSecond) *
          1_000
        : undefined;
  const detail =
    status.total_bytes > 0
      ? `${formatBytes(status.completed_bytes)} / ${formatBytes(status.total_bytes)}`
      : status.completed_bytes > 0
        ? `${formatBytes(status.completed_bytes)} processed`
        : status.total_entries > 0
          ? `${status.completed_entries} / ${status.total_entries} entries`
          : status.completed_entries > 0
            ? `${status.completed_entries} entries processed`
            : undefined;
  return (
    <Stack gap={5}>
      <Group justify="space-between" gap="md">
        <Text size="sm">
          {phaseLabel(status.phase ?? Types.FileManagerOperationPhase.Queued)}
        </Text>
        {cancel ? (
          <Button size="compact-xs" variant="subtle" onClick={cancel}>
            Cancel
          </Button>
        ) : percent !== undefined ? (
          <Text size="xs" c="dimmed">
            {Math.round(percent)}%
          </Text>
        ) : null}
      </Group>
      <Progress value={percent ?? 100} animated={percent === undefined} />
      {detail && (
        <Text size="xs" c="dimmed">
          {detail} · elapsed {formatDuration(elapsed)}
          {bytesPerSecond
            ? ` · ${formatBytes(bytesPerSecond)}/s`
            : entriesPerSecond
              ? ` · ${entriesPerSecond.toFixed(1)} entries/s`
              : ""}
          {eta ? ` · ETA ${formatDuration(eta)}` : ""}
        </Text>
      )}
      {(status.temporary_storage_bytes ?? 0) > 0 && (
        <Text size="xs" c="dimmed">
          Temporary storage: {formatBytes(status.temporary_storage_bytes ?? 0)}
        </Text>
      )}
    </Stack>
  );
}

export function FileOperationProvider({ children }: { children: ReactNode }) {
  const queryClient = useQueryClient();
  const [operations, setOperations] =
    useState<TrackedOperation[]>(loadOperations);
  const [pendingConflict, setPendingConflict] = useState<{
    operation: TrackedOperation;
    status: Types.FileManagerOperationStatus;
  }>();
  const [applyToAll, setApplyToAll] = useState(false);
  const pendingDecisionId =
    pendingConflict?.status.pending_conflict?.decision_id;
  const operationsRef = useRef(operations);
  const polling = useRef(false);
  const restoredNotifications = useRef(new Set<string>());
  const failures = useRef(new Map<string, number>());
  const timeouts = useRef(new Map<string, number>());
  const cancellations = useRef(new Map<string, () => void>());
  const waiters = useRef(
    new Map<
      string,
      Array<(status: Types.FileManagerOperationStatus) => void>
    >(),
  );

  useEffect(() => {
    operationsRef.current = operations;
    localStorage.setItem(STORAGE_KEY, JSON.stringify(operations));
  }, [operations]);

  useEffect(() => {
    setApplyToAll(false);
  }, [pendingDecisionId]);

  const removeOperation = useCallback((operationId: string) => {
    setOperations((current) =>
      current.filter((operation) => operation.operationId !== operationId),
    );
  }, []);

  const settle = useCallback(
    (operation: TrackedOperation, status: Types.FileManagerOperationStatus) => {
      const complete =
        status.state === Types.FileManagerOperationState.Complete;
      const cancelled =
        status.state === Types.FileManagerOperationState.Cancelled;
      notifications.update({
        id: operation.notificationId,
        title: complete
          ? `${operation.label} complete`
          : cancelled
            ? `${operation.label} cancelled`
            : `${operation.label} failed`,
        message: complete
          ? "The file operation completed successfully."
          : status.error
            ? updateLogToText(status.error)
            : "The file operation did not complete.",
        color: complete ? "green" : cancelled ? "gray" : "red",
        icon: complete ? <Check size={18} /> : <CircleX size={18} />,
        loading: false,
        withCloseButton: true,
        autoClose: complete || cancelled ? 4_000 : false,
      });
      removeOperation(operation.operationId);
      failures.current.delete(operation.operationId);
      timeouts.current.delete(operation.operationId);
      cancellations.current.delete(operation.notificationId);
      setPendingConflict((current) =>
        current?.operation.operationId === operation.operationId
          ? undefined
          : current,
      );
      for (const resolve of waiters.current.get(operation.operationId) ?? []) {
        resolve(status);
      }
      waiters.current.delete(operation.operationId);
      if (complete) {
        void queryClient.invalidateQueries({
          predicate: ({ queryKey }) =>
            (queryKey[0] === "ListFileManagerDirectory" ||
              queryKey[0] === "GetFileManagerJournalStatus") &&
            queryTargets(queryKey, operation.target),
        });
      }
    },
    [queryClient, removeOperation],
  );

  const retireMissingOperation = useCallback(
    (operation: TrackedOperation) => {
      const unavailableStatus: Types.FileManagerOperationStatus = {
        operation_id: operation.operationId,
        state: Types.FileManagerOperationState.Failed,
        description: operation.label,
        started_at: operation.startedAt,
        completed_entries: 0,
        total_entries: 0,
        completed_bytes: 0,
        total_bytes: 0,
        error:
          "The operation is no longer active and its final status is unavailable.",
      };
      notifications.update({
        id: operation.notificationId,
        title: `${operation.label} no longer active`,
        message:
          "The server no longer reports this operation as active. Its final status is unavailable, so local tracking was cleared.",
        color: "gray",
        loading: false,
        withCloseButton: true,
        autoClose: 4_000,
      });
      removeOperation(operation.operationId);
      failures.current.delete(operation.operationId);
      timeouts.current.delete(operation.operationId);
      cancellations.current.delete(operation.notificationId);
      setPendingConflict((current) =>
        current?.operation.operationId === operation.operationId
          ? undefined
          : current,
      );
      for (const resolve of waiters.current.get(operation.operationId) ?? []) {
        resolve(unavailableStatus);
      }
      waiters.current.delete(operation.operationId);
    },
    [removeOperation],
  );

  const poll = useCallback(async () => {
    if (polling.current || operationsRef.current.length === 0) return;
    polling.current = true;
    try {
      await Promise.all(
        operationsRef.current.map(async (operation) => {
          try {
            const status = await withTimeout(
              komodo_client().read("GetFileManagerOperationStatus", {
                target: operation.target,
                operation_id: operation.operationId,
              }),
              STATUS_REQUEST_TIMEOUT_MS,
            );
            failures.current.delete(operation.operationId);
            timeouts.current.delete(operation.operationId);
            if (
              status.state === Types.FileManagerOperationState.Complete ||
              status.state === Types.FileManagerOperationState.Failed ||
              status.state === Types.FileManagerOperationState.Cancelled
            ) {
              settle(operation, status);
              return;
            }
            notifications.update({
              id: operation.notificationId,
              title: operation.label,
              message: (
                <StatusMessage
                  status={status}
                  cancel={
                    cancellations.current.get(operation.notificationId) ??
                    (status.cancellable
                      ? () => {
                          void komodo_client().write(
                            "CancelFileManagerOperation",
                            {
                              target: operation.target,
                              operation_id: operation.operationId,
                            },
                          );
                        }
                      : undefined)
                  }
                />
              ),
              color: "blue",
              loading: true,
              autoClose: false,
              withCloseButton: false,
            });
            if (status.pending_conflict) {
              setPendingConflict({ operation, status });
            } else {
              setPendingConflict((current) =>
                current?.operation.operationId === operation.operationId
                  ? undefined
                  : current,
              );
            }
          } catch (error) {
            if (error instanceof FileOperationStatusTimeoutError) {
              const count =
                (timeouts.current.get(operation.operationId) ?? 0) + 1;
              timeouts.current.set(operation.operationId, count);
              if (count === STATUS_TIMEOUT_WARNING_THRESHOLD) {
                notifications.update({
                  id: operation.notificationId,
                  title: operation.label,
                  message:
                    "Operation status is unavailable. The operation may still be running on the server; retrying…",
                  color: "yellow",
                  loading: true,
                  autoClose: false,
                  withCloseButton: false,
                });
              }
              return;
            }
            if (isOperationNotFoundError(error)) {
              try {
                const active = await withTimeout(
                  komodo_client().read("ListActiveFileManagerOperations", {
                    target: operation.target,
                  }),
                  STATUS_REQUEST_TIMEOUT_MS,
                );
                if (
                  !active.operations.some(
                    (status) => status.operation_id === operation.operationId,
                  )
                ) {
                  retireMissingOperation(operation);
                  return;
                }
              } catch {
                // A failed confirmation is transient; keep polling the operation.
              }
            }
            timeouts.current.delete(operation.operationId);
            const count =
              (failures.current.get(operation.operationId) ?? 0) + 1;
            failures.current.set(operation.operationId, count);
            if (count === 3) {
              notifications.update({
                id: operation.notificationId,
                title: operation.label,
                message:
                  "Operation status is temporarily unavailable; reconnecting without assuming the job failed…",
                color: "yellow",
                loading: true,
                autoClose: false,
                withCloseButton: false,
              });
            }
          }
        }),
      );
    } finally {
      polling.current = false;
    }
  }, [retireMissingOperation, settle]);

  useEffect(() => {
    for (const operation of operations) {
      if (restoredNotifications.current.has(operation.notificationId)) {
        continue;
      }
      restoredNotifications.current.add(operation.notificationId);
      notifications.show({
        id: operation.notificationId,
        title: operation.label,
        message: "Restoring operation status…",
        loading: true,
        autoClose: false,
        withCloseButton: false,
      });
    }
  }, [operations]);

  useEffect(() => {
    void poll();
    const interval = window.setInterval(() => void poll(), 1_000);
    return () => window.clearInterval(interval);
  }, [poll]);

  const begin = useCallback((label: string): string => {
    const notificationId =
      typeof globalThis.crypto?.randomUUID === "function"
        ? globalThis.crypto.randomUUID()
        : `${Date.now()}-${Math.random().toString(36).slice(2)}`;
    notifications.show({
      id: notificationId,
      title: label,
      message: "Preparing…",
      loading: true,
      autoClose: false,
      withCloseButton: false,
    });
    return notificationId;
  }, []);

  const waiting = useCallback((notificationId: string, label: string) => {
    notifications.update({
      id: notificationId,
      title: label,
      message: "Waiting for confirmation…",
      loading: true,
      autoClose: false,
      withCloseButton: false,
    });
  }, []);

  const submitting = useCallback((notificationId: string, label: string) => {
    notifications.update({
      id: notificationId,
      title: label,
      message: "Submitting operation…",
      color: "blue",
      loading: true,
      autoClose: false,
      withCloseButton: false,
    });
  }, []);

  const cancelPending = useCallback((notificationId: string, label: string) => {
    notifications.update({
      id: notificationId,
      title: `${label} cancelled`,
      message: "No files were changed.",
      color: "gray",
      loading: false,
      withCloseButton: true,
      autoClose: 4_000,
    });
  }, []);

  const failPending = useCallback(
    (notificationId: string, label: string, error: unknown) => {
      notifications.update({
        id: notificationId,
        title: `${label} failed`,
        message: errorText(error),
        color: "red",
        loading: false,
        withCloseButton: true,
        autoClose: false,
      });
    },
    [],
  );

  const setCancel = useCallback(
    (notificationId: string, cancel?: () => void) => {
      if (cancel) cancellations.current.set(notificationId, cancel);
      else cancellations.current.delete(notificationId);
    },
    [],
  );

  const track = useCallback(
    (
      operationId: string,
      target: Types.FileManagerTarget,
      label: string,
      write: boolean,
      notificationId = begin(label),
    ) => {
      const operation: TrackedOperation = {
        operationId,
        notificationId,
        target,
        label,
        write,
        startedAt: Date.now(),
      };
      setOperations((current) => [
        ...current.filter((item) => item.operationId !== operationId),
        operation,
      ]);
      return new Promise<Types.FileManagerOperationStatus>((resolve) => {
        waiters.current.set(operationId, [
          ...(waiters.current.get(operationId) ?? []),
          resolve,
        ]);
      });
    },
    [begin],
  );

  const discover = useCallback(async (target: Types.FileManagerTarget) => {
    const response = await komodo_client().read(
      "ListActiveFileManagerOperations",
      { target },
    );
    setOperations((current) => {
      const known = new Set(current.map((item) => item.operationId));
      const restored = response.operations
        .filter((status) => !known.has(status.operation_id))
        .map((status) => ({
          operationId: status.operation_id,
          notificationId: status.operation_id,
          target,
          label: status.description || "File operation",
          write: true,
          startedAt: status.started_at || Date.now(),
        }));
      return restored.length ? [...current, ...restored] : current;
    });
  }, []);

  const untrack = useCallback(
    (operationId: string) => {
      removeOperation(operationId);
      failures.current.delete(operationId);
      timeouts.current.delete(operationId);
      waiters.current.delete(operationId);
    },
    [removeOperation],
  );

  const isWriteActive = useCallback(
    (target: Types.FileManagerTarget) => {
      const key = fileManagerTargetKey(target);
      return operations.some(
        (operation) =>
          operation.write && fileManagerTargetKey(operation.target) === key,
      );
    },
    [operations],
  );

  const value = useMemo(
    () => ({
      begin,
      waiting,
      submitting,
      cancelPending,
      failPending,
      setCancel,
      track,
      discover,
      untrack,
      isWriteActive,
    }),
    [
      begin,
      cancelPending,
      failPending,
      discover,
      isWriteActive,
      setCancel,
      submitting,
      track,
      untrack,
      waiting,
    ],
  );

  return (
    <OperationContext.Provider value={value}>
      {children}
      <Modal
        opened={!!pendingConflict?.status.pending_conflict}
        onClose={() => undefined}
        withCloseButton={false}
        closeOnClickOutside={false}
        closeOnEscape={false}
        title="File conflict"
        centered
      >
        {pendingConflict?.status.pending_conflict && (
          <Stack>
            <Text size="sm">
              A file already exists immediately before publish. Choose what to
              do; existing data has not been changed yet.
            </Text>
            <Stack gap={2}>
              <Text ff="monospace" size="sm">
                {pendingConflict.status.pending_conflict.conflict.path}
              </Text>
              <Text size="xs" c="dimmed">
                Existing{" "}
                {pendingConflict.status.pending_conflict.conflict.existing_kind}
                ; incoming{" "}
                {pendingConflict.status.pending_conflict.conflict.incoming_kind}
              </Text>
            </Stack>
            <Checkbox
              checked={applyToAll}
              onChange={(event) => setApplyToAll(event.currentTarget.checked)}
              label="Apply to all remaining conflicts in this operation"
            />
            <Group justify="space-between">
              <Button
                variant="default"
                onClick={() => {
                  setPendingConflict(undefined);
                  setApplyToAll(false);
                  void komodo_client().write("CancelFileManagerOperation", {
                    target: pendingConflict.operation.target,
                    operation_id: pendingConflict.operation.operationId,
                  });
                }}
              >
                Cancel operation
              </Button>
              <Group>
                {[
                  Types.FileManagerConflictAction.Skip,
                  Types.FileManagerConflictAction.Overwrite,
                ].map((action) => (
                  <Button
                    key={action}
                    color={
                      action === Types.FileManagerConflictAction.Overwrite
                        ? "red"
                        : undefined
                    }
                    variant={
                      action === Types.FileManagerConflictAction.Skip
                        ? "default"
                        : "filled"
                    }
                    onClick={() => {
                      const pending = pendingConflict.status.pending_conflict;
                      if (!pending) return;
                      const applyToAllRemaining = applyToAll;
                      setPendingConflict(undefined);
                      setApplyToAll(false);
                      void komodo_client().write(
                        "ResolveFileManagerOperationConflict",
                        {
                          target: pendingConflict.operation.target,
                          operation_id: pendingConflict.operation.operationId,
                          decision_id: pending.decision_id,
                          action,
                          apply_to_all: applyToAllRemaining,
                        },
                      );
                    }}
                  >
                    {action === Types.FileManagerConflictAction.Skip
                      ? "Skip"
                      : "Overwrite"}
                  </Button>
                ))}
              </Group>
            </Group>
          </Stack>
        )}
      </Modal>
    </OperationContext.Provider>
  );
}

export function useFileOperations() {
  const context = useContext(OperationContext);
  if (!context) {
    throw new Error(
      "useFileOperations must be used inside FileOperationProvider",
    );
  }
  return context;
}
