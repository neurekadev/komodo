import { useInvalidate, useRead, useWrite } from "@/lib/hooks";
import { ICONS } from "@/lib/icons";
import ResourceSelector from "@/resources/selector";
import {
  Alert,
  Button,
  Checkbox,
  Collapse,
  Code,
  Group,
  Modal,
  Pagination,
  ScrollArea,
  Select,
  Stack,
  Text,
  Textarea,
  TextInput,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { Types } from "komodo_client";
import { Section } from "mogh_ui";
import { ChevronDown, ChevronRight, File, Folder } from "lucide-react";
import { ReactNode, useMemo, useState } from "react";

type ResourceBackupProps = {
  target: Types.BackupTarget;
  sourceServerId: string;
  titleOther?: ReactNode;
  canExecute: boolean;
};

const isBelow = (path: string, parent: string) =>
  path !== parent && path.startsWith(parent.endsWith("/") ? parent : `${parent}/`);

const targetLabel = (target: Types.BackupTarget) => {
  if (target.type === "Stack") return "Stack";
  if (target.type === "Volume") return "Volume";
  return target.type;
};

export default function ResourceBackups({
  target,
  sourceServerId,
  titleOther,
  canExecute,
}: ResourceBackupProps) {
  const invalidate = useInvalidate();
  const [snapshot, setSnapshot] = useState<string>();
  const [restoreOpen, setRestoreOpen] = useState(false);
  const [selectedPaths, setSelectedPaths] = useState<string[]>([]);
  const [destinationServerId, setDestinationServerId] =
    useState(sourceServerId);
  const [recoveredName, setRecoveredName] = useState("");
  const [destinationVolume, setDestinationVolume] = useState("");
  const [confirmExistingVolume, setConfirmExistingVolume] = useState(false);
  const [mappingText, setMappingText] = useState("");
  const [plan, setPlan] = useState<Types.BackupRestorePlan>();

  const snapshots = useRead(
    "ListBackupSnapshots",
    { target, page: 0, limit: 100 },
    { refetchInterval: 30_000 },
  );
  const { mutate: runBackup, isPending: backupPending } = useWrite(
    "RunBackup",
    {
      onSuccess: (run) => {
        notifications.show({
          color: run.state === Types.BackupRunState.Failed ? "red" : "green",
          message: run.message,
        });
        invalidate(["ListBackupSnapshots", { target, page: 0, limit: 100 }]);
      },
    },
  );
  const { mutate: createPlan, isPending: planPending } = useWrite(
    "PlanBackupRestore",
    { onSuccess: setPlan },
  );
  const { mutate: executeRestore, isPending: restorePending } = useWrite(
    "ExecuteBackupRestore",
    {
      onSuccess: (run) => {
        setPlan(undefined);
        setRestoreOpen(false);
        notifications.show({
          color: run.state === Types.BackupRunState.Complete ? "green" : "red",
          message: run.message,
        });
      },
    },
  );

  const options =
    snapshots.data?.snapshots.map((item) => ({
      value: item.name,
      label: `${new Date(item.created_at).toLocaleString()}${item.partial ? " (partial)" : ""}`,
      disabled: item.partial,
    })) ?? [];
  const selectedSnapshot = snapshots.data?.snapshots.find(
    (item) => item.name === snapshot,
  );
  const requiredStackMappings =
    selectedSnapshot?.source_paths.filter(
      (path) => !path.includes("komodo-backup-manifest-"),
    ) ?? [];

  const crossNode = destinationServerId !== sourceServerId;
  const bindings = useMemo(() => {
    const pairs = mappingText
      .split("\n")
      .map((line) => line.split("=").map((part) => part.trim()))
      .filter((pair) => pair.length === 2 && pair[0] && pair[1]);
    return Object.fromEntries(pairs as [string, string][]);
  }, [mappingText]);

  return (
    <Section
      title={`${targetLabel(target)} backups`}
      icon={<ICONS.Backup size="1.3rem" />}
      titleOther={titleOther}
    >
      <Stack>
        <Text c="dimmed" size="sm">
          Snapshots are always loaded from the active primary Vykar repository.
          Partial snapshots stay visible for diagnosis but cannot be restored.
        </Text>
        <Group align="end">
          <Select
            label="Snapshot"
            placeholder="Choose a complete snapshot"
            data={options}
            value={snapshot}
            onChange={(value) => setSnapshot(value ?? undefined)}
            searchable
            miw={300}
          />
          {canExecute && (
            <>
              <Button
                leftSection={<ICONS.Backup size="1rem" />}
                loading={backupPending}
                onClick={() => runBackup({ target })}
              >
                Back up now
              </Button>
              <Button
                variant="light"
                leftSection={<ICONS.Restart size="1rem" />}
                disabled={!snapshot}
                onClick={() => {
                  setSelectedPaths([]);
                  setDestinationServerId(sourceServerId);
                  setRestoreOpen(true);
                }}
              >
                Restore
              </Button>
            </>
          )}
        </Group>
        {!snapshots.isPending && !options.length && (
          <Text c="dimmed">No snapshots are available for this resource.</Text>
        )}
      </Stack>

      <Modal
        opened={restoreOpen}
        onClose={() => setRestoreOpen(false)}
        title={`Restore ${targetLabel(target)}`}
        size="xl"
      >
        <Stack>
          <Alert color="orange" icon={<ICONS.Alert size="1rem" />}>
            Restore always stops affected running containers. They restart only
            after a verified publish or a proven rollback. See the{" "}
            <a
              href="https://komodo.docs.neureka.dev/docs/backups#restore-a-stack-or-volume"
              target="_blank"
              rel="noreferrer"
            >
              recovery guide
            </a>
            .
          </Alert>
          <ResourceSelector
            type="Server"
            selected={destinationServerId}
            onSelect={setDestinationServerId}
            clearable={false}
            wrapperProps={{ label: "Restore destination" }}
          />
          {crossNode && target.type === "Stack" && (
            <>
              <TextInput
                label="New recovered Stack name"
                description="Cross-node recovery creates a new Stack and never retargets the original."
                value={recoveredName}
                onChange={(event) => setRecoveredName(event.currentTarget.value)}
                required
              />
              <Textarea
                label="Bind-path mappings"
                description="One absolute source=destination mapping per line."
                placeholder="/old/data=/srv/recovered/data"
                value={mappingText}
                onChange={(event) => setMappingText(event.currentTarget.value)}
                autosize
                minRows={3}
              />
              <Stack gap={4}>
                <Text size="xs" c="dimmed">
                  Map every source root; the first path is the recovered Stack
                  run directory.
                </Text>
                {requiredStackMappings.map((path) => (
                  <Code key={path}>{path}</Code>
                ))}
              </Stack>
            </>
          )}
          {crossNode && target.type === "Volume" && (
            <>
              <TextInput
                label="Destination volume name"
                description="A new local volume is created unless you explicitly confirm an existing one."
                value={destinationVolume}
                onChange={(event) =>
                  setDestinationVolume(event.currentTarget.value)
                }
                required
              />
              <Checkbox
                label="I explicitly allow restoring into an existing local volume"
                checked={confirmExistingVolume}
                onChange={(event) =>
                  setConfirmExistingVolume(event.currentTarget.checked)
                }
              />
            </>
          )}
          {snapshot && (
            <SnapshotPicker
              snapshot={snapshot}
              selected={selectedPaths}
              onChange={setSelectedPaths}
            />
          )}
          <Text size="sm" c="dimmed">
            Select nothing to restore the full snapshot. Selecting folders or
            files performs an exact restore of only those subtrees.
          </Text>
          <Group justify="end">
            <Button variant="default" onClick={() => setRestoreOpen(false)}>
              Cancel
            </Button>
            <Button
              color="orange"
              loading={planPending}
              disabled={
                !snapshot ||
                !destinationServerId ||
                (crossNode && target.type === "Stack" && !recoveredName) ||
                (crossNode &&
                  target.type === "Stack" &&
                  requiredStackMappings.some((path) => !bindings[path])) ||
                (crossNode && target.type === "Volume" && !destinationVolume)
              }
              onClick={() =>
                snapshot &&
                createPlan({
                  snapshot,
                  destination_server_id: destinationServerId,
                  selected_paths: selectedPaths,
                  recovered_stack_name:
                    crossNode && target.type === "Stack"
                      ? recoveredName
                      : undefined,
                  bind_path_mappings: bindings,
                  destination_volume_name:
                    crossNode && target.type === "Volume"
                      ? destinationVolume
                      : undefined,
                  confirm_existing_volume: confirmExistingVolume,
                })
              }
            >
              Review changes
            </Button>
          </Group>
        </Stack>
      </Modal>

      <Modal
        opened={!!plan}
        onClose={() => setPlan(undefined)}
        title="Confirm exact restore"
        size="lg"
      >
        {plan && (
          <Stack>
            <PreflightList label="Create" paths={plan.created_paths} />
            <PreflightList label="Overwrite" paths={plan.overwritten_paths} />
            <PreflightList label="Delete" paths={plan.deleted_paths} />
            <PreflightList
              label="Stop containers"
              paths={plan.containers_to_stop}
            />
            <Alert color="red">
              This plan expires at {new Date(plan.expires_at).toLocaleString()}.
              Komodo stages and verifies data before publishing it with a
              persisted rollback journal.
            </Alert>
            <Group justify="end">
              <Button variant="default" onClick={() => setPlan(undefined)}>
                Cancel
              </Button>
              <Button
                color="red"
                loading={restorePending}
                onClick={() => executeRestore({ plan_id: plan.id })}
              >
                Confirm restore
              </Button>
            </Group>
          </Stack>
        )}
      </Modal>
    </Section>
  );
}

function PreflightList({ label, paths }: { label: string; paths: string[] }) {
  return (
    <Stack gap={4}>
      <Text fw={600}>{label} ({paths.length})</Text>
      {paths.length ? paths.map((path) => <Code key={path}>{path}</Code>) : <Text c="dimmed">None</Text>}
    </Stack>
  );
}

function SnapshotPicker({
  snapshot,
  selected,
  onChange,
}: {
  snapshot: string;
  selected: string[];
  onChange: (paths: string[]) => void;
}) {
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(0);
  const directory = useRead("ListBackupSnapshotDirectory", {
    snapshot,
    parent: "",
    search,
    page,
    limit: 100,
  });
  const pages = Math.max(1, Math.ceil((directory.data?.total ?? 0) / 100));
  return (
    <Stack gap="xs">
      <Text fw={600}>Files and folders</Text>
      <TextInput
        placeholder="Search snapshot paths"
        leftSection={<ICONS.Search size="1rem" />}
        value={search}
        onChange={(event) => {
          setSearch(event.currentTarget.value);
          setPage(0);
        }}
      />
      <ScrollArea h={320} className="bordered-light" p="xs">
        <Stack gap={2}>
          {directory.data?.entries.map((entry) => (
            <SnapshotEntry
              key={entry.path}
              snapshot={snapshot}
              entry={entry}
              selected={selected}
              onChange={onChange}
              depth={0}
            />
          ))}
          {!directory.isPending && !directory.data?.entries.length && (
            <Text c="dimmed">No matching paths.</Text>
          )}
        </Stack>
      </ScrollArea>
      {pages > 1 && (
        <Pagination value={page + 1} total={pages} onChange={(p) => setPage(p - 1)} />
      )}
    </Stack>
  );
}

function SnapshotEntry({
  snapshot,
  entry,
  selected,
  onChange,
  depth,
}: {
  snapshot: string;
  entry: Types.BackupSnapshotItem;
  selected: string[];
  onChange: (paths: string[]) => void;
  depth: number;
}) {
  const [open, setOpen] = useState(false);
  const [page, setPage] = useState(0);
  const children = useRead(
    "ListBackupSnapshotDirectory",
    { snapshot, parent: entry.path, search: "", page, limit: 100 },
    { enabled: open && entry.directory },
  );
  const exact = selected.includes(entry.path);
  const ancestor = selected.some((path) => isBelow(entry.path, path));
  const descendants = selected.some((path) => isBelow(path, entry.path));
  const checked = exact || ancestor;
  const toggle = () => {
    if (exact) {
      onChange(selected.filter((path) => path !== entry.path && !isBelow(path, entry.path)));
    } else if (ancestor) {
      // A parent selection represents the whole subtree. Remove that covering
      // selection before allowing finer-grained child choices.
      onChange(selected.filter((path) => !isBelow(entry.path, path)));
    } else {
      onChange([
        ...selected.filter((path) => !isBelow(path, entry.path)),
        entry.path,
      ]);
    }
  };
  return (
    <Stack gap={2}>
      <Group gap="xs" wrap="nowrap" pl={depth * 18}>
        {entry.directory && entry.has_children ? (
          <Button
            variant="subtle"
            size="compact-xs"
            px={2}
            onClick={() => setOpen((value) => !value)}
          >
            {open ? <ChevronDown size="1rem" /> : <ChevronRight size="1rem" />}
          </Button>
        ) : (
          <span style={{ width: 24 }} />
        )}
        <Checkbox
          checked={checked}
          indeterminate={!checked && descendants}
          onChange={toggle}
          aria-label={`Select ${entry.path}`}
        />
        {entry.directory ? <Folder size="1rem" /> : <File size="1rem" />}
        <Text size="sm" className="text-ellipsis">{entry.name}</Text>
      </Group>
      {entry.directory && (
        <Collapse expanded={open}>
          <Stack gap={2}>
            {children.data?.entries.map((child) => (
              <SnapshotEntry
                key={child.path}
                snapshot={snapshot}
                entry={child}
                selected={selected}
                onChange={onChange}
                depth={depth + 1}
              />
            ))}
            {open && !children.isPending && !children.data?.entries.length && (
              <Text size="sm" c="dimmed" pl={(depth + 1) * 18 + 24}>
                Empty folder
              </Text>
            )}
            {(children.data?.total ?? 0) > 100 && (
              <Pagination
                size="xs"
                value={page + 1}
                total={Math.ceil((children.data?.total ?? 0) / 100)}
                onChange={(next) => setPage(next - 1)}
                ml={(depth + 1) * 18 + 24}
              />
            )}
          </Stack>
        </Collapse>
      )}
    </Stack>
  );
}
