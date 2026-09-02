import { useInvalidate, useRead, useSetTitle, useUser, useWrite } from "@/lib/hooks";
import { ICONS } from "@/lib/icons";
import {
  Accordion,
  Alert,
  Badge,
  Button,
  Checkbox,
  Divider,
  Grid,
  Group,
  Loader,
  Modal,
  MultiSelect,
  NumberInput,
  Pagination,
  PasswordInput,
  Select,
  SimpleGrid,
  Stack,
  Switch,
  Text,
  TextInput,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { Types } from "komodo_client";
import { Page, Section } from "mogh_ui";
import { useEffect, useState } from "react";

const backendDefaults = (
  type: Types.BackupRepositoryBackend["type"],
): Types.BackupRepositoryBackend => {
  switch (type) {
    case "S3":
      return {
        type,
        params: {
          url: "",
          region: "auto",
          access_key_id: {},
          secret_access_key: {},
          soft_delete: true,
        },
      };
    case "Sftp":
      return {
        type,
        params: {
          url: "",
          private_key: {},
          known_hosts: "",
          timeout_seconds: 30,
        },
      };
    case "Rest":
      return {
        type,
        params: { url: "", access_token: {}, allow_insecure_http: false },
      };
    default:
      return { type: "CoreLocal", params: { path: "/data/backups/vykar" } };
  }
};

export default function Backups() {
  useSetTitle("Backups");
  const admin = useUser().data?.admin ?? false;
  const status = useRead("GetBackupStatus", {}, { refetchInterval: 15_000 });
  const settingsQuery = useRead("GetBackupSettings", {}, { enabled: admin });
  const [settings, setSettings] = useState<Types.BackupSettings>();

  useEffect(() => {
    if (settingsQuery.data) setSettings(structuredClone(settingsQuery.data));
  }, [settingsQuery.data?.updated_at]);

  return (
    <Page
      title="Backups"
      icon={ICONS.Backup}
      description="Schedule and recover encrypted Core, Stack, and Volume backups across every Periphery."
    >
      <Stack gap="xl">
        <Group justify="end">
          <Button
            component="a"
            href="https://komodo.docs.neureka.dev/docs/backups"
            target="_blank"
            variant="subtle"
          >
            Backup and recovery guide
          </Button>
        </Group>
        <StatusSection status={status.data} />
        {admin && <SnapshotInventory />}
        {!admin ? (
          <Alert color="blue">
            Repository setup and fleet scheduling are administrator-only. Use
            the Backups tab on a permitted Stack or Volume to browse snapshots,
            run a backup, or restore it.
          </Alert>
        ) : !settings ? (
          <Loader />
        ) : (
          <BackupSettingsForm settings={settings} onChange={setSettings} />
        )}
      </Stack>
    </Page>
  );
}

function SnapshotInventory() {
  const inventory = useRead(
    "ListBackupSnapshots",
    { page: 0, limit: 100 },
    { refetchInterval: 30_000 },
  );
  return (
    <Section title="Primary snapshots" icon={<ICONS.Backup size="1.3rem" />}>
      <Stack gap="xs">
        <Text size="sm" c="dimmed">
          This inventory is read directly from the active primary repository.
          Resource restore controls appear on each Stack or Volume.
        </Text>
        {inventory.data?.snapshots.slice(0, 20).map((snapshot) => (
          <Group key={snapshot.name} justify="space-between" wrap="nowrap">
            <Stack gap={0} className="overflow-hidden">
              <Text size="sm" fw={500} truncate>
                {snapshot.source_label}
              </Text>
              <Text size="xs" c="dimmed" truncate>
                {snapshot.name} · {snapshot.hostname}
              </Text>
            </Stack>
            <Badge color={snapshot.partial ? "orange" : "green"}>
              {snapshot.partial ? "Partial" : "Complete"}
            </Badge>
          </Group>
        ))}
        {!inventory.isPending && !inventory.data?.snapshots.length && (
          <Text c="dimmed">No snapshots in the primary repository.</Text>
        )}
        {(inventory.data?.total ?? 0) > 20 && (
          <Text size="xs" c="dimmed">
            Showing 20 of {inventory.data?.total} snapshots.
          </Text>
        )}
      </Stack>
    </Section>
  );
}

function StatusSection({ status }: { status?: Types.BackupStatus }) {
  const admin = useUser().data?.admin ?? false;
  const invalidate = useInvalidate();
  const { mutate: cancel, isPending: cancelling } = useWrite(
    "CancelBackupRun",
    {
      onSuccess: () => invalidate(["GetBackupStatus", {}]),
    },
  );
  return (
    <Section title="Fleet status" icon={<ICONS.Stats size="1.3rem" />}>
      <SimpleGrid cols={{ base: 1, sm: 2, lg: 4 }}>
        <StatusItem
          label="Primary"
          value={status?.primary_healthy ? "Healthy" : "Unavailable"}
          color={status?.primary_healthy ? "green" : "red"}
        />
        <StatusItem
          label="Mirror"
          value={
            status?.mirror_healthy == null
              ? "Not configured"
              : status.mirror_healthy
                ? "Healthy"
                : "Unavailable"
          }
          color={
            status?.mirror_healthy == null
              ? "gray"
              : status.mirror_healthy
                ? "green"
                : "red"
          }
        />
        <StatusItem
          label="Mirror lag"
          value={`${status?.mirror_lagging_snapshots ?? 0} snapshots`}
        />
        <StatusItem
          label="Next run"
          value={
            status?.next_run_at
              ? new Date(status.next_run_at).toLocaleString()
              : "Not scheduled"
          }
        />
      </SimpleGrid>
      {status?.critical_alert && (
        <Alert color="red" mt="md" icon={<ICONS.Alert size="1rem" />}>
          {status.critical_alert}
        </Alert>
      )}
      {admin && status?.active_run && (
        <Group mt="md" justify="space-between">
          <Text size="sm">
            Active: {status.active_run.message}
          </Text>
          <Button
            color="red"
            variant="light"
            loading={cancelling}
            onClick={() => cancel({ run_id: status.active_run!.id })}
          >
            Cancel run
          </Button>
        </Group>
      )}
      {!!status?.recent_runs.length && (
        <Stack mt="md" gap="xs">
          <Text fw={600}>Recent runs</Text>
          {status.recent_runs.slice(0, 8).map((run) => (
            <Group key={run.id} justify="space-between">
              <Text size="sm">{run.message}</Text>
              <Badge
                color={
                  run.state === Types.BackupRunState.Complete
                    ? "green"
                    : run.state === Types.BackupRunState.Partial
                      ? "orange"
                      : run.state === Types.BackupRunState.Running
                        ? "blue"
                        : "red"
                }
              >
                {run.state}
              </Badge>
            </Group>
          ))}
        </Stack>
      )}
    </Section>
  );
}

function StatusItem({
  label,
  value,
  color,
}: {
  label: string;
  value: string;
  color?: string;
}) {
  return (
    <Stack gap={2} className="bordered-light" p="md" bdrs="md">
      <Text size="sm" c="dimmed">{label}</Text>
      <Text fw={600} c={color}>{value}</Text>
    </Stack>
  );
}

function BackupSettingsForm({
  settings,
  onChange,
}: {
  settings: Types.BackupSettings;
  onChange: (settings: Types.BackupSettings) => void;
}) {
  const invalidate = useInvalidate();
  const patch = (value: Partial<Types.BackupSettings>) =>
    onChange({ ...settings, ...value });
  const { mutate: save, isPending: saving } = useWrite(
    "UpdateBackupSettings",
    {
      onSuccess: (saved) => {
        onChange(structuredClone(saved));
        invalidate(["GetBackupSettings"], ["GetBackupStatus"]);
        notifications.show({ color: "green", message: "Backup settings saved." });
      },
    },
  );
  const { mutate: initialize, isPending: initializing } = useWrite(
    "InitializeBackupRepositories",
    { onSuccess: (run) => notifications.show({ color: "green", message: run.message }) },
  );
  const { mutate: run, isPending: running } = useWrite("RunBackup", {
    onSuccess: (result) => {
      invalidate(["GetBackupStatus"]);
      notifications.show({
        color: result.state === Types.BackupRunState.Failed ? "red" : "green",
        message: result.message,
      });
    },
  });
  const { mutate: verify, isPending: verifying } = useWrite(
    "VerifyBackupRepository",
    {
      onSuccess: (result) =>
        notifications.show({
          color: result.state === Types.BackupRunState.Failed ? "red" : "green",
          message: result.message,
        }),
    },
  );
  const { mutate: promote, isPending: promoting } = useWrite(
    "PromoteBackupMirror",
    {
      onSuccess: (saved) => {
        onChange(structuredClone(saved));
        invalidate(["GetBackupSettings"], ["GetBackupStatus"]);
        notifications.show({ color: "green", message: "Verified mirror promoted to primary." });
      },
    },
  );

  return (
    <Stack gap="xl">
      <Section title="Simple schedule" icon={<ICONS.Schedule size="1.3rem" />}>
        <Stack>
          <Switch
            label="Enable scheduled backups"
            checked={settings.enabled}
            onChange={(event) => patch({ enabled: event.currentTarget.checked })}
          />
          <Grid>
            <Grid.Col span={{ base: 12, md: 8 }}>
              <TextInput
                label="One shared schedule"
                description="Use plain English or a five-field cron expression."
                value={settings.schedule}
                onChange={(event) => patch({ schedule: event.currentTarget.value })}
              />
            </Grid.Col>
            <Grid.Col span={{ base: 12, md: 4 }}>
              <TextInput
                label="IANA timezone"
                value={settings.timezone}
                onChange={(event) => patch({ timezone: event.currentTarget.value })}
              />
            </Grid.Col>
          </Grid>
          <Switch
            label="Stop affected containers while backing up"
            description="Recommended for application-consistent data. Restore always stops affected containers."
            checked={settings.stop_containers}
            onChange={(event) => patch({ stop_containers: event.currentTarget.checked })}
          />
        </Stack>
      </Section>

      <Section title="What to protect" icon={<ICONS.Backup size="1.3rem" />}>
        <SimpleGrid cols={{ base: 1, md: 3 }}>
          <CategoryEditor
            label="Core"
            description="A consistent, versioned export of the Komodo Core database."
            enabled={settings.core_enabled}
            keep={settings.core_keep_last}
            onEnabled={(core_enabled) => patch({ core_enabled })}
            onKeep={(core_keep_last) => patch({ core_keep_last })}
          />
          <CategoryEditor
            label="Stacks"
            description="Compose run directories and local bind mounts; named volumes are separate."
            enabled={settings.stacks_enabled}
            keep={settings.stack_keep_last}
            onEnabled={(stacks_enabled) => patch({ stacks_enabled })}
            onKeep={(stack_keep_last) => patch({ stack_keep_last })}
          />
          <CategoryEditor
            label="Volumes"
            description="Every eligible local named volume, including unmanaged volumes."
            enabled={settings.volumes_enabled}
            keep={settings.volume_keep_last}
            onEnabled={(volumes_enabled) => patch({ volumes_enabled })}
            onKeep={(volume_keep_last) => patch({ volume_keep_last })}
          />
        </SimpleGrid>
        <Divider my="md" />
        <StackSelectionEditor settings={settings} patch={patch} />
        <VolumeSelectionEditor settings={settings} patch={patch} />
      </Section>

      <Section title="Encrypted repositories" icon={<ICONS.Volume size="1.3rem" />}>
        <Alert color="blue" mb="md">
          The primary is the only snapshot source of truth. A mirror is not
          readable until full verification and explicit promotion.
        </Alert>
        <RepositoryEditor
          label="Primary"
          repository={settings.primary}
          onChange={(primary) => patch({ primary })}
        />
        <Divider my="lg" />
        <Switch
          label="Use a mirror repository"
          checked={!!settings.mirror}
          onChange={(event) =>
            patch({
              mirror: event.currentTarget.checked
                ? {
                    name: "mirror",
                    backend: {
                      type: "CoreLocal",
                      params: { path: "/data/backups/vykar-mirror" },
                    },
                    passphrase: {},
                  }
                : undefined,
            })
          }
        />
        {settings.mirror && (
          <RepositoryEditor
            label="Mirror"
            repository={settings.mirror}
            onChange={(mirror) => patch({ mirror })}
          />
        )}
      </Section>

      <CoreRecoverySection />

      <Accordion variant="contained">
        <Accordion.Item value="advanced">
          <Accordion.Control>Advanced integrity and performance</Accordion.Control>
          <Accordion.Panel>
            <SimpleGrid cols={{ base: 1, md: 2 }}>
              <NumberInput
                label="Concurrent nodes"
                min={1}
                value={settings.advanced.node_concurrency}
                onChange={(value) => patch({ advanced: { ...settings.advanced, node_concurrency: Number(value) } })}
              />
              <NumberInput
                label="Upload MiB/s, per node"
                description="Zero is unlimited."
                min={0}
                step={1}
                allowDecimal={false}
                value={settings.advanced.upload_bytes_per_second / (1024 * 1024)}
                onChange={(value) => patch({ advanced: { ...settings.advanced, upload_bytes_per_second: Number(value) * 1024 * 1024 } })}
              />
              <NumberInput
                label="Client repack cap (bytes)"
                description="Defaults to 5 GiB per S3/SFTP maintenance cycle."
                min={0}
                value={settings.advanced.client_repack_limit_bytes}
                onChange={(value) => patch({ advanced: { ...settings.advanced, client_repack_limit_bytes: Number(value) } })}
              />
              <NumberInput
                label="Compaction threshold (%)"
                min={1}
                max={100}
                value={settings.advanced.compact_threshold_percent}
                onChange={(value) => patch({ advanced: { ...settings.advanced, compact_threshold_percent: Number(value) } })}
              />
              <NumberInput
                label="Full verification interval (days)"
                min={1}
                value={settings.advanced.full_verify_every_days}
                onChange={(value) => patch({ advanced: { ...settings.advanced, full_verify_every_days: Number(value) } })}
              />
              <NumberInput
                label="Repository sample per cycle (%)"
                min={1}
                max={100}
                value={settings.advanced.verify_sample_percent}
                onChange={(value) => patch({ advanced: { ...settings.advanced, verify_sample_percent: Number(value) } })}
              />
            </SimpleGrid>
          </Accordion.Panel>
        </Accordion.Item>
      </Accordion>

      <Group justify="end">
        <Button variant="default" loading={verifying} onClick={() => verify({ mirror: false, full: true })}>
          Verify primary
        </Button>
        {settings.mirror && (
          <>
            <Button variant="default" loading={verifying} onClick={() => verify({ mirror: true, full: true })}>
              Verify mirror
            </Button>
            <Button color="orange" loading={promoting} onClick={() => promote({})}>
              Verify and promote mirror
            </Button>
          </>
        )}
        <Button variant="default" loading={initializing} onClick={() => initialize({})}>
          Initialize repositories
        </Button>
        <Button variant="light" loading={running} onClick={() => run({})}>
          Back up fleet now
        </Button>
        <Button
          variant="light"
          loading={running}
          onClick={() => run({ target: { type: "Core" } })}
        >
          Back up Core now
        </Button>
        <Button leftSection={<ICONS.Save size="1rem" />} loading={saving} onClick={() => save({ settings })}>
          Save settings
        </Button>
      </Group>
    </Stack>
  );
}

function CoreRecoverySection() {
  const [snapshot, setSnapshot] = useState<string>();
  const [snapshotPage, setSnapshotPage] = useState(1);
  const [plan, setPlan] = useState<Types.CoreRecoveryPlan>();
  const snapshotResponse = useRead("ListBackupSnapshots", {
    target: { type: "Core" },
    page: snapshotPage - 1,
    limit: 100,
  }).data;
  const snapshots = snapshotResponse?.snapshots ?? [];
  const snapshotPages = Math.max(
    1,
    Math.ceil((snapshotResponse?.total ?? 0) / 100),
  );
  const { mutate: planRecovery, isPending: planning } = useWrite(
    "PlanCoreRecovery",
    { onSuccess: setPlan },
  );
  const { mutate: executeRecovery, isPending: executing } = useWrite(
    "ExecuteCoreRecovery",
    {
      onSuccess: (run) =>
        notifications.show({ color: "orange", message: run.message }),
    },
  );
  return (
    <Section title="Fresh Core recovery" icon={<ICONS.Restart size="1.3rem" />}>
      <Stack>
        <Text c="dimmed" size="sm">
          Restore a Core snapshot into a separate validation database. Komodo
          verifies its schema, version, and administrator access before it can
          become active; the current database is retained for rollback.
        </Text>
        <Group align="end">
          <Select
            label="Complete Core snapshot"
            value={snapshot}
            onChange={(value) => setSnapshot(value ?? undefined)}
            data={snapshots.map((item) => ({
              value: item.name,
              label: `${new Date(item.created_at).toLocaleString()}${item.partial ? " (partial)" : ""}`,
              disabled: item.partial,
            }))}
            searchable
            miw={320}
          />
          <Button
            color="orange"
            loading={planning}
            disabled={!snapshot}
            onClick={() => snapshot && planRecovery({ snapshot })}
          >
            Restore and validate
          </Button>
        </Group>
        {snapshotPages > 1 && (
          <Pagination
            total={snapshotPages}
            value={snapshotPage}
            onChange={(page) => {
              setSnapshot(undefined);
              setSnapshotPage(page);
            }}
          />
        )}
      </Stack>
      <Modal
        opened={!!plan}
        onClose={() => setPlan(undefined)}
        title="Activate recovered Core database"
        size="lg"
      >
        {plan && (
          <Stack>
            <Alert color="orange">
              Core will restart after activation. Do not run another active
              Core against either database during recovery.
            </Alert>
            <StatusItem label="Snapshot version" value={plan.backup_version} />
            <StatusItem label="Validated database" value={plan.validation_database} />
            <StatusItem label="Retained rollback database" value={plan.current_database} />
            <Group justify="end">
              <Button variant="default" onClick={() => setPlan(undefined)}>
                Cancel
              </Button>
              <Button
                color="red"
                loading={executing}
                onClick={() => executeRecovery({ plan_id: plan.id })}
              >
                Activate and restart Core
              </Button>
            </Group>
          </Stack>
        )}
      </Modal>
    </Section>
  );
}

function CategoryEditor({
  label,
  description,
  enabled,
  keep,
  onEnabled,
  onKeep,
}: {
  label: string;
  description: string;
  enabled: boolean;
  keep: number;
  onEnabled: (value: boolean) => void;
  onKeep: (value: number) => void;
}) {
  return (
    <Stack className="bordered-light" p="md" bdrs="md">
      <Switch label={label} checked={enabled} onChange={(event) => onEnabled(event.currentTarget.checked)} />
      <Text size="sm" c="dimmed">{description}</Text>
      <NumberInput label="Complete backups to keep" min={1} value={keep} onChange={(value) => onKeep(Number(value))} />
    </Stack>
  );
}

function StackSelectionEditor({
  settings,
  patch,
}: {
  settings: Types.BackupSettings;
  patch: (value: Partial<Types.BackupSettings>) => void;
}) {
  const stacks = useRead("ListStacks", { query: {}, limit: 500 }).data ?? [];
  return (
    <Grid align="end">
      <Grid.Col span={{ base: 12, md: 3 }}>
        <Select
          label="Stack selection"
          value={settings.stack_selection.mode ?? Types.BackupSelectionMode.All}
          data={Object.values(Types.BackupSelectionMode)}
          onChange={(mode) => patch({ stack_selection: { ...settings.stack_selection, mode: mode as Types.BackupSelectionMode } })}
        />
      </Grid.Col>
      <Grid.Col span={{ base: 12, md: 9 }}>
        <MultiSelect
          label="Selected Stacks"
          disabled={(settings.stack_selection.mode ?? Types.BackupSelectionMode.All) === Types.BackupSelectionMode.All}
          data={stacks.map((stack) => ({ value: stack.id, label: stack.name }))}
          value={settings.stack_selection.stack_ids ?? []}
          onChange={(stack_ids) => patch({ stack_selection: { ...settings.stack_selection, stack_ids } })}
          searchable
          clearable
        />
      </Grid.Col>
    </Grid>
  );
}

function VolumeSelectionEditor({
  settings,
  patch,
}: {
  settings: Types.BackupSettings;
  patch: (value: Partial<Types.BackupSettings>) => void;
}) {
  const servers = useRead("ListServers", { query: {}, limit: 500 }).data ?? [];
  const mode = settings.volume_selection.mode ?? Types.BackupSelectionMode.All;
  const selected = settings.volume_selection.volumes ?? [];
  const toggle = (target: Types.BackupVolumeTarget) => {
    const exists = selected.some((item) => item.server_id === target.server_id && item.volume_name === target.volume_name);
    patch({ volume_selection: { ...settings.volume_selection, volumes: exists ? selected.filter((item) => item.server_id !== target.server_id || item.volume_name !== target.volume_name) : [...selected, target] } });
  };
  return (
    <Stack mt="md">
      <Select
        label="Volume selection"
        value={mode}
        data={Object.values(Types.BackupSelectionMode)}
        onChange={(value) => patch({ volume_selection: { ...settings.volume_selection, mode: value as Types.BackupSelectionMode } })}
      />
      {mode !== Types.BackupSelectionMode.All && (
        <Accordion variant="separated">
          {servers.map((server) => (
            <VolumeServerChoices key={server.id} serverId={server.id} serverName={server.name} selected={selected} toggle={toggle} />
          ))}
        </Accordion>
      )}
    </Stack>
  );
}

function VolumeServerChoices({
  serverId,
  serverName,
  selected,
  toggle,
}: {
  serverId: string;
  serverName: string;
  selected: Types.BackupVolumeTarget[];
  toggle: (target: Types.BackupVolumeTarget) => void;
}) {
  const volumes = useRead("ListVolumes", { server: serverId }).data ?? [];
  return (
    <Accordion.Item value={serverId}>
      <Accordion.Control>{serverName}</Accordion.Control>
      <Accordion.Panel>
        <SimpleGrid cols={{ base: 1, md: 3 }}>
          {volumes.map((volume) => (
            <Checkbox
              key={volume.name}
              label={volume.name}
              checked={selected.some((item) => item.server_id === serverId && item.volume_name === volume.name)}
              onChange={() => toggle({ server_id: serverId, volume_name: volume.name })}
            />
          ))}
          {!volumes.length && <Text c="dimmed">No volumes reported.</Text>}
        </SimpleGrid>
      </Accordion.Panel>
    </Accordion.Item>
  );
}

function RepositoryEditor({
  label,
  repository,
  onChange,
}: {
  label: string;
  repository: Types.BackupRepository;
  onChange: (repository: Types.BackupRepository) => void;
}) {
  const backend = repository.backend;
  const updateBackend = (next: Types.BackupRepositoryBackend) => onChange({ ...repository, backend: next });
  return (
    <Stack mt="md">
      <Text fw={600}>{label}</Text>
      <Grid>
        <Grid.Col span={{ base: 12, md: 4 }}>
          <TextInput label="Repository name" value={repository.name} onChange={(event) => onChange({ ...repository, name: event.currentTarget.value })} />
        </Grid.Col>
        <Grid.Col span={{ base: 12, md: 4 }}>
          <Select label="Backend" value={backend.type} data={["CoreLocal", "S3", "Sftp", "Rest"]} onChange={(value) => updateBackend(backendDefaults(value as Types.BackupRepositoryBackend["type"]))} />
        </Grid.Col>
        <Grid.Col span={{ base: 12, md: 4 }}>
          <PasswordInput
            label="Encryption passphrase"
            description={repository.passphrase?.configured ? "Configured; leave empty to preserve it." : "Required"}
            value={repository.passphrase?.value ?? ""}
            onChange={(event) => onChange({ ...repository, passphrase: { ...repository.passphrase, value: event.currentTarget.value } })}
          />
        </Grid.Col>
      </Grid>
      {backend.type === "CoreLocal" && (
        <TextInput
          label="Persistent Core path"
          description="Restart Core after adding or changing a Core-local repository. Backups fail safely until its authenticated endpoint is active."
          value={backend.params.path}
          onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, path: event.currentTarget.value } })}
        />
      )}
      {backend.type === "S3" && (
        <SimpleGrid cols={{ base: 1, md: 2 }}>
          <TextInput label="S3 URL" placeholder="s3://bucket/prefix" value={backend.params.url} onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, url: event.currentTarget.value } })} />
          <TextInput label="Region" value={backend.params.region} onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, region: event.currentTarget.value } })} />
          <PasswordInput label="Access key ID" description={backend.params.access_key_id.configured ? "Configured" : undefined} value={backend.params.access_key_id.value ?? ""} onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, access_key_id: { ...backend.params.access_key_id, value: event.currentTarget.value } } })} />
          <PasswordInput label="Secret access key" description={backend.params.secret_access_key.configured ? "Configured" : undefined} value={backend.params.secret_access_key.value ?? ""} onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, secret_access_key: { ...backend.params.secret_access_key, value: event.currentTarget.value } } })} />
        </SimpleGrid>
      )}
      {backend.type === "Sftp" && (
        <SimpleGrid cols={{ base: 1, md: 2 }}>
          <TextInput label="SFTP URL" placeholder="sftp://user@host/path" value={backend.params.url} onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, url: event.currentTarget.value } })} />
          <NumberInput label="Timeout (seconds)" min={1} value={backend.params.timeout_seconds} onChange={(value) => updateBackend({ ...backend, params: { ...backend.params, timeout_seconds: Number(value) } })} />
          <PasswordInput label="Private key" description={backend.params.private_key.configured ? "Configured" : "Paste an OpenSSH private key"} value={backend.params.private_key.value ?? ""} onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, private_key: { ...backend.params.private_key, value: event.currentTarget.value } } })} />
          <TextInput label="Known-hosts entry" value={backend.params.known_hosts} onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, known_hosts: event.currentTarget.value } })} />
        </SimpleGrid>
      )}
      {backend.type === "Rest" && (
        <SimpleGrid cols={{ base: 1, md: 2 }}>
          <TextInput label="REST repository URL" value={backend.params.url} onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, url: event.currentTarget.value } })} />
          <PasswordInput label="Access token" description={backend.params.access_token.configured ? "Configured" : undefined} value={backend.params.access_token.value ?? ""} onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, access_token: { ...backend.params.access_token, value: event.currentTarget.value } } })} />
          <Checkbox label="Allow insecure HTTP" checked={backend.params.allow_insecure_http ?? false} onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, allow_insecure_http: event.currentTarget.checked } })} />
        </SimpleGrid>
      )}
    </Stack>
  );
}
