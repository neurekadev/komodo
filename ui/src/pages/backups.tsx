import { BrowseSnapshotButton, RestoreSnapshotButton } from "@/components/backups/resource";
import { usePreviewRequest } from "@/components/backups/use-preview-request";
import { komodo_client, useInvalidate, useRead, useSetTitle, useUser, useWrite } from "@/lib/hooks";
import { ICONS } from "@/lib/icons";
import { useUrlBackedTab } from "@/lib/navigation";
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
  Paper,
  PasswordInput,
  Select,
  SimpleGrid,
  Stack,
  Switch,
  Tabs,
  Text,
  TextInput,
  Textarea,
} from "@mantine/core";
import { useLocalStorage } from "@mantine/hooks";
import { notifications } from "@mantine/notifications";
import { Types } from "komodo_client";
import { MobileFriendlyTabsSelector, Page, Section, TabNoContent } from "mogh_ui";
import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";

type BackupTabsView = "Overview" | "Schedule" | "Repositories" | "Recovery";

const BACKUP_TAB_VALUES: readonly BackupTabsView[] = [
  "Overview",
  "Schedule",
  "Repositories",
  "Recovery",
];

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
          worker_access_key_id: {},
          worker_secret_access_key: {},
          soft_delete: false,
        },
      };
    case "Sftp":
      return {
        type,
        params: {
          url: "",
          private_key: {},
          worker_private_key: {},
          known_hosts: "",
          timeout_seconds: 30,
        },
      };
    case "Rest":
      return {
        type,
        params: { url: "", access_token: {}, worker_access_token: {}, allow_insecure_http: false },
      };
    default:
      return { type: "CoreLocal", params: { path: "/backups/vykar" } };
  }
};

const patternLines = (value: string) =>
  value.split("\n").map((line) => line.replace(/\r$/, ""));

export default function Backups() {
  useSetTitle("Backups");
  const user = useUser().data;
  const admin = user?.admin ?? false;
  const status = useRead("GetBackupStatus", {}, { refetchInterval: 15_000 });
  const settingsQuery = useRead("GetBackupSettings", {}, { enabled: admin });
  const [settings, setSettings] = useState<Types.BackupSettings>();
  const [storedView, setStoredView] = useLocalStorage<BackupTabsView>({
    key: "backups-tab-v1",
    defaultValue: "Overview",
  });
  const [requestedView, setView] = useUrlBackedTab(
    "tab",
    BACKUP_TAB_VALUES,
    storedView,
    setStoredView,
  );
  const [searchParams] = useSearchParams();
  const tabParameter = searchParams.get("tab")?.toLowerCase();
  const invalidTab =
    tabParameter !== undefined &&
    !BACKUP_TAB_VALUES.some((tab) => tab.toLowerCase() === tabParameter);
  const view = admin && !invalidTab ? requestedView : "Overview";
  const tabs = useMemo<TabNoContent[]>(
    () => [
      { value: "Overview", icon: ICONS.Dashboard },
      { value: "Schedule", icon: ICONS.Schedule, hidden: !admin },
      { value: "Repositories", icon: ICONS.Volume, hidden: !admin },
      { value: "Recovery", icon: ICONS.Restart, hidden: !admin },
    ],
    [admin],
  );

  useEffect(() => {
    if (settingsQuery.data) setSettings(structuredClone(settingsQuery.data));
  }, [settingsQuery.data?.updated_at]);

  useEffect(() => {
    if (invalidTab || (user && !admin && requestedView !== "Overview")) {
      setView("Overview");
    }
  }, [admin, invalidTab, requestedView, setView, user]);

  return (
    <Page
      title="Backups"
      icon={ICONS.Backup}
      description="Schedule and recover encrypted Core, Stack, and Volume backups across every Periphery."
    >
      <Tabs value={view}>
        <Stack gap="xl">
          <Group justify="space-between">
            {admin ? (
              <MobileFriendlyTabsSelector
                tabs={tabs}
                value={view}
                onValueChange={setView as any}
              />
            ) : (
              <div />
            )}
          <Button
            component="a"
            href="https://komodo.docs.neureka.dev/administration/backups"
            target="_blank"
            variant="subtle"
          >
            Backup and recovery guide
          </Button>
          </Group>
          {view === "Overview" ? (
            <OverviewTab status={status.data} />
          ) : !settings ? (
            <Loader />
          ) : view === "Recovery" ? (
            <CoreRecoverySection />
          ) : (
            <BackupSettingsForm
              view={view}
              settings={settings}
              persistedSettings={settingsQuery.data}
              onChange={setSettings}
            />
          )}
          {!admin && (
            <Alert color="blue">
              Repository setup and fleet scheduling are administrator-only. Use
              the Backups tab on a permitted Stack or Volume to browse snapshots,
              run a backup, or restore it.
            </Alert>
          )}
        </Stack>
      </Tabs>
    </Page>
  );
}

function OverviewTab({ status }: { status?: Types.BackupStatus }) {
  const admin = useUser().data?.admin ?? false;
  const invalidate = useInvalidate();
  const { mutate: run, isPending: running } = useWrite("RunBackup", {
    onSuccess: (result) => {
      invalidate(["GetBackupStatus"]);
      notifications.show({
        color: result.state === Types.BackupRunState.Failed ? "red" : "green",
        message: result.message,
      });
    },
  });
  return (
    <Stack gap="xl">
      {admin && (
        <Group justify="end">
          <Button
            variant="light"
            loading={running}
            onClick={() => run({})}
          >
            Back up fleet now
          </Button>
          <Button
            variant="light"
            loading={running}
            onClick={() => run({ target: { type: "Core" } })}
          >
            Back up Core now
          </Button>
        </Group>
      )}
      <StatusSection status={status} />
      {admin && <SnapshotInventory />}
    </Stack>
  );
}

function SnapshotInventory() {
  const [page, setPage] = useState(1);
  const limit = 20;
  const inventory = useRead(
    "ListBackupSnapshots",
    { page: page - 1, limit },
    { refetchOnWindowFocus: false },
  );
  const pages = Math.max(1, Math.ceil((inventory.data?.total ?? 0) / limit));
  return (
    <Section title="Primary snapshots" icon={<ICONS.Backup size="1.3rem" />}>
      <Stack gap="xs">
        <Text size="sm" c="dimmed">
          This inventory is read directly from the active primary repository.
          Administrators can recover complete Stack and Volume snapshots here
          even after the original resource has been deleted.
        </Text>
        <Group justify="end">
          <Button variant="subtle" loading={inventory.isFetching} onClick={() => inventory.refetch()}>
            Refresh inventory
          </Button>
        </Group>
        {inventory.data?.snapshots.map((snapshot) => (
          <Group key={snapshot.name} justify="space-between" wrap="nowrap">
            <Stack gap={0} className="overflow-hidden">
              <Text size="sm" fw={500} truncate>
                {snapshotTargetLabel(snapshot.target)}
              </Text>
              <Text size="xs" c="dimmed" truncate>
                {snapshot.name} · {snapshot.hostname}
              </Text>
            </Stack>
            <Group gap="xs" wrap="nowrap">
              <Badge color={snapshot.partial ? "orange" : "green"}>
                {snapshot.partial ? "Partial" : "Complete"}
              </Badge>
              <BrowseSnapshotButton snapshot={snapshot} compact />
              {!snapshot.partial &&
                (snapshot.target.type === "Stack" ||
                  snapshot.target.type === "Volume") && (
                  <RestoreSnapshotButton
                    target={snapshot.target}
                    sourceServerId={snapshotSourceServerId(snapshot)}
                    snapshot={snapshot}
                    forceStackRecovery={snapshot.target.type === "Stack"}
                    forceVolumeRecovery={snapshot.target.type === "Volume"}
                    compact
                  >
                    Recover
                  </RestoreSnapshotButton>
                )}
            </Group>
          </Group>
        ))}
        {!inventory.isPending && !inventory.data?.snapshots.length && (
          <Text c="dimmed">No snapshots in the primary repository.</Text>
        )}
        {pages > 1 && (
          <Pagination total={pages} value={page} onChange={setPage} />
        )}
      </Stack>
    </Section>
  );
}

function snapshotSourceServerId(snapshot: Types.BackupSnapshot) {
  if (snapshot.target.type === "Volume") {
    return snapshot.target.params.server_id;
  }
  const prefix = "komodo-periphery-";
  return snapshot.hostname.startsWith(prefix)
    ? snapshot.hostname.slice(prefix.length)
    : "";
}

function snapshotTargetLabel(target: Types.BackupTarget) {
  switch (target.type) {
    case "Core":
      return "Core";
    case "Stack":
      return `Stack · ${target.params.stack_id}`;
    case "Volume":
      return `Volume · ${target.params.server_id}/${target.params.volume_name}`;
    case "Unbound":
      return `Unbound · ${target.params.source_label}`;
  }
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
                ? `Healthy · ${status.mirror_lagging_snapshots} lagging`
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
          label="Next run"
          value={
            status?.next_run_at
              ? new Date(status.next_run_at).toLocaleString()
              : "Not scheduled"
            }
          />
        <StatusItem
          label="Last full verification"
          value={
            status?.last_full_verification_at
              ? new Date(status.last_full_verification_at).toLocaleString()
              : "Never"
          }
        />
      </SimpleGrid>
      {status?.critical_alert && (
        <Alert
          color="red"
          mt="md"
          icon={<ICONS.Alert size="1rem" />}
          style={{ whiteSpace: "pre-wrap" }}
        >
          {status.critical_alert}
        </Alert>
      )}
      {admin && !!status?.active_runs?.length && (
        <Stack mt="md" gap="xs">
          {(status.active_runs ?? []).map((run) => (
            <Group key={run.id} justify="space-between">
              <Text size="sm">Active: {run.message}</Text>
              {run.cancellable && (
                <Button
                  color="red"
                  variant="light"
                  loading={cancelling}
                  onClick={() => cancel({ run_id: run.id })}
                >
                  Cancel run
                </Button>
              )}
            </Group>
          ))}
        </Stack>
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
  view,
  settings,
  persistedSettings,
  onChange,
}: {
  view: "Schedule" | "Repositories";
  settings: Types.BackupSettings;
  persistedSettings?: Types.BackupSettings;
  onChange: (settings: Types.BackupSettings) => void;
}) {
  const invalidate = useInvalidate();
  const patch = (value: Partial<Types.BackupSettings>) =>
    onChange({ ...settings, ...value });
  const settingsDirty =
    JSON.stringify(settings) !== JSON.stringify(persistedSettings);
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
      {view === "Schedule" && (
        <>
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
        <Stack mb="md">
          <Switch
            label="Include cross-filesystem mount content"
            description="Disabled by default. Enable only when Stack bind mounts or nested source directories on other filesystems, including rclone/FUSE mounts, must be backed up."
            checked={settings.include_cross_filesystem_mounts ?? false}
            onChange={(event) =>
              patch({
                include_cross_filesystem_mounts: event.currentTarget.checked,
              })
            }
          />
          <Switch
            label="Include anonymous Docker volumes"
            description="Disabled by default. Anonymous daemon-generated volumes are omitted from Volume backups unless enabled."
            checked={settings.include_anonymous_volumes ?? false}
            onChange={(event) =>
              patch({ include_anonymous_volumes: event.currentTarget.checked })
            }
          />
          <SimpleGrid cols={{ base: 1, md: 2 }}>
            <Textarea
              label="Bind mount include patterns"
              description="Optional Vykar/gitignore-style absolute path rules, one per line. Empty includes every eligible bind mount."
              placeholder={"/srv/**\n!/srv/cache/**"}
              value={(settings.bind_mount_include_patterns ?? []).join("\n")}
              onChange={(event) =>
                patch({
                  bind_mount_include_patterns: patternLines(
                    event.currentTarget.value,
                  ),
                })
              }
              autosize
              minRows={3}
            />
            <Textarea
              label="Bind mount exclude patterns"
              description="Vykar/gitignore-style absolute path rules, one per line. Excludes are applied after includes."
              placeholder={"/mnt/rclone/**\n**/.cache/**"}
              value={(settings.bind_mount_exclude_patterns ?? []).join("\n")}
              onChange={(event) =>
                patch({
                  bind_mount_exclude_patterns: patternLines(
                    event.currentTarget.value,
                  ),
                })
              }
              autosize
              minRows={3}
            />
          </SimpleGrid>
        </Stack>
        <StackSelectionEditor settings={settings} patch={patch} />
        <VolumeSelectionEditor settings={settings} patch={patch} />
          </Section>
        </>
      )}

      {view === "Repositories" && (
        <>
          <Section title="Encrypted repositories" icon={<ICONS.Volume size="1.3rem" />}>
        <Alert color="blue" mb="md">
          The primary is the only snapshot source of truth. A mirror is not
          readable until full verification and explicit promotion.
        </Alert>
        <Alert color="orange" mb="md">
          Every Periphery that writes to a shared Vykar repository receives
          its encryption passphrase and can read that repository. Workers must
          also trust each other's writes: a compromised worker can replace
          another worker's Stack or Volume snapshot. Do not use cross-node
          restore across untrusted hosts. Use separate Komodo deployments,
          repositories, credentials, and passphrases for hosts that must not
          share this mutual read/write trust.
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
                      params: { path: "/backups/vykar-mirror" },
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
          <TrustedWorkersEditor settings={settings} patch={patch} />
        </>
      )}

      <Accordion variant="contained">
        <Accordion.Item value="advanced">
          <Accordion.Control>
            {view === "Schedule"
              ? "Advanced execution settings"
              : "Advanced integrity and maintenance"}
          </Accordion.Control>
          <Accordion.Panel>
            <SimpleGrid cols={{ base: 1, md: 2 }}>
              {view === "Schedule" ? (
                <>
                  <NumberInput
                    label="Concurrent nodes"
                    min={1}
                    value={settings.advanced.node_concurrency}
                    onChange={(value) =>
                      patch({
                        advanced: {
                          ...settings.advanced,
                          node_concurrency: Number(value),
                        },
                      })
                    }
                  />
                  <NumberInput
                    label="Upload MiB/s, per node"
                    description="Zero is unlimited."
                    min={0}
                    step={1}
                    allowDecimal={false}
                    value={
                      settings.advanced.upload_bytes_per_second / (1024 * 1024)
                    }
                    onChange={(value) =>
                      patch({
                        advanced: {
                          ...settings.advanced,
                          upload_bytes_per_second:
                            Number(value) * 1024 * 1024,
                        },
                      })
                    }
                  />
                </>
              ) : (
                <>
                  <NumberInput
                    label="Client repack cap (bytes)"
                    description="Defaults to 5 GiB per S3/SFTP maintenance cycle."
                    min={0}
                    value={settings.advanced.client_repack_limit_bytes}
                    onChange={(value) =>
                      patch({
                        advanced: {
                          ...settings.advanced,
                          client_repack_limit_bytes: Number(value),
                        },
                      })
                    }
                  />
                  <NumberInput
                    label="Compaction threshold (%)"
                    min={1}
                    max={100}
                    value={settings.advanced.compact_threshold_percent}
                    onChange={(value) =>
                      patch({
                        advanced: {
                          ...settings.advanced,
                          compact_threshold_percent: Number(value),
                        },
                      })
                    }
                  />
                  <NumberInput
                    label="Full verification interval (days)"
                    min={1}
                    value={settings.advanced.full_verify_every_days}
                    onChange={(value) =>
                      patch({
                        advanced: {
                          ...settings.advanced,
                          full_verify_every_days: Number(value),
                        },
                      })
                    }
                  />
                  <NumberInput
                    label="Repository sample per cycle (%)"
                    min={1}
                    max={100}
                    value={settings.advanced.verify_sample_percent}
                    onChange={(value) =>
                      patch({
                        advanced: {
                          ...settings.advanced,
                          verify_sample_percent: Number(value),
                        },
                      })
                    }
                  />
                </>
              )}
            </SimpleGrid>
          </Accordion.Panel>
        </Accordion.Item>
      </Accordion>

      {view === "Repositories" && (
        <Section
          title="Repository operations"
          icon={<ICONS.Execution size="1.3rem" />}
        >
          {settingsDirty && (
            <Text size="sm" c="dimmed" mb="md">
              Save settings before initializing, verifying, or promoting a
              repository.
            </Text>
          )}
          <Group justify="end">
            <Button
              variant="default"
              loading={initializing}
              disabled={settingsDirty || saving}
              title={
                settingsDirty
                  ? "Save the displayed settings before initializing repositories."
                  : undefined
              }
              onClick={() => initialize({})}
            >
              Initialize repositories
            </Button>
            <Button
              variant="default"
              loading={verifying}
              disabled={settingsDirty || saving}
              onClick={() => verify({ mirror: false, full: true })}
            >
              Verify primary
            </Button>
            {settings.mirror && (
              <>
                <Button
                  variant="default"
                  loading={verifying}
                  disabled={settingsDirty || saving}
                  onClick={() => verify({ mirror: true, full: true })}
                >
                  Verify mirror
                </Button>
                <Button
                  color="orange"
                  loading={promoting}
                  disabled={settingsDirty || saving}
                  onClick={() =>
                    promote({ allow_primary_unavailable: false })
                  }
                >
                  Verify and promote mirror
                </Button>
                <Button
                  color="red"
                  variant="light"
                  loading={promoting}
                  disabled={settingsDirty || saving}
                  onClick={() => {
                    if (
                      globalThis.confirm(
                        "The primary repository is unavailable. Fully verify and promote the mirror without comparing it to the primary inventory?",
                      )
                    ) {
                      promote({ allow_primary_unavailable: true });
                    }
                  }}
                >
                  Disaster recovery promotion
                </Button>
              </>
            )}
          </Group>
        </Section>
      )}

      <Paper
        withBorder
        shadow="md"
        p="md"
        style={{ position: "sticky", bottom: "1rem", zIndex: 10 }}
      >
        <Group justify="space-between">
          <Text size="sm" c={settingsDirty ? undefined : "dimmed"}>
            {settingsDirty ? "You have unsaved changes." : "Settings are up to date."}
          </Text>
          <Button
            leftSection={<ICONS.Save size="1rem" />}
            loading={saving}
            disabled={!settingsDirty}
            onClick={() => save({ settings })}
          >
            Save settings
          </Button>
        </Group>
      </Paper>
    </Stack>
  );
}

function CoreRecoverySection() {
  const [repository, setRepository] = useState<Types.BackupRepository>({
    name: "Recovery source", backend: backendDefaults("CoreLocal"), passphrase: {},
  });
  const [snapshot, setSnapshot] = useState<string>();
  const [snapshotPage, setSnapshotPage] = useState(1);
  const [snapshotPages, setSnapshotPages] = useState(1);
  const { preview: plan, begin, invalidate } = usePreviewRequest<Types.CoreRecoveryPlan>(
    JSON.stringify({ snapshot, snapshotPage, repository }),
  );
  const { preview: snapshotResponse, begin: beginInventory } = usePreviewRequest<Types.BackupSnapshotList>(
    JSON.stringify({ repository, snapshotPage }),
  );
  const [loadingSnapshots, setLoadingSnapshots] = useState(false);
  const loadSnapshots = async (page = snapshotPage) => {
    setSnapshot(undefined);
    const accept = beginInventory();
    setLoadingSnapshots(true);
    try {
      accept(await komodo_client().read("ListCoreRecoverySnapshots", {
        repository, page: page - 1, limit: 100,
      }));
    } catch {
      notifications.show({ color: "red", message: "Could not open the recovery repository. Check its address, access credentials, and passphrase." });
    } finally {
      setLoadingSnapshots(false);
    }
  };
  const snapshots = snapshotResponse?.snapshots ?? [];
  useEffect(() => {
    if (snapshotResponse) {
      setSnapshotPages(Math.max(1, Math.ceil(snapshotResponse.total / 100)));
    }
  }, [snapshotResponse]);
  const { mutateAsync: planRecovery, isPending: planning } = useWrite("PlanCoreRecovery");
  const { mutate: executeRecovery, isPending: executing } = useWrite(
    "ExecuteCoreRecovery",
    {
      onSuccess: (run) =>
        notifications.show({ color: "orange", message: run.message }),
    },
  );
  const reviewRecovery = async () => {
    if (!snapshot) return;
    const accept = begin();
    try {
      accept(await planRecovery({ snapshot, repository }));
    } catch {
      // useWrite already reports the failed validation request.
    }
  };
  return (
    <Section title="Fresh Core recovery" icon={<ICONS.Restart size="1.3rem" />}>
      <Stack>
        <Text c="dimmed" size="sm">
          Restore a Core snapshot into a separate validation database. Komodo
          verifies its schema, version, and administrator access before it can
          become active; the current database is retained for rollback.
        </Text>
        <RepositoryEditor label="Existing recovery repository" repository={repository} recoveryOnly onChange={(value) => {
          setSnapshot(undefined);
          setSnapshotPage(1);
          setSnapshotPages(1);
          setRepository(value);
        }} />
        <Button variant="light" loading={loadingSnapshots} onClick={() => loadSnapshots()}>
          Load recovery snapshots
        </Button>
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
            disabled={!snapshot || !snapshotResponse}
            onClick={reviewRecovery}
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
        onClose={invalidate}
        title="Activate recovered Core database"
        size="lg"
      >
        {plan && (
          <Stack>
            <Alert color="orange">
              Core will restart after activation. Do not run another active
              Core against either database during recovery.
            </Alert>
            <StatusItem label="Snapshot" value={plan.snapshot} />
            <StatusItem label="Snapshot version" value={plan.backup_version} />
            <StatusItem label="Validated database" value={plan.validation_database} />
            <StatusItem label="Retained rollback database" value={plan.current_database} />
            <Group justify="end">
              <Button variant="default" onClick={invalidate}>
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
  const servers = useRead("ListServers", { query: {}, limit: 0 }).data ?? [];
  const mode = settings.stack_selection.mode ?? Types.BackupSelectionMode.All;
  const selected = settings.stack_selection.stack_ids ?? [];
  const toggle = (stackId: string) => {
    patch({
      stack_selection: {
        ...settings.stack_selection,
        stack_ids: selected.includes(stackId)
          ? selected.filter((id) => id !== stackId)
          : [...selected, stackId],
      },
    });
  };

  return (
    <Stack mt="md">
      <Select
        label="Stack selection"
        value={mode}
        data={Object.values(Types.BackupSelectionMode)}
        onChange={(value) =>
          patch({
            stack_selection: {
              ...settings.stack_selection,
              mode: value as Types.BackupSelectionMode,
            },
          })
        }
      />
      {mode !== Types.BackupSelectionMode.All && (
        <Accordion variant="separated">
          {servers.map((server) => (
            <StackServerChoices
              key={server.id}
              serverId={server.id}
              serverName={server.name}
              selected={selected}
              toggle={toggle}
            />
          ))}
        </Accordion>
      )}
    </Stack>
  );
}

function StackServerChoices({
  serverId,
  serverName,
  selected,
  toggle,
}: {
  serverId: string;
  serverName: string;
  selected: string[];
  toggle: (stackId: string) => void;
}) {
  const stacks =
    useRead("ListStacks", {
      query: { specific: { server_ids: [serverId] } },
      limit: 0,
    }).data ?? [];

  return (
    <Accordion.Item value={serverId}>
      <Accordion.Control>{serverName}</Accordion.Control>
      <Accordion.Panel>
        <SimpleGrid cols={{ base: 1, md: 3 }}>
          {stacks.map((stack) => (
            <Checkbox
              key={stack.id}
              label={stack.name}
              checked={selected.includes(stack.id)}
              onChange={() => toggle(stack.id)}
            />
          ))}
          {!stacks.length && <Text c="dimmed">No stacks reported.</Text>}
        </SimpleGrid>
      </Accordion.Panel>
    </Accordion.Item>
  );
}

function TrustedWorkersEditor({
  settings,
  patch,
}: {
  settings: Types.BackupSettings;
  patch: (value: Partial<Types.BackupSettings>) => void;
}) {
  const servers = useRead("ListServers", { query: {}, limit: 0 }).data ?? [];
  const [selected, setSelected] = useState<string | null>(null);
  const serverQuery = useRead(
    "GetServer",
    { server: selected ?? "" },
    { enabled: !!selected, refetchOnWindowFocus: false },
  );
  // Never approve stale details from a different selected Server.
  const server = serverQuery.data?._id?.$oid === selected ? serverQuery.data : undefined;
  const publicKey = server?.info?.public_key;
  const address = server?.config?.address;
  const workers = settings.trusted_workers ?? [];
  return (
    <Section title="Trusted backup workers" icon={<ICONS.Server size="1.3rem" />}>
      <Stack>
        <Alert color="orange">
          Enrollment gives this host the shared passphrase and repository
          credentials available to workers, including read/decryption access to
          fleet and Core snapshots. Verify the host and its public key
          independently. Server creation and Backups permissions do not enroll a
          worker. Save settings to apply changes.
        </Alert>
        <Select
          label="Server to trust"
          searchable
          clearable
          value={selected}
          onChange={setSelected}
          data={servers.map((item) => ({ value: item.id, label: item.name }))}
        />
        {server && (
          <>
            <Text size="sm">Address: {address || "Inbound Periphery connection"}</Text>
            <Text size="sm" style={{ overflowWrap: "anywhere" }}>
              Public key: {publicKey || "Not configured; configure and verify a public key first"}
            </Text>
          </>
        )}
        <Button
          color="orange"
          disabled={!selected || !publicKey?.trim() || address === undefined || serverQuery.isFetching || !!serverQuery.error}
          onClick={() => {
            if (!selected || !publicKey?.trim() || address === undefined || serverQuery.isFetching || serverQuery.error) return;
            patch({ trusted_workers: [
              ...workers.filter((worker) => worker.server_id !== selected),
              { server_id: selected, address, public_key: publicKey },
            ] });
            setSelected(null);
          }}
        >
          Trust worker with repository access
        </Button>
        {workers.map((worker) => (
          <Group key={worker.server_id} justify="space-between" wrap="nowrap">
            <Stack gap={2} style={{ minWidth: 0 }}>
              <Text size="sm">{servers.find((item) => item.id === worker.server_id)?.name ?? worker.server_id}</Text>
              <Text size="xs" c="dimmed" style={{ overflowWrap: "anywhere" }}>
                {worker.address || "Inbound Periphery connection"} — {worker.public_key}
              </Text>
            </Stack>
            <Button
              color="red"
              variant="subtle"
              onClick={() => patch({ trusted_workers: workers.filter((item) => item.server_id !== worker.server_id) })}
            >
              Remove trust
            </Button>
          </Group>
        ))}
        {!workers.length && <Text c="dimmed">No workers enrolled. Core-only backups remain available.</Text>}
      </Stack>
    </Section>
  );
}

function VolumeSelectionEditor({
  settings,
  patch,
}: {
  settings: Types.BackupSettings;
  patch: (value: Partial<Types.BackupSettings>) => void;
}) {
  const servers = useRead("ListServers", { query: {}, limit: 0 }).data ?? [];
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
  const volumes = (useRead("ListVolumes", { server: serverId }).data ?? [])
    .filter(
      (volume) =>
        volume.driver === "local" &&
        volume.scope === Types.VolumeScopeEnum.Local,
    );
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
  recoveryOnly = false,
}: {
  label: string;
  recoveryOnly?: boolean;
  repository: Types.BackupRepository;
  onChange: (repository: Types.BackupRepository) => void;
}) {
  const backend = repository.backend;
  const updateBackend = (next: Types.BackupRepositoryBackend) => onChange({ ...repository, backend: next });
  const useWorkerCredentials =
    repository.use_worker_credentials ?? workerCredentialsConfigured(backend);
  return (
    <Stack mt="md">
      <Text fw={600}>{label}</Text>
      <Grid>
        <Grid.Col span={{ base: 12, md: 4 }}>
          <TextInput label="Repository name" value={repository.name} onChange={(event) => onChange({ ...repository, name: event.currentTarget.value })} />
        </Grid.Col>
        <Grid.Col span={{ base: 12, md: 4 }}>
          <Select
            label="Backend"
            value={backend.type}
            data={[
              { value: "CoreLocal", label: "Local" },
              { value: "S3", label: "S3" },
              { value: "Sftp", label: "SFTP" },
              { value: "Rest", label: "REST" },
            ]}
            onChange={(value) => {
              const next = backendDefaults(
                value as Types.BackupRepositoryBackend["type"],
              );
              onChange({
                ...repository,
                backend: next,
                use_worker_credentials:
                  next.type === "CoreLocal" ? undefined : false,
              });
            }}
          />
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
          description={recoveryOnly ? "Path to the existing repository mounted into Core." : "Restart Core after adding or changing a local repository."}
          value={backend.params.path}
          onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, path: event.currentTarget.value } })}
        />
      )}
      {backend.type !== "CoreLocal" && !recoveryOnly && (
        <Stack gap="xs">
          <Switch
            label="Use separate credentials for backup workers"
            description="Recommended when workers should not be able to delete or maintain repository data."
            checked={useWorkerCredentials}
            onChange={(event) =>
              onChange({
                ...repository,
                use_worker_credentials: event.currentTarget.checked,
              })
            }
          />
          {!useWorkerCredentials && (
            <Alert color="orange">
              Trusted workers will receive the Core repository credentials. A
              compromised worker could delete or maintain backup data.
            </Alert>
          )}
        </Stack>
      )}
      {backend.type === "S3" && (
        <SimpleGrid cols={{ base: 1, md: 2 }}>
          <TextInput label="S3 URL" placeholder="s3://endpoint/bucket/prefix" value={backend.params.url} onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, url: event.currentTarget.value } })} />
          {<Checkbox label="S3 soft delete" description="Uses tombstones; bucket policies may retain older data and increase storage." checked={backend.params.soft_delete ?? false} onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, soft_delete: event.currentTarget.checked } })} />}
          <TextInput label="Region" value={backend.params.region} onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, region: event.currentTarget.value } })} />
          <PasswordInput label={recoveryOnly ? "Access key ID" : "Core access key ID"} description={backend.params.access_key_id.configured ? "Configured" : undefined} value={backend.params.access_key_id.value ?? ""} onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, access_key_id: { ...backend.params.access_key_id, value: event.currentTarget.value } } })} />
          <PasswordInput label={recoveryOnly ? "Secret access key" : "Core secret access key"} description={backend.params.secret_access_key.configured ? "Configured" : undefined} value={backend.params.secret_access_key.value ?? ""} onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, secret_access_key: { ...backend.params.secret_access_key, value: event.currentTarget.value } } })} />
          {!recoveryOnly && useWorkerCredentials && (<PasswordInput label="Worker access key ID" description={backend.params.worker_access_key_id?.configured ? "Configured" : "Required; must differ from Core credentials"} value={backend.params.worker_access_key_id?.value ?? ""} onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, worker_access_key_id: { ...backend.params.worker_access_key_id, value: event.currentTarget.value } } })} />)}
          {!recoveryOnly && useWorkerCredentials && (<PasswordInput label="Worker secret access key" description={backend.params.worker_secret_access_key?.configured ? "Configured" : "Required; must differ from Core credentials"} value={backend.params.worker_secret_access_key?.value ?? ""} onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, worker_secret_access_key: { ...backend.params.worker_secret_access_key, value: event.currentTarget.value } } })} />)}
        </SimpleGrid>
      )}
      {backend.type === "Sftp" && (
        <SimpleGrid cols={{ base: 1, md: 2 }}>
          <TextInput label="SFTP URL" placeholder="sftp://user@host/path" value={backend.params.url} onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, url: event.currentTarget.value } })} />
          <NumberInput label="Timeout (seconds)" min={1} value={backend.params.timeout_seconds} onChange={(value) => updateBackend({ ...backend, params: { ...backend.params, timeout_seconds: Number(value) } })} />
          <Textarea autosize minRows={5} autoComplete="off" spellCheck={false} label={recoveryOnly ? "Private key" : "Core private key"} description={backend.params.private_key.configured ? "Configured; paste only to replace it" : "Paste the complete multiline OpenSSH private key"} value={backend.params.private_key.value ?? ""} onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, private_key: { ...backend.params.private_key, value: event.currentTarget.value } } })} />
          {!recoveryOnly && useWorkerCredentials && (<Textarea autosize minRows={5} autoComplete="off" spellCheck={false} label="Worker private key" description={backend.params.worker_private_key?.configured ? "Configured; paste only to replace it" : "Paste a distinct key for a maintenance-denied account"} value={backend.params.worker_private_key?.value ?? ""} onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, worker_private_key: { ...backend.params.worker_private_key, value: event.currentTarget.value } } })} />)}
          <TextInput label="Known-hosts entry" value={backend.params.known_hosts} onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, known_hosts: event.currentTarget.value } })} />
        </SimpleGrid>
      )}
      {backend.type === "Rest" && (
        <SimpleGrid cols={{ base: 1, md: 2 }}>
          <TextInput label="REST repository URL" value={backend.params.url} onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, url: event.currentTarget.value } })} />
          <PasswordInput label={recoveryOnly ? "Access token" : "Core access token"} description={backend.params.access_token.configured ? "Configured" : undefined} value={backend.params.access_token.value ?? ""} onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, access_token: { ...backend.params.access_token, value: event.currentTarget.value } } })} />
          {!recoveryOnly && useWorkerCredentials && (<PasswordInput label="Worker access token" description={backend.params.worker_access_token?.configured ? "Configured" : "Required; must differ from the Core token"} value={backend.params.worker_access_token?.value ?? ""} onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, worker_access_token: { ...backend.params.worker_access_token, value: event.currentTarget.value } } })} />)}
          <Checkbox label="Allow insecure HTTP" checked={backend.params.allow_insecure_http ?? false} onChange={(event) => updateBackend({ ...backend, params: { ...backend.params, allow_insecure_http: event.currentTarget.checked } })} />
        </SimpleGrid>
      )}
    </Stack>
  );
}

function workerCredentialsConfigured(
  backend: Types.BackupRepositoryBackend,
) {
  switch (backend.type) {
    case "S3":
      return (
        !!backend.params.worker_access_key_id?.configured ||
        !!backend.params.worker_secret_access_key?.configured ||
        !!backend.params.worker_access_key_id?.value ||
        !!backend.params.worker_secret_access_key?.value
      );
    case "Sftp":
      return (
        !!backend.params.worker_private_key?.configured ||
        !!backend.params.worker_private_key?.value
      );
    case "Rest":
      return (
        !!backend.params.worker_access_token?.configured ||
        !!backend.params.worker_access_token?.value
      );
    default:
      return false;
  }
}
