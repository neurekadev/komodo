import ContainersSection from "@/components/docker/containers-section";
import DockerLabelsSection from "@/components/docker/labels-section";
import DockerOptions from "@/components/docker/options";
import FileManager from "@/components/file-manager";
import InspectSection from "@/components/inspect-section";
import { useExecute, usePermissions, useRead, useSetTitle } from "@/lib/hooks";
import { useServer } from "@/resources/server";
import ResourceSubPage from "@/resources/sub-page";
import { ICONS } from "@/lib/icons";
import {
  ConfirmButton,
  MobileFriendlyTabsSelector,
  TabNoContent,
} from "mogh_ui";
import { DataTable } from "mogh_ui";
import { Section } from "mogh_ui";
import { Center, Group, Loader, Stack, Tabs, Text } from "@mantine/core";
import { useLocalStorage } from "@mantine/hooks";
import { Types } from "komodo_client";
import { useNavigate, useParams } from "react-router-dom";
import { useEffect } from "react";
import { DockerDiskMetric } from "@/components/docker/metrics";

type VolumeView = "Info" | "Files";

const VOLUME_TABS: TabNoContent[] = [
  { value: "Info", icon: ICONS.Info },
  { value: "Files", icon: ICONS.FileManager },
];

export default function Volume() {
  const { type, id, volume } = useParams() as {
    type: string;
    id: string;
    volume: string;
  };
  if (type !== "servers") {
    return (
      <Center h="50vh">
        <Text>This resource type does not have any volumes.</Text>
      </Center>
    );
  }
  return <VolumeInner serverId={id} volumeName={volume} />;
}

function VolumeInner({
  serverId,
  volumeName,
}: {
  serverId: string;
  volumeName: string;
}) {
  const [storedView, setView] = useLocalStorage<VolumeView>({
    key: `volume-${serverId}-${volumeName}-tab-v1`,
    defaultValue: "Info",
  });
  const server = useServer(serverId);
  useSetTitle(`${server?.name} | Volume | ${volumeName}`);
  const nav = useNavigate();

  const { specific, specificFileManager, permissionsLoaded } = usePermissions({
    type: "Server",
    id: serverId,
  });

  const {
    data: volume,
    isPending,
    isError,
  } = useRead(
    "InspectVolume",
    {
      server: serverId,
      volume: volumeName,
    },
    { refetchInterval: 10_000 },
  );

  const { mutate: deleteVolume, isPending: deletePending } = useExecute(
    "DeleteVolume",
    {
      onSuccess: () => nav("/servers/" + serverId),
    },
  );

  const containers = useRead(
    "ListContainers",
    {
      server: serverId,
    },
    { refetchInterval: 10_000 },
  ).data?.filter((container) => container.volumes?.includes(volumeName));

  const view =
    storedView === "Files" && !specificFileManager ? "Info" : storedView;
  useEffect(() => {
    if (
      permissionsLoaded &&
      storedView === "Files" &&
      !specificFileManager
    ) {
      setView("Info");
    }
  }, [permissionsLoaded, setView, specificFileManager, storedView]);

  if (isPending) {
    return (
      <Center h="30vh">
        <Loader size="xl" />
      </Center>
    );
  }

  if (isError) {
    return (
      <Center h="30vh">
        <Text>Failed to inspect volume.</Text>
      </Center>
    );
  }

  if (!volume) {
    return (
      <Center h="30vh">
        <Text>No volume found with given name: {volumeName}</Text>
      </Center>
    );
  }

  const unused = containers && containers.length === 0 ? true : false;

  const intention = unused ? "Critical" : "Good";

  const selector = (
    <MobileFriendlyTabsSelector
      tabs={VOLUME_TABS.map((tab) =>
        tab.value === "Files"
          ? { ...tab, hidden: !specificFileManager }
          : tab,
      )}
      value={view}
      onValueChange={setView as any}
    />
  );

  return (
    <ResourceSubPage
      entityTypeName="Volume"
      parentType="Server"
      parentId={serverId}
      name={volumeName}
      icon={ICONS.Volume}
      intent={intention}
      state={unused ? "Unused" : "In Use"}
      info={
        volume.Scope && (
          <Group gap="xs">
            <Text c="dimmed">Scope:</Text>
            <Text>{volume.Scope}</Text>
          </Group>
        )
      }
      executions={
        unused && (
          <ConfirmButton
            color="red"
            icon={<ICONS.Delete size="1rem" />}
            loading={deletePending}
            onClick={() => deleteVolume({ server: serverId, name: volumeName })}
          >
            Delete Volume
          </ConfirmButton>
        )
      }
    >
      <Tabs value={view}>
        {view === "Files" ? (
          <FileManager
            target={{
              type: "Volume",
              params: { server: serverId, volume: volumeName },
            }}
            titleOther={selector}
          />
        ) : (
          <Stack>
            <Group justify="start">{selector}</Group>
            {containers && containers.length > 0 && (
              <ContainersSection serverId={serverId} containers={containers} />
            )}

            <Section title="Details" icon={<ICONS.Info size="1.3rem" />}>
              <DataTable
                tableKey="volume-info"
                data={[volume]}
                columns={[
                  {
                    accessorKey: "Driver",
                    header: "Driver",
                  },
                  {
                    accessorKey: "Scope",
                    header: "Scope",
                  },
                  {
                    accessorKey: "CreatedAt",
                    header: "Created At",
                  },
                  {
                    accessorKey: "DiskUsage.used_bytes",
                    header: "Used Size",
                    cell: ({ row }) => (
                      <DockerDiskMetric
                        status={row.original.DiskUsage?.status}
                        bytes={row.original.DiskUsage?.used_bytes}
                        measuredAt={row.original.DiskUsage?.measured_at}
                        unavailableReason={
                          row.original.DiskUsage?.unavailable_reason
                        }
                      />
                    ),
                  },
                ]}
              />
              {volume.Options && (
                <DockerOptions options={Object.entries(volume.Options)} />
              )}
            </Section>

            {specific.includes(Types.SpecificPermission.Inspect) && (
              <InspectSection json={volume} showToggle />
            )}

            <DockerLabelsSection labels={volume?.Labels} />
          </Stack>
        )}
      </Tabs>
    </ResourceSubPage>
  );
}
