import ContainersSection from "@/components/docker/containers-section";
import DockerLabelsSection from "@/components/docker/labels-section";
import InspectSection from "@/components/inspect-section";
import { useExecute, usePermissions, useRead, useSetTitle } from "@/lib/hooks";
import { useServer } from "@/resources/server";
import ResourceSubPage from "@/resources/sub-page";
import { ICONS } from "@/lib/icons";
import { ConfirmButton, fmtDateWithMinutes } from "mogh_ui";
import { DataTable } from "mogh_ui";
import { PageGuard } from "mogh_ui";
import { Section } from "mogh_ui";
import { ShowHideButton } from "mogh_ui";
import { Box, Center, Group, Text } from "@mantine/core";
import { Types } from "komodo_client";
import { useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { DockerDiskMetric } from "@/components/docker/metrics";
import { serverDockerPath } from "@/lib/navigation";

export default function Image() {
  const { type, id, image } = useParams() as {
    type: string;
    id: string;
    image: string;
  };
  if (type !== "servers") {
    return (
      <Center h="50vh">
        <Text>This resource type does not have any images.</Text>
      </Center>
    );
  }
  return <ImageInner serverId={id} imageName={image} />;
}

function ImageInner({
  serverId,
  imageName,
}: {
  serverId: string;
  imageName: string;
}) {
  const [showHistory, setShowHistory] = useState(false);
  const server = useServer(serverId);
  useSetTitle(`${server?.name} | Image | ${imageName}`);
  const nav = useNavigate();

  const { specific } = usePermissions({
    type: "Server",
    id: serverId,
  });

  const {
    data: image,
    isPending,
    isError,
  } = useRead(
    "InspectImage",
    {
      server: serverId,
      image: imageName,
    },
    { refetchInterval: 10_000 },
  );

  const containers = useRead(
    "ListContainers",
    {
      server: serverId,
    },
    { refetchInterval: 10_000 },
  ).data?.filter((container) =>
    !image?.Id ? false : container.image_id === image?.Id,
  );

  const history = useRead("ListImageHistory", {
    server: serverId,
    image: imageName,
  }).data;

  const { mutate: deleteImage, isPending: deletePending } = useExecute(
    "DeleteImage",
    {
      onSuccess: () => nav(serverDockerPath(serverId, "images")),
    },
  );

  const unused = containers && containers.length === 0 ? true : false;
  const intention = unused ? "Critical" : "Good";

  return (
    <PageGuard
      isPending={isPending}
      error={
        isError
          ? "Failed to inspect image."
          : !image
            ? "No image found with name: " + imageName
            : undefined
      }
    >
      {image && (
        <ResourceSubPage
          entityTypeName="Image"
          parentType="Server"
          parentId={serverId}
          pageProps={{ backTo: serverDockerPath(serverId, "images") }}
          name={imageName}
          icon={ICONS.Image}
          intent={intention}
          state={unused ? "Unused" : "In Use"}
          info={
            image.Id && (
              <Group gap="xs">
                <Text>Id:</Text>
                <Text title={image.Id} maw={150} className="text-ellipsis">
                  {image.Id}
                </Text>
              </Group>
            )
          }
          executions={
            unused && (
              <ConfirmButton
                variant="filled"
                color="red"
                icon={<ICONS.Delete size="1rem" />}
                loading={deletePending}
                onClick={() =>
                  deleteImage({ server: serverId, name: imageName })
                }
              >
                Delete Image
              </ConfirmButton>
            )
          }
        >
          {containers && containers.length > 0 && (
            <ContainersSection serverId={serverId} containers={containers} />
          )}

          {/* TOP LEVEL IMAGE INFO */}
          <Section title="Details" icon={<ICONS.Info size="1.3rem" />}>
            <DataTable
              tableKey="image-info"
              data={[image]}
              columns={[
                {
                  accessorKey: "Architecture",
                  header: "Architecture",
                },
                {
                  accessorKey: "Os",
                  header: "Os",
                },
                {
                  accessorKey: "DiskUsage.total_bytes",
                  header: "Total",
                  cell: ({ row }) => (
                    <DockerDiskMetric
                      status={row.original.DiskUsage?.status}
                      bytes={row.original.DiskUsage?.total_bytes}
                      measuredAt={row.original.DiskUsage?.measured_at}
                      unavailableReason={
                        row.original.DiskUsage?.unavailable_reason
                      }
                    />
                  ),
                },
                {
                  accessorKey: "DiskUsage.shared_bytes",
                  header: "Shared",
                  cell: ({ row }) => (
                    <DockerDiskMetric
                      status={row.original.DiskUsage?.status}
                      bytes={row.original.DiskUsage?.shared_bytes}
                      measuredAt={row.original.DiskUsage?.measured_at}
                      unavailableReason={
                        row.original.DiskUsage?.unavailable_reason
                      }
                    />
                  ),
                },
                {
                  accessorKey: "DiskUsage.unique_bytes",
                  header: "Unique (approximately reclaimable)",
                  cell: ({ row }) => (
                    <DockerDiskMetric
                      status={row.original.DiskUsage?.status}
                      bytes={row.original.DiskUsage?.unique_bytes}
                      measuredAt={row.original.DiskUsage?.measured_at}
                      unavailableReason={
                        row.original.DiskUsage?.unavailable_reason
                      }
                      approximate
                    />
                  ),
                },
              ]}
            />
          </Section>

          {history && history.length > 0 && (
            <Section
              title="History"
              icon={<ICONS.History size="1.3rem" />}
              titleRight={
                <Box pl="md">
                  <ShowHideButton show={showHistory} setShow={setShowHistory} />
                </Box>
              }
            >
              {showHistory && (
                <DataTable
                  tableKey="image-history"
                  data={history.toReversed()}
                  columns={[
                    {
                      accessorKey: "CreatedBy",
                      header: "Created By",
                    },
                    {
                      accessorKey: "Created",
                      header: "Timestamp",
                      cell: ({ row }) =>
                        fmtDateWithMinutes(
                          new Date(row.original.Created * 1000),
                        ),
                    },
                  ]}
                />
              )}
            </Section>
          )}

          {specific.includes(Types.SpecificPermission.Inspect) && (
            <InspectSection json={image} showToggle />
          )}

          <DockerLabelsSection labels={image?.Config?.Labels} />
        </ResourceSubPage>
      )}
    </PageGuard>
  );
}
