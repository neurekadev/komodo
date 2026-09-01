import { ReactNode } from "react";
import { useServerDockerSearch } from ".";
import { useDockerSelectionState, useRead } from "@/lib/hooks";
import DockerBatchExecutions from "@/components/docker/batch-executions";
import { filterBySplit } from "mogh_ui";
import { Section } from "mogh_ui";
import { Badge, Group } from "@mantine/core";
import { Prune } from "../executions";
import { DataTable, SortableHeader } from "mogh_ui";
import DockerResourceLink from "@/components/docker/link";
import { SearchInput } from "mogh_ui";
import { DockerDiskMetric } from "@/components/docker/metrics";

export default function ServerImages({
  id,
  titleOther,
}: {
  id: string;
  titleOther: ReactNode;
}) {
  const [search, setSearch] = useServerDockerSearch();
  const selectionState = useDockerSelectionState("Image");
  const images =
    useRead("ListImages", { server: id }, { refetchInterval: 10_000 }).data ??
    [];

  const allInUse = images.every((image) => image.in_use);

  const filtered = filterBySplit(images, search, (image) => image.name);

  return (
    <Section titleOther={titleOther}>
      <Group justify="space-between">
        <Group>
          <DockerBatchExecutions type="Image" />
          {!allInUse && <Prune serverId={id} type="Images" />}
        </Group>

        <SearchInput value={search} onSearch={setSearch} />
      </Group>

      <DataTable
        mih="60vh"
        tableKey="server-images"
        data={filtered}
        selectOptions={{
          selectKey: ({ name }) => `${id} ${name}`,
          state: selectionState,
        }}
        columns={[
          {
            accessorKey: "name",
            header: ({ column }) => (
              <SortableHeader column={column} title="Name" />
            ),
            cell: ({ row }) => (
              <DockerResourceLink
                type="Image"
                serverId={id}
                name={row.original.name}
                id={row.original.id}
                extra={
                  !row.original.in_use && <Badge color="red">Unused</Badge>
                }
              />
            ),
          },
          {
            accessorKey: "id",
            header: ({ column }) => (
              <SortableHeader column={column} title="ID" />
            ),
          },
          {
            accessorKey: "disk_usage.total_bytes",
            header: ({ column }) => (
              <SortableHeader column={column} title="Total" />
            ),
            cell: ({ row }) => (
              <DockerDiskMetric
                status={row.original.disk_usage?.status}
                bytes={row.original.disk_usage?.total_bytes}
                measuredAt={row.original.disk_usage?.measured_at}
                unavailableReason={row.original.disk_usage?.unavailable_reason}
              />
            ),
          },
          {
            accessorKey: "disk_usage.shared_bytes",
            header: ({ column }) => (
              <SortableHeader column={column} title="Shared" />
            ),
            cell: ({ row }) => (
              <DockerDiskMetric
                status={row.original.disk_usage?.status}
                bytes={row.original.disk_usage?.shared_bytes}
                measuredAt={row.original.disk_usage?.measured_at}
                unavailableReason={row.original.disk_usage?.unavailable_reason}
              />
            ),
          },
          {
            accessorKey: "disk_usage.unique_bytes",
            header: ({ column }) => (
              <SortableHeader column={column} title="Unique (approx.)" />
            ),
            cell: ({ row }) => (
              <DockerDiskMetric
                status={row.original.disk_usage?.status}
                bytes={row.original.disk_usage?.unique_bytes}
                measuredAt={row.original.disk_usage?.measured_at}
                unavailableReason={row.original.disk_usage?.unavailable_reason}
                approximate
              />
            ),
          },
        ]}
      />
    </Section>
  );
}
