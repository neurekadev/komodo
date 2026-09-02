import { Text, Tooltip } from "@mantine/core";
import { LoaderCircle } from "lucide-react";
import { fmtDateWithMinutes, fmtRateBytes, fmtSizeBytes } from "mogh_ui";
import classes from "./metrics.module.scss";

type MetricStatus = "pending" | "available" | "unavailable";

type DiskMetricProps = {
  status?: MetricStatus;
  bytes?: number;
  measuredAt?: number;
  unavailableReason?: string;
  approximate?: boolean;
};

function MetricSpinner() {
  return (
    <LoaderCircle
      aria-label="Measuring"
      className={classes.spinner}
      role="status"
      size="1rem"
    />
  );
}

export function DockerDiskMetric({
  status = "pending",
  bytes,
  measuredAt,
  unavailableReason,
  approximate,
}: DiskMetricProps) {
  if (status === "pending") {
    return <MetricSpinner />;
  }
  if (status !== "available" || bytes === undefined) {
    return (
      <Text c="dimmed">
        Unavailable
        {unavailableReason ? ` — ${unavailableReason}` : ""}
      </Text>
    );
  }
  const measured = measuredAt
    ? fmtDateWithMinutes(new Date(measuredAt))
    : "Unknown time";
  return (
    <Tooltip
      label={`Measured ${measured}${
        approximate ? ". Approximately reclaimable." : "."
      }`}
      openDelay={300}
    >
      <Text span>{fmtSizeBytes(bytes)}</Text>
    </Tooltip>
  );
}

type NetworkMetricProps = {
  traffic?: {
    status: MetricStatus;
    measured_at?: number;
    unavailable_reason?: string;
    rate_status: MetricStatus;
    rate_unavailable_reason?: string;
    ingress_bytes?: number;
    egress_bytes?: number;
    ingress_bytes_per_second?: number;
    egress_bytes_per_second?: number;
  };
  direction: "ingress" | "egress";
};

export function DockerNetworkMetric({
  traffic,
  direction,
}: NetworkMetricProps) {
  if (!traffic) {
    return <MetricSpinner />;
  }
  if (traffic.status === "pending") {
    return <MetricSpinner />;
  }
  if (traffic.status !== "available") {
    return (
      <Text c="dimmed">
        Unavailable
        {traffic.unavailable_reason
          ? ` — ${traffic.unavailable_reason}`
          : ""}
      </Text>
    );
  }

  const bytes =
    direction === "ingress" ? traffic.ingress_bytes : traffic.egress_bytes;
  const rate =
    direction === "ingress"
      ? traffic.ingress_bytes_per_second
      : traffic.egress_bytes_per_second;
  const measured = traffic.measured_at
    ? fmtDateWithMinutes(new Date(traffic.measured_at))
    : "Unknown time";

  return (
    <Tooltip label={`Measured ${measured}.`} openDelay={300}>
      <div>
        <Text>{bytes === undefined ? "Unavailable" : fmtSizeBytes(bytes)}</Text>
        {traffic.rate_status === "available" && rate !== undefined ? (
          <Text size="xs" c="dimmed">
            {fmtRateBytes(rate)}
          </Text>
        ) : (
          <Text size="xs" c="dimmed">
            Rate unavailable
            {traffic.rate_unavailable_reason
              ? ` — ${traffic.rate_unavailable_reason}`
              : ""}
          </Text>
        )}
      </div>
    </Tooltip>
  );
}
