import { useNavigate } from "react-router-dom";
import { UsableResource } from ".";
import { usePermissions, useRead, useWrite } from "@/lib/hooks";
import { usableResourcePath } from "@/lib/utils";
import { ConfirmModal } from "mogh_ui";
import { ICONS } from "@/lib/icons";
import { Alert, Checkbox, Stack, Text } from "@mantine/core";
import { useState } from "react";

export default function DeleteResource({
  type,
  id,
}: {
  type: UsableResource;
  id: string;
}) {
  const nav = useNavigate();
  const [removeVolumes, setRemoveVolumes] = useState(false);
  const key = type === "ResourceSync" ? "sync" : type.toLowerCase();
  const { canWrite } = usePermissions({ type, id });
  const resource = useRead(`Get${type}`, {
    [key]: id,
  } as any).data;
  const { mutateAsync, isPending } = useWrite(`Delete${type}`, {
    onSuccess: () => nav(`/${usableResourcePath(type)}`),
  });

  if (!resource || !canWrite) return null;

  return (
    <ConfirmModal
      title={
        <>
          Confirm <b>Delete</b>
        </>
      }
      confirmButtonContent="Delete"
      icon={<ICONS.Delete size="1rem" />}
      targetNoIcon
      targetProps={{ w: "fit", px: "xs" }}
      confirmText={resource.name}
      topAdditonal={
        type === "Stack" ? (
          <Alert color="red" title="Stack files will be deleted">
            <Stack gap="xs">
              <Text size="sm">
                The entire Komodo-owned stack directory and every file in it
                will be permanently deleted with this stack.
              </Text>
              <Text size="sm">
                Linked repository contents are retained. Volumes are retained
                unless you explicitly select the option below.
              </Text>
            </Stack>
          </Alert>
        ) : undefined
      }
      additional={
        type === "Stack" ? (
          <Checkbox
            color="red"
            checked={removeVolumes}
            onChange={(event) =>
              setRemoveVolumes(event.currentTarget.checked)
            }
            label="Remove stack-owned volumes"
            description="External Compose volumes and volumes without exact stack ownership labels are never removed."
          />
        ) : undefined
      }
      onExitTransitionEnd={() => setRemoveVolumes(false)}
      onConfirm={() =>
        mutateAsync(
          type === "Stack" ? { id, remove_volumes: removeVolumes } : { id },
        )
      }
      loading={isPending}
      confirmProps={{ variant: "filled", color: "red" }}
    >
      <ICONS.Delete size="1.3rem" />
    </ConfirmModal>
  );
}
