import { UsableResource } from ".";
import {
  EntityHeader,
  EntityHeaderProps,
  EntityPageProps,
} from "mogh_ui";
import { ReactNode } from "react";
import { Button, Group, Stack, Text } from "@mantine/core";
import { DividedChildren } from "mogh_ui";
import ResourceLink from "./link";
import ResourceDescription from "./description";
import { usableResourcePath } from "@/lib/utils";
import ResourceUpdates from "@/components/updates/resource";
import { usePermissions } from "@/lib/hooks";
import { Section } from "mogh_ui";
import { ICONS } from "@/lib/icons";
import { useHistoryAwareBack } from "@/lib/navigation";

export interface ResourceSubPageProps extends EntityHeaderProps {
  parentType: UsableResource;
  parentId: string;
  pageProps?: EntityPageProps;
  entityTypeName?: string;
  info?: ReactNode;
  executions?: ReactNode;
  children?: ReactNode;
}

export default function ResourceSubPage({
  parentType,
  parentId,
  pageProps,
  entityTypeName,
  info,
  executions,
  children,
  ...headerProps
}: ResourceSubPageProps) {
  const { canExecute } = usePermissions({ type: parentType, id: parentId });
  const { backTo, actions, ...stackProps } = pageProps ?? {};
  const fallback =
    backTo ?? `/${usableResourcePath(parentType)}/${parentId}`;
  const goBack = useHistoryAwareBack(fallback);
  const Header = (
    <Stack justify="space-between">
      <Stack gap="md" pb="md" className="bordered-light" bdrs="md">
        <EntityHeader {...headerProps} />
        <DividedChildren px="md">
          {entityTypeName && <Text>{entityTypeName}</Text>}
          <ResourceLink type={parentType} id={parentId} />
          {info}
        </DividedChildren>
      </Stack>
      <ResourceDescription type={parentType} id={parentId} />
    </Stack>
  );
  return (
    <Stack mb="50vh" {...stackProps}>
      <Group justify="space-between">
        <Button
          leftSection={<ICONS.Back size="1rem" />}
          onClick={goBack}
        >
          Back
        </Button>
        {actions && <Group wrap="nowrap">{actions}</Group>}
      </Group>
      <Stack hiddenFrom="lg" w="100%">
        {Header}
        <ResourceUpdates type={parentType} id={parentId} />
      </Stack>
      <Group
        visibleFrom="lg"
        gap="xl"
        w="100%"
        align="stretch"
        grow
        preventGrowOverflow={false}
      >
        {Header}
        <ResourceUpdates type={parentType} id={parentId} />
      </Group>

      <Stack gap="xl">
        {canExecute && executions && (
          <Section
            title="Execute"
            icon={<ICONS.Execution size="1.3rem" />}
            my="md"
          >
            <Group>{executions}</Group>
          </Section>
        )}

        {children}
      </Stack>
    </Stack>
  );
}
