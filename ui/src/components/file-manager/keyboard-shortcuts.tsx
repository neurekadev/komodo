import {
  ActionIcon,
  Divider,
  Group,
  Kbd,
  Modal,
  SimpleGrid,
  Text,
} from "@mantine/core";
import { useDisclosure } from "@mantine/hooks";
import { Keyboard } from "lucide-react";

export default function FileManagerKeyboardShortcuts() {
  const [opened, { open, close }] = useDisclosure();

  return (
    <>
      <Modal
        opened={opened}
        onClose={close}
        title={<Text size="xl">File Manager Shortcuts</Text>}
        size="lg"
      >
        <SimpleGrid cols={2} p="md" bg="accent.1">
          <Shortcut label="Select all" keys={["Ctrl/Cmd", "A"]} />
          <Shortcut label="Copy" keys={["Ctrl/Cmd", "C"]} />
          <Shortcut label="Cut" keys={["Ctrl/Cmd", "X"]} />
          <Shortcut label="Paste" keys={["Ctrl/Cmd", "V"]} />
          <Shortcut label="Undo" keys={["Ctrl/Cmd", "Z"]} divider={false} />
          <Shortcut label="Redo" keys={["Ctrl/Cmd", "Shift", "Z"]} divider={false} />
          <Shortcut label="Move selection" keys={["Arrow Up/Down"]} />
          <Shortcut label="Open parent" keys={["Arrow Left"]} />
          <Shortcut label="Open selected" keys={["Arrow Right", "Enter"]} />
          <Shortcut label="Delete" keys={["Delete"]} />
          <Shortcut label="Delete permanently" keys={["Shift", "Delete"]} />
          <Shortcut label="Clear selection / close editor" keys={["Escape"]} divider={false} />
        </SimpleGrid>
      </Modal>

      <ActionIcon
        variant="subtle"
        aria-label="Keyboard shortcuts"
        onClick={open}
      >
        <Keyboard size={17} />
      </ActionIcon>
    </>
  );
}

function Shortcut({
  label,
  keys,
  divider = true,
}: {
  label: string;
  keys: string[];
  divider?: boolean;
}) {
  return (
    <>
      <Text>{label}</Text>
      <Group gap="xs">
        {keys.map((key) => (
          <Kbd key={key}>{key}</Kbd>
        ))}
      </Group>

      {divider && <Divider style={{ gridColumn: "1 / -1" }} />}
    </>
  );
}
