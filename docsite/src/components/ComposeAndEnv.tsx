import React from "react";
import RemoteCodeFile from "./RemoteCodeFile";
import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

export default function ComposeAndEnv({
  file_name,
}: {
  file_name: string;
}) {
  return (
    <Tabs>
      <TabItem value="compose.yaml" label="compose.yaml">
        <RemoteCodeFile
          title={`https://github.com/neurekadev/komodo/blob/main/compose/${file_name}`}
          url={`https://raw.githubusercontent.com/neurekadev/komodo/main/compose/${file_name}`}
          language="yaml"
        />
      </TabItem>
      <TabItem value=".env" label=".env">
        <RemoteCodeFile
          title="https://github.com/neurekadev/komodo/blob/main/compose/compose.env"
          url="https://raw.githubusercontent.com/neurekadev/komodo/main/compose/compose.env"
          language="bash"
        />
      </TabItem>
    </Tabs>
  );
}
