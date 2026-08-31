import { LoginPage } from "mogh_ui";
import { useUserInvalidate } from "@/lib/hooks";

export default function Login(props: {
  passkeyIsPending?: boolean;
  totpIsPending?: boolean;
}) {
  const userInvalidate = useUserInvalidate();
  return (
    <LoginPage
      {...props}
      appName="KOMODO"
      iconLink="/logo-512x512.png"
      iconLinkAlt="Komodo"
      exampleConfigLink="https://github.com/moghtech/komodo/blob/main/config/core.config.toml"
      onLogin={userInvalidate}
    />
  );
}
