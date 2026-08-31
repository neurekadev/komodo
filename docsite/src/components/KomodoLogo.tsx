import useBaseUrl from "@docusaurus/useBaseUrl";
import type { CSSProperties } from "react";

export default function KomodoLogo({
  width = "4rem",
}: {
  width?: CSSProperties["width"];
}) {
  return (
    <img
      style={{ width, maxWidth: "100%", height: "auto" }}
      src={useBaseUrl("img/logo.png")}
      alt="Komodo"
    />
  );
}
