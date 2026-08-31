import clsx from "clsx";
import Link from "@docusaurus/Link";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import Layout from "@theme/Layout";
import HomepageFeatures from "@site/src/components/HomepageFeatures";
import styles from "./index.module.css";
import KomodoLogo from "../components/KomodoLogo";
import { JSX } from "react";

function HomepageHeader() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <header className={clsx("hero hero--primary", styles.heroBanner)}>
      <div className="container">
        <div className={styles.heroBrand}>
          <KomodoLogo width="min(20rem, 72vw)" />
          <h1 className="hero__title">Komodo</h1>
        </div>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className={styles.buttons}>
          <Link
            className="button button--secondary button--lg"
            to="/docs/intro"
          >
            Docs
          </Link>
          <Link
            className="button button--secondary button--lg"
            to="https://github.com/neurekadev/komodo"
          >
            GitHub
          </Link>
          <Link
            className={"button button--secondary button--lg " + styles["mobile-full-grid"]}
            to="https://github.com/neurekadev/komodo#screenshots"
          >
            Screenshots
          </Link>
        </div>
      </div>
    </header>
  );
}

export default function Home(): JSX.Element {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout title="Home" description={siteConfig.tagline}>
      <HomepageHeader />
      <main>
        <div className={styles.upgradeBanner}>
          <div className="container">
            Running <b>Komodo v1</b>? See the{" "}
            <Link to="/docs/releases/v2.0.0#upgrading-to-komodo-v2">
              v2 upgrade guide
            </Link>
            .
          </div>
        </div>
        <HomepageFeatures />
      </main>
    </Layout>
  );
}
