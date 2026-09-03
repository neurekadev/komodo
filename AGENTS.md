# Documentation requirements

Komodo documentation is part of the product. Every change that affects a user-visible feature, configuration option, security boundary, deployment workflow, or migration must update `docs/` in the same change.

## Write for people using and self-hosting Komodo

- Keep each page focused on one task or a small set of closely related tasks. Start with what the reader can accomplish and why.
- Include only the prerequisites, numbered UI or command steps, expected result, and troubleshooting needed to complete the task. Use current Komodo labels and verified commands.
- Explain a setting only when it helps the reader make a choice or complete an action. Keep consequential warnings short and beside the relevant action.
- Split distinct workflows into linked pages. Do not append implementation narratives, internal algorithms, exhaustive edge cases, or change history to user guides.
- Keep contributor setup and implementation material outside the published user guide, in CONTRIBUTING.md or source documentation.
- Use editorial judgment without a word-count limit. Before finishing, remove unnecessary detail and duplication from every changed page; shortening a page must not remove instructions needed to use the feature safely.

## Keep the documentation organized

- Keep **Quick Start** and its landing page first in navigation, providing the shortest supported path to install Core, connect a server, and deploy a first stack.
- Put user-facing capabilities, including running backups and restoring workloads, in **Features**.
- Put installation, configuration, authentication, permissions, backup setup, and disaster-recovery tasks in **Administration**.
- Put practical resource-model, CLI, API, client-library, and integration guidance in **Reference**.
- Use navigation groups instead of category landing pages that repeat the sidebar. Quick Start is the intentional exception; retain the site homepage too.
- List each destination once in documentation navigation. Keep the top **Screenshots** link and do not add a second gallery entry at the bottom.
- Keep product images in **Screenshots** and follow the filename and metadata convention documented for contributors.
- Keep actionable upgrade instructions in **Migrations**. Host release notes only in GitHub Releases; do not add release-note or inline-changelog pages.
- Komodo 3.0.0+ is a divergent hard fork. Do not offer upstream migration guides or promise upstream backward compatibility, including from `v2.3.2`. Cherry-picking upstream fixes does not change this policy. Any future fork-to-fork upgrade guide must begin with a backup and include verification and rollback steps.

## Keep examples synchronized

- Keep complete production Compose and environment examples inline in the MongoDB, FerretDB, and standalone Periphery deployment guides. Those examples are canonical; link to them instead of maintaining duplicate templates or download files.
- Validate the exact deployment examples users copy. Keep them synchronized with configuration, image startup behavior, generated schemas, and current UI labels.
- Update relevant task instructions whenever product behavior or an example changes.
- Use root-relative links for this site. It is served only at `/`; do not introduce `/docs` base paths, compatibility redirects, `DOCSITE_URL`, `DOCSITE_BASE_URL`, a CNAME, or GitHub Pages deployment configuration.

## Verify documentation changes

From `docs/`, run the Yarn scripts for type checking, linting, production build, internal-link and anchor validation, and static-output validation. When container behavior changes, also build the documentation image and verify the homepage, assets, and a direct deep link on port 80.
