# Documentation requirements

Komodo documentation is part of the product. Every change that affects a user-visible feature, configuration option, security boundary, deployment workflow, or migration must update `docs/` in the same change.

## Write for end users first

- Start with what the reader can accomplish and why they would do it.
- Put prerequisites immediately before the task that needs them.
- Give numbered UI or command steps using current Komodo labels and repository-provided commands.
- State the expected result, then cover common troubleshooting.
- Put advanced configuration, architecture, security, and recovery information after the end-user workflow. Do not make routine users read administrator internals before they can complete the task.
- Keep security warnings beside the action that creates the risk.
- Prefer short, task-based pages over broad inventories. Link to one canonical explanation instead of duplicating configuration details.

## Keep the documentation organized

- Keep **Quick Start** first in navigation and use it for the shortest supported path to install Core, connect a server, and deploy a first stack.
- Put user-facing capabilities in **Features**.
- Put operator-only configuration, authentication, permissions, backup, recovery, and trust-boundary material in **Administration**.
- Put the resource model, CLI, API and client libraries, integrations, and contributor information in **Reference**.
- Keep product images in **Screenshots** and use the documented filename and metadata convention.
- Keep actionable upgrade instructions in **Migrations**. Host release notes only in GitHub Releases; do not add release-note or inline-changelog pages.
- Do not promise migration from upstream Komodo versions newer than `v2.3.2`. Every migration guide must begin with a backup and include verification and rollback steps.

## Keep examples synchronized

- Copy commands and examples from the repository's Compose templates, configuration, generated schemas, and current UI labels whenever possible.
- Update the relevant documentation whenever those sources change.
- Use root-relative links for this site. It is served only at `/`; do not introduce `/docs` base paths, compatibility redirects, `DOCSITE_URL`, `DOCSITE_BASE_URL`, a CNAME, or GitHub Pages deployment configuration.

## Verify documentation changes

From `docs/`, run the Yarn scripts for type checking, linting, production build, internal-link and anchor validation, and static-output validation. When container behavior changes, also build the documentation image and verify the homepage, assets, and a direct deep link on port 80.
