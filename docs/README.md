# Komodo documentation image

This Fumadocs application is built as a static export and delivered only through the `komodo-docs` container image. Caddy serves the site from `/` on port 80; subpath hosting and legacy `/docs/...` routes are intentionally unsupported.

## Work locally

Node 22 and Yarn are required.

```bash
yarn install --frozen-lockfile
yarn dev
```

Before committing a documentation change, run:

```bash
yarn verify
```

That command type checks, lints, validates internal links and anchors, creates the production export, and validates root-relative static output.

## Build and run the image

From the repository root:

```bash
docker build -t komodo-docs -f docs/Dockerfile docs
docker run --rm -p 127.0.0.1:8080:80 komodo-docs
```

Open `http://127.0.0.1:8080/`, then verify a direct deep link such as `http://127.0.0.1:8080/quick-start`.

The minimal Compose example uses the published image:

```bash
docker compose -f docs/compose.example.yaml up -d
```

It binds only to loopback so an operator-managed reverse proxy can terminate HTTPS and forward to `127.0.0.1:8080`. Change the binding deliberately if the proxy runs on another host or Docker network.

## Container contract

- Image: `ghcr.io/neurekadev/komodo-docs`
- Architectures: `linux/amd64` and `linux/arm64`
- Listen address: container port 80
- Supported base path: `/` only
- Runtime: static files served by Caddy; no Node.js server or hosted search service
- Health/deep-link check: request `/`, an asset beneath `/_next/`, and a page such as `/features/stacks`

The Caddy configuration resolves clean routes to exported `.html` files, so direct browser requests do not depend on a client-side fallback.
