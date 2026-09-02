## All in one, multi stage compile + runtime Docker build for your architecture.

# Build Core dependencies independently from application source.
FROM lukemathwalker/cargo-chef:0.1.78-rust-1.97.1-trixie@sha256:6dce65df3d7430c797e94348b4cf36d8d5876b63ca54f35dbfd37a97c42d0add AS chef
ENV CARGO_HTTP_TIMEOUT=600 \
  CARGO_NET_RETRY=10
WORKDIR /builder

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY ./lib ./lib
COPY ./client/core/rs ./client/core/rs
COPY ./client/periphery/rs ./client/periphery/rs
COPY ./bin/core ./bin/core
COPY ./bin/cli ./bin/cli
COPY ./xtask ./xtask
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS core-builder
COPY --from=planner /builder/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

COPY Cargo.toml Cargo.lock ./
COPY ./lib ./lib
COPY ./client/core/rs ./client/core/rs
COPY ./client/periphery/rs ./client/periphery/rs
COPY ./bin/core ./bin/core
COPY ./bin/cli ./bin/cli
COPY ./xtask ./xtask

ARG GIT_TAG=dev
ARG GIT_HASH=unknown

# Compile app, retain only final binaries in this source-sensitive layer.
RUN cargo build -p komodo_core --release && \
  cargo build -p komodo_cli --release && \
  mkdir -p /out && \
  cp target/release/core target/release/km /out/ && \
  strip /out/core /out/km && \
  rm -rf target

# Build UI
FROM node:22.12-bookworm-slim@sha256:35531c52ce27b6575d69755c73e65d4468dba93a25644eed56dc12879cae9213 AS client-builder
WORKDIR /builder/client
COPY ./client/core/ts/package.json ./client/core/ts/yarn.lock ./
RUN yarn install --frozen-lockfile --network-timeout 600000
COPY ./client/core/ts ./
RUN yarn build

FROM node:22.12-bookworm-slim@sha256:35531c52ce27b6575d69755c73e65d4468dba93a25644eed56dc12879cae9213 AS ui-builder
WORKDIR /builder/ui
COPY ./ui/package.json ./ui/yarn.lock ./
RUN yarn install --frozen-lockfile --network-timeout 600000
COPY ./ui ./
COPY --from=client-builder /builder/client /builder/client
RUN cd /builder/client && yarn link && \
  cd /builder/ui && yarn link komodo_client && yarn build

# Final Image
FROM debian:trixie-slim

COPY ./bin/core/starship.toml /starship.toml
COPY ./bin/core/debian-deps.sh .
RUN sh ./debian-deps.sh && rm ./debian-deps.sh

# Setup an application directory
WORKDIR /app

# Copy
COPY ./config/core.config.toml /config/.default.config.toml
COPY --from=ui-builder /builder/ui/dist /app/ui
COPY --from=core-builder /out/core /usr/local/bin/core
COPY --from=core-builder /out/km /usr/local/bin/km
COPY --from=denoland/deno:bin /deno /usr/local/bin/deno

# Set $DENO_DIR and preload external Deno deps
ENV DENO_DIR=/action-cache/deno
RUN mkdir /action-cache && \
  cd /action-cache && \
  deno install jsr:@std/yaml jsr:@std/toml

COPY ./bin/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ARG GIT_TAG=dev
ARG GIT_HASH=unknown
ENV GIT_TAG=$GIT_TAG \
  GIT_HASH=$GIT_HASH

# Hint at the port
EXPOSE 9120

ENV KOMODO_CLI_CONFIG_PATHS="/config"
# This ensures any `komodo.cli.*` takes precedence over the Core `/config/*config.*`
ENV KOMODO_CLI_CONFIG_KEYWORDS="*config.*,*komodo.cli*.*"

ENTRYPOINT [ "entrypoint.sh" ]
CMD [ "core" ]

# Label to prevent Komodo from stopping with StopAllContainers
LABEL komodo.skip="true"
# Label for Ghcr
LABEL org.opencontainers.image.source="https://github.com/moghtech/komodo"
LABEL org.opencontainers.image.description="Komodo Core"
LABEL org.opencontainers.image.licenses="GPL-3.0"
