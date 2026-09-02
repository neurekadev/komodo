## All in one, multi stage compile + runtime Docker build for your architecture.

FROM rust:1.98.0-trixie AS builder
ENV CARGO_HTTP_TIMEOUT=600 \
  CARGO_NET_RETRY=10
RUN cargo install cargo-strip

WORKDIR /builder
COPY Cargo.toml Cargo.lock ./
COPY ./lib ./lib
COPY ./client/core/rs ./client/core/rs
COPY ./client/periphery ./client/periphery
COPY ./bin/periphery ./bin/periphery
COPY ./xtask ./xtask

ARG GIT_TAG=dev
ARG GIT_HASH=unknown

# Compile app
RUN cargo build -p komodo_periphery --release && cargo strip

# Final Image
FROM debian:trixie-slim

COPY ./bin/periphery/starship.toml /starship.toml
COPY ./bin/periphery/debian-deps.sh .
RUN sh ./debian-deps.sh && rm ./debian-deps.sh

COPY --from=builder /builder/target/release/periphery /usr/local/bin/periphery

COPY ./bin/entrypoint.sh /usr/local/bin/entrypoint.sh
COPY ./bin/compose-defaults.sh /app/bin/compose-defaults.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ARG GIT_TAG=dev
ARG GIT_HASH=unknown
ENV GIT_TAG=$GIT_TAG \
  GIT_HASH=$GIT_HASH

EXPOSE 8120

# Can mount config file to /config/*config*.toml and it will be picked up.
ENV PERIPHERY_CONFIG_PATHS="/config"
# The entrypoint retains /config/keys for ordinary Periphery invocations and
# selects /data/keys only for the explicit periphery-compose command.

ENTRYPOINT [ "entrypoint.sh" ]
CMD [ "periphery" ]

# Label to prevent Komodo from stopping with StopAllContainers
LABEL komodo.skip="true"
# Label for ghcr
LABEL org.opencontainers.image.source="https://github.com/moghtech/komodo"
LABEL org.opencontainers.image.description="Komodo Periphery"
LABEL org.opencontainers.image.licenses="GPL-3.0"
