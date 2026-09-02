FROM rust:1.97.1-trixie AS builder
ENV CARGO_HTTP_TIMEOUT=600 \
  CARGO_NET_RETRY=10
RUN cargo install cargo-strip

WORKDIR /builder
COPY Cargo.toml Cargo.lock ./
COPY ./lib ./lib
COPY ./client/core/rs ./client/core/rs
COPY ./client/periphery ./client/periphery
COPY ./bin/cli ./bin/cli
COPY ./xtask ./xtask

ARG GIT_TAG=dev
ARG GIT_HASH=unknown

# Compile bin
RUN cargo build -p komodo_cli --release && cargo strip

# Copy binaries to distroless base
FROM gcr.io/distroless/cc

COPY --from=builder /builder/target/release/km /usr/local/bin/km

ARG GIT_TAG=dev
ARG GIT_HASH=unknown
ENV GIT_TAG=$GIT_TAG \
  GIT_HASH=$GIT_HASH

ENV KOMODO_CLI_CONFIG_PATHS="/config"

CMD [ "km" ]

LABEL org.opencontainers.image.source="https://github.com/moghtech/komodo"
LABEL org.opencontainers.image.description="Komodo CLI"
LABEL org.opencontainers.image.licenses="GPL-3.0"