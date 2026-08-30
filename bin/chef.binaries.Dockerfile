# syntax=docker/dockerfile:1@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32

## Builds the Komodo Core, Periphery, and CLI binaries once for a native
## architecture. The scratch output is passed to each runtime image by digest.

FROM lukemathwalker/cargo-chef:0.1.78-rust-1.97.1-bookworm@sha256:63489cf2f47e819b82f9bcb97787b18a186d4381e4f112432246cf31e179206f AS chef
ENV CARGO_HTTP_TIMEOUT=600 \
  CARGO_NET_RETRY=10
WORKDIR /builder

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /builder/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN \
  cargo build --locked --release --bin core --bin periphery --bin km

FROM scratch

COPY --from=builder /builder/target/release/core /core
COPY --from=builder /builder/target/release/periphery /periphery
COPY --from=builder /builder/target/release/km /km

LABEL org.opencontainers.image.source="https://github.com/neurekadev/komodo"
LABEL org.opencontainers.image.description="Komodo Binaries"
LABEL org.opencontainers.image.licenses="GPL-3.0-or-later"
