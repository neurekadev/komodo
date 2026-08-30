# syntax=docker/dockerfile:1@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32

FROM node:22.12.0-bookworm-slim@sha256:35531c52ce27b6575d69755c73e65d4468dba93a25644eed56dc12879cae9213 AS builder

WORKDIR /builder

# Optionally specify a specific Komodo host.
ARG VITE_KOMODO_HOST=""
ENV VITE_KOMODO_HOST=$VITE_KOMODO_HOST

COPY ./client/core/ts/package.json ./client/core/ts/yarn.lock ./client/
COPY ./ui/package.json ./ui/yarn.lock ./ui/
RUN --mount=type=cache,target=/usr/local/share/.cache/yarn,sharing=locked \
  cd client && yarn install --frozen-lockfile --non-interactive --network-timeout 600000
RUN --mount=type=cache,target=/usr/local/share/.cache/yarn,sharing=locked \
  cd ui && yarn install --frozen-lockfile --non-interactive --network-timeout 600000

COPY ./client/core/ts ./client
RUN cd client && yarn build && yarn link

COPY ./ui ./ui
RUN cd ui && yarn link komodo_client && yarn build

FROM scratch

COPY --from=builder /builder/ui/dist /ui

LABEL org.opencontainers.image.source="https://github.com/neurekadev/komodo"
LABEL org.opencontainers.image.description="Komodo UI"
LABEL org.opencontainers.image.licenses="GPL-3.0-or-later"
