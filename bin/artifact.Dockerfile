# syntax=docker/dockerfile:1@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32

FROM scratch

COPY core /core
COPY periphery /periphery
COPY km /km

LABEL org.opencontainers.image.source="https://github.com/neurekadev/komodo"
LABEL org.opencontainers.image.description="Komodo Binaries"
LABEL org.opencontainers.image.licenses="GPL-3.0-or-later"
