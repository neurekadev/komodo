FROM node:22.23-bookworm-slim AS builder

WORKDIR /app

COPY package.json yarn.lock ./
RUN yarn install --frozen-lockfile --non-interactive

COPY . .
RUN yarn typecheck \
    && yarn lint \
    && yarn check:links \
    && yarn build \
    && yarn check:static

FROM caddy:2.11.4 AS caddy-binary
FROM debian:bookworm-20260824-slim AS runtime

ENV XDG_CONFIG_HOME=/config \
    XDG_DATA_HOME=/data

RUN mkdir -p /config /data /srv \
    && chown -R 65532:65532 /config /data /srv

COPY --from=caddy-binary /usr/bin/caddy /usr/bin/caddy
COPY Caddyfile /etc/caddy/Caddyfile
COPY --from=builder --chown=65532:65532 /app/out /srv

EXPOSE 80

ENTRYPOINT ["caddy"]
CMD ["run", "--config", "/etc/caddy/Caddyfile", "--adapter", "caddyfile"]
