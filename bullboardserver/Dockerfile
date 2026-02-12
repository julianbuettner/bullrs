FROM docker.io/oven/bun:1 AS base
WORKDIR /usr/src/app

FROM base AS install
RUN mkdir -p /install
COPY package.json bun.lock /install/
RUN cd /install && bun install --frozen-lockfile --production

FROM base
RUN mkdir -p /app
COPY --from=install /install/node_modules node_modules
COPY index.ts index.ts

USER bun
EXPOSE 3000
ENTRYPOINT ["bun", "run", "index.ts"]

