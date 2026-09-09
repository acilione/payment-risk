FROM node:24.20.0-alpine@sha256:e67514e5d0f6c46656005e1b693b2ec9d52e80b641307de684d4a015ba7a4eaf AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM node:24.20.0-alpine@sha256:e67514e5d0f6c46656005e1b693b2ec9d52e80b641307de684d4a015ba7a4eaf AS runtime
ENV NODE_ENV=production HOST=0.0.0.0 PORT=23002
WORKDIR /app
COPY package.json package-lock.json ./
RUN apk upgrade --no-cache libcrypto3 libssl3 \
    && npm ci --omit=dev && npm cache clean --force \
    && rm -rf /usr/local/lib/node_modules/npm /usr/local/bin/npm /usr/local/bin/npx /opt/yarn-* /usr/local/bin/yarn /usr/local/bin/yarnpkg
COPY --from=build /app/dist ./dist
COPY server ./server
USER node
EXPOSE 23002
CMD ["node", "server/index.mjs"]
