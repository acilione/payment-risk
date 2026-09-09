# Pulse payment risk observatory

The [public demo](https://acilione.github.io/payment-risk/) is a static React application containing explicitly labeled synthetic sample data. It requires no infrastructure and publishes no credentials, internal service addresses, or captured live data.

The local website at http://localhost:23001 serves the same interface with a Fastify API reading ClickHouse, Flink REST, and Prometheus. Overview, transaction search/filtering, decision inspection, baseline rule descriptions, and the architecture walkthrough work in both modes. The window selector applies to decision finalization time; the transaction list loads the latest 100 decisions in that window. Times use the browser's timezone. Amounts are EUR integer cents, displayed with two decimal places.

## Run

`make up` includes the website. To build/start only the website against an existing stack:

```bash
make website
```

For frontend development, use Node 24 LTS (see `web/.nvmrc`):

```bash
npm --prefix web ci
npm --prefix web run dev
```

Vite serves port 23001 and proxies `/api` to port 23002. Stop the Compose website to free the port first. In another terminal, run the API with the generated credentials loaded by Node directly from the ignored environment file:

```bash
node --env-file=.env web/server/index.mjs
```

For a standalone demo build:

```bash
VITE_DEMO_MODE=true npm --prefix web run build
npm --prefix web run preview
```

## Boundaries and operational behavior

- Live data refreshes every 15 seconds. The API deduplicates concurrent requests and caches each of three allowed windows for five seconds. ClickHouse queries use the `FINAL` business view, fixed SQL, read-only settings, four-second execution limits, and five-second upstream request deadlines.
- Failed analytics requests return 503. The interface explicitly marks a previously successful snapshot as stale; it never substitutes illustrative data into live mode. Failed health probes remain unavailable while successful analytics can still render.
- Kafka status comes from Flink source lag telemetry, not a direct broker probe. Flink availability requires the named running job; checkpoints must have completed within three minutes. These signals are presentation indicators, not a replacement for the provisioned Prometheus alerts.
- The risk rules screen documents baseline defaults, not the currently active broadcast state. Inspectors show the fingerprint recorded by each decision.
- Credentials stay in the API process. No `VITE_*` variable may contain a secret: Vite embeds these in public JavaScript. The server returns generic upstream errors and restrictive security headers, limits request size and rate, and runs as a non-root user with a read-only container filesystem. The Compose port binds only to localhost.
- This local operational API is intentionally unauthenticated. Before hosting live data for other users, add authenticated access and authorization at a trusted gateway, provision a separate least-privilege ClickHouse identity with SELECT access only, and use TLS for all upstream connections. The public Pages build has no operational API.

## Verification and updates

`make website-check` runs backend boundary tests, TypeScript checking, and a production build. `scripts/verify-website.py` uses Playwright to verify live readings (unless `--static`), demo navigation, filters, search, dialogs, keyboard dismissal, and mobile overflow. CI builds the static demo, runs these browser checks, and deploys it through GitHub Pages. A separate job builds and scans the operational website image for fixable HIGH/CRITICAL vulnerabilities. Screenshots are labeled demo evidence.

Dependencies are locked in `web/package-lock.json`; Dependabot covers npm and the container. The runtime image is pinned to the latest published Node 24 LTS Docker release verified during implementation, 24.20.0, including its immutable manifest digest. Node 24.21.0 had been released upstream but its official container tag was not yet published. Release sources: [Node](https://github.com/nodejs/node/releases), [official container manifest](https://github.com/docker-library/official-images/blob/master/library/node), [React](https://react.dev/versions), [Vite](https://vite.dev/guide/).
