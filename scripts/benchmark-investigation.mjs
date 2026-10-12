// Isolated synthetic database. No inserts, resets or deletions in risk.*.
import { readFile, mkdir, writeFile } from "node:fs/promises";
import { randomUUID, createHash } from "node:crypto";
import { cpus, totalmem, platform } from "node:os";
import { execFileSync } from "node:child_process";
import { createInvestigationReader } from "../web/server/investigation.mjs";
import { publishAnalytics } from "../web/server/analytics-publisher.mjs";
import { createServingReader } from "../web/server/serving-reader.mjs";
import { createReader } from "../web/server/overview.mjs";
const config = {
  clickhouse: process.env.CLICKHOUSE_URL || "http://127.0.0.1:28123",
  user: process.env.CLICKHOUSE_USER || "risk",
  password: process.env.CLICKHOUSE_PASSWORD || "",
  flink: "http://127.0.0.1:28082",
  prometheus: "http://127.0.0.1:29090",
};
// Dataset size and concurrency are explicit; insertion remains bounded to 500 logical rows.
const stages = (process.env.RISK_QUERY_STAGES || "1000,10000,50000")
  .split(",")
  .map(Number);
const concurrencies = (process.env.RISK_QUERY_CONCURRENCY || "1,4")
  .split(",")
  .map(Number);
const rounds = Number(process.env.RISK_QUERY_ROUNDS || "3");
if (
  !stages.length ||
  stages.some(
    (n, i) =>
      !Number.isSafeInteger(n) ||
      n < 1000 ||
      n > 1000000 ||
      n % 1000 ||
      (i && n <= stages[i - 1]),
  )
)
  throw new Error(
    "RISK_QUERY_STAGES must be increasing multiples of 1000, at most 1000000",
  );
if (
  !concurrencies.length ||
  concurrencies.some((n) => !Number.isInteger(n) || n < 1 || n > 32) ||
  !Number.isInteger(rounds) ||
  rounds < 1 ||
  rounds > 100
)
  throw new Error("Query concurrency must be 1-32 and rounds 1-100");
const database = "risk_benchmark_" + randomUUID().replaceAll("-", "");
const samples = JSON.parse(
  await readFile(
    new URL("../web/src/investigation-fixture.json", import.meta.url),
  ),
).cases.flatMap((s) => s.timeline.map((t) => t.decision));
async function sql(query) {
  const response = await fetch(config.clickhouse, {
    method: "POST",
    headers: {
      "X-ClickHouse-User": config.user,
      "X-ClickHouse-Key": config.password,
    },
    body: query,
    signal: AbortSignal.timeout(60000),
  });
  if (!response.ok)
    throw new Error(
      `ClickHouse status ${response.status}: ${(await response.text()).slice(0, 200)}`,
    );
  return response.text();
}
const queryErrors = [];
const fetcher = async (url, options = {}) => {
  if (String(url).startsWith(config.clickhouse)) {
    const response = await fetch(url, {
      ...options,
      body: options.body.replaceAll("risk.", database + "."),
    });
    if (!response.ok && queryErrors.length < 8)
      queryErrors.push({
        status: response.status,
        detail: (await response.clone().text()).slice(0, 600),
      });
    return response;
  }
  return {
    ok: true,
    json: async () => ({ jobs: [], status: "success", data: { result: [] } }),
  };
};
const serving =
  process.env.RISK_QUERY_MODE === "snapshot"
    ? createServingReader(config, fetcher)
    : null;
const reader = serving || createInvestigationReader(config, fetcher);
const overview = createReader({ ...config, snapshot: serving }, fetcher);
const report = {
  sourceCommit: execFileSync("git", ["rev-parse", "HEAD"], {
    encoding: "utf8",
  }).trim(),
  generatedAt: new Date().toISOString(),
  environment: {
    platform: platform(),
    cpus: cpus().length,
    memoryBytes: totalmem(),
  },
  scope:
    "Exact production SQL in an isolated synthetic database; health probes stubbed; excludes Kafka and HTTP server routing",
  budgets: { requestP95Ms: 2000, errors: 0 },
  queryErrors,
  workload: {
    mode: serving ? "snapshot" : "direct",
    stages,
    concurrencies,
    rounds,
    customers: 1000,
    retryPercent: 10,
  },
  stages: [],
};
report.sourceDirty = Boolean(
  execFileSync("git", ["status", "--porcelain", "--untracked-files=normal"], {
    encoding: "utf8",
  }).trim(),
);
report.sourceHashes = Object.fromEntries(
  await Promise.all(
    [
      "infrastructure/docker/clickhouse.sql",
      "web/server/investigation.mjs",
      "web/server/overview.mjs",
      "scripts/benchmark-investigation.mjs",
      "web/server/analytics-publisher.mjs",
      "web/server/analytics-store.mjs",
      "web/server/serving-reader.mjs",
    ].map(async (path) => [
      path,
      createHash("sha256")
        .update(await readFile(path))
        .digest("hex"),
    ]),
  ),
);
const percentile = (a, p) =>
  [...a].sort((x, y) => x - y)[Math.ceil(a.length * p) - 1];
const eventBase = Date.now() - 60000;
let created = false;
try {
  await sql(`CREATE DATABASE ${database}`);
  created = true;
  const schema = await readFile(
    new URL("../infrastructure/docker/clickhouse.sql", import.meta.url),
    "utf8",
  );
  for (const statement of schema
    .replace(/--[^\n]*/g, "")
    .split(";")
    .map((s) => s.trim())
    .filter(Boolean)) {
    if (statement.startsWith("CREATE DATABASE")) continue;
    await sql(statement.replaceAll("risk.", database + "."));
  }
  let inserted = 0;
  for (const count of stages) {
    while (inserted < count) {
      const rows = [];
      for (let i = inserted; i < Math.min(inserted + 500, count); i++) {
        const source = samples[i % samples.length];
        const row = {
          ...source,
          decision_id: `risk_load_${i}`,
          evaluation_id: `eval_load_${i}`,
          event_id: `event_load_${i}`,
          transaction_id: `txn_load_${i}`,
          customer_id: `customer_${String(i % 1000).padStart(4, "0")}`,
          event_time: eventBase + Math.floor(i / 2000) * 1000,
          processed_at: Date.now(),
          source_topic: "risk.decisions",
          source_partition: i % 3,
          source_offset: i,
          rule_evidence: JSON.stringify(source.rule_evidence),
        };
        rows.push(row);
        if (i % 10 === 0) rows.push({ ...row, source_offset: count + i });
      }
      await sql(
        `INSERT INTO ${database}.evaluations FORMAT JSONEachRow\n` +
          rows.map((r) => JSON.stringify(r)).join("\n"),
      );
      inserted = Math.min(inserted + 500, count);
    }
    const physical = JSON.parse(
      await sql(`SELECT count() AS n FROM ${database}.evaluations FORMAT JSON`),
    ).data[0].n;
    const stage = {
      logicalPayments: count,
      physicalDeliveries: Number(physical),
      checks: [],
      measurements: [],
    };
    report.stages.push(stage);
    if (serving) {
      const started = performance.now();
      stage.publication = await publishAnalytics(config, fetcher);
      stage.publication.buildMs = performance.now() - started;
      if (
        stage.publication.assessments !== count ||
        stage.publication.deliveries !== Number(physical)
      )
        throw new Error("Publication coverage differs");
      stage.checks.push(
        "Complete immutable generation; exact logical count and delivery coverage",
      );
    }
    for (const [operation, run] of [
      ["customers", () => reader.customers({ q: "customer_" })],
      ["timeline", () => reader.timeline({ customer: "customer_0000" })],
      ["overview", () => overview("24h")],
    ]) {
      for (const concurrency of concurrencies) {
        const durations = [];
        let errors = 0;
        for (let round = 0; round < rounds; round++)
          await Promise.all(
            Array.from({ length: concurrency }, async () => {
              const start = performance.now();
              try {
                const result = await run();
                if (
                  operation === "overview" &&
                  result.totals.transactions !== count
                )
                  throw new Error("Duplicate counting");
                if (
                  operation === "customers" &&
                  (result.items.length !== 20 ||
                    result.items.some((c) => c.payments !== count / 1000))
                )
                  throw new Error("Customer counts differ");
                if (
                  operation === "timeline" &&
                  result.customer.payments !== count / 1000
                )
                  throw new Error("Timeline count differs");
                durations.push(performance.now() - start);
              } catch (error) {
                if (
                  /Duplicate counting|counts differ|count differs/.test(
                    error.message,
                  )
                )
                  throw error;
                errors++;
                if (queryErrors.length < 8)
                  queryErrors.push({
                    operation,
                    concurrency,
                    type: error.name,
                    message: error.message,
                  });
              }
            }),
          );
        stage.measurements.push({
          operation,
          concurrency,
          attempts: concurrency * rounds,
          samples: durations.length,
          p50Ms: durations.length ? percentile(durations, 0.5) : null,
          p95Ms: durations.length ? percentile(durations, 0.95) : null,
          maxMs: durations.length ? Math.max(...durations) : null,
          errors,
          withinBudget:
            errors === 0 &&
            percentile(durations, 0.95) <= report.budgets.requestP95Ms,
        });
      }
    }
    // Assert pagination coverage at a manageable scale; never use OFFSET or merge deliveries in JS.
    if (count === 10000) {
      const seen = new Set();
      let cursor = "";
      do {
        const page = await reader.customers({ q: "customer_", cursor });
        for (const c of page.items) {
          if (seen.has(c.customer_id))
            throw new Error("Repeated page customer");
          seen.add(c.customer_id);
        }
        cursor = page.nextCursor;
      } while (cursor);
      if (seen.size !== 1000) throw new Error("Customer pagination gap");
      stage.checks.push(
        "1000 customers across 50 pages, no duplicate or missing IDs",
      );
    }
    await mkdir("artifacts", { recursive: true });
    await writeFile(
      "artifacts/investigation-load.json",
      JSON.stringify(report, null, 2) + "\n",
    );
  }
  // All pages include tied timestamps in the synthetic customer. Check duplicate-free IDs.
  const ids = new Set();
  let cursor = "";
  do {
    const page = await reader.timeline({ customer: "customer_0000", cursor });
    for (const d of page.items) {
      if (ids.has(d.evaluation_id)) throw new Error("Repeated timeline ID");
      ids.add(d.evaluation_id);
    }
    cursor = page.nextCursor;
  } while (cursor);
  if (ids.size !== stages.at(-1) / 1000)
    throw new Error("Timeline pagination gap");
  report.stages
    .at(-1)
    .checks.push(
      `${ids.size} customer payments paginated; physical retries counted once`,
    );
  if (serving) {
    // A late contradictory delivery must block publication, including existing cursors.
    await sql(
      `INSERT INTO ${database}.evaluations SELECT * REPLACE ('conflicting-event' AS event_id) FROM ${database}.evaluations WHERE evaluation_id='eval_load_0' LIMIT 1`,
    );
    let rejected = false;
    try {
      await publishAnalytics(config, fetcher);
    } catch {
      rejected = true;
    }
    if (!rejected) throw new Error("Conflicting generation was published");
    try {
      await overview("24h");
      throw new Error("Blocked generation was served");
    } catch (error) {
      if (error.message === "Blocked generation was served") throw error;
    }
    report.stages
      .at(-1)
      .checks.push(
        "Late content conflict blocks new publication and prevents fallback to previous ready generation",
      );
  }
  if (
    report.stages.some((stage) =>
      stage.measurements.some((measurement) => measurement.errors),
    )
  )
    throw new Error(
      "Analytical requests failed; inspect artifacts/investigation-load.json",
    );
} finally {
  await mkdir("artifacts", { recursive: true });
  await writeFile(
    "artifacts/investigation-load.json",
    JSON.stringify(report, null, 2) + "\n",
  );
  if (created) await sql(`DROP DATABASE ${database} SYNC`);
}
console.log(JSON.stringify(report, null, 2));
