// Compare writer concurrency against the real acceptance function in isolated databases.
import assert from "node:assert/strict";
import { readFile, mkdir, writeFile } from "node:fs/promises";
import { createHash, randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
import { setTimeout as delay } from "node:timers/promises";
import { postgresPool } from "../web/server/postgres-store.mjs";

const options = {
  host: process.env.PGHOST || "127.0.0.1",
  port: Number(process.env.PGPORT || 25432),
  user: "risk",
  password: process.env.RISK_DB_PASSWORD,
  database: "risk",
  statement_timeout: 120000,
};
const count = Number(process.env.WRITER_TEST_ROWS || 9000);
assert(
  Number.isInteger(count) && count >= 300 && count <= 100000 && count % 3 === 0,
);
const schema = await readFile(
  new URL("../infrastructure/docker/postgres.sql", import.meta.url),
  "utf8",
);
const fixture = JSON.parse(
  await readFile(
    new URL("../web/src/investigation-fixture.json", import.meta.url),
    "utf8",
  ),
);
const hash = (s) => createHash("sha256").update(s).digest("hex");
const canonical = (o) =>
  JSON.stringify(
    Object.fromEntries(
      Object.entries(o).sort(([a], [b]) => a.localeCompare(b, "en")),
    ),
  );
const admin = postgresPool(options);
const results = [];
try {
  // Reverse the order in the second pair to expose warm-cache/order effects.
  for (const writers of [1, 3, 3, 1]) {
    const name = "risk_writers_" + randomUUID().replaceAll("-", "");
    await admin.query(`CREATE DATABASE ${name}`);
    const pool = postgresPool({ ...options, database: name });
    try {
      await pool.query(schema);
      const base = Math.floor(Date.now() / 10000) * 10000;
      function row(i) {
        const sample = fixture.cases[i % fixture.cases.length];
        const tx = {
          ...sample.target_payment,
          event_id: `event-${i}`,
          transaction_id: `tx-${i}`,
          // A multiple of three keeps each customer's records on one source partition.
          customer_id: `customer-${i % 999}`,
          event_time: base + i * 2,
        };
        const { producer_time, ...input } = tx;
        const { event_id, ...facts } = input;
        return {
          ...sample.decision,
          event_id,
          transaction_id: tx.transaction_id,
          customer_id: tx.customer_id,
          event_time: tx.event_time,
          processed_at: tx.event_time + 1000,
          decision_id: `risk_tx-${i}`,
          evaluation_id: `eval-${i}`,
          input_sha256: hash(canonical(input)),
          transaction_facts: canonical(facts),
          source_topic: "writer-test",
          source_partition: i % 3,
          source_offset: Math.floor(i / 3),
        };
      }
      let stopped = false,
        samples = 0,
        blockedSamples = 0,
        maxBlocked = 0;
      const monitor = (async () => {
        while (!stopped) {
          const {
            rows: [r],
          } = await admin.query(
            "SELECT count(*)::int AS blocked FROM pg_stat_activity WHERE datname=$1 AND wait_event_type='Lock'",
            [name],
          );
          samples++;
          blockedSamples += r.blocked > 0 ? 1 : 0;
          maxBlocked = Math.max(maxBlocked, r.blocked);
          await delay(50);
        }
      })();
      const durations = [];
      const started = performance.now();
      try {
        await Promise.all(
          Array.from({ length: writers }, async (_, worker) => {
            const client = await pool.connect();
            try {
              for (let first = worker; first < count; first += 250 * writers) {
                const rows = [];
                for (
                  let i = first;
                  i < Math.min(count, first + 250 * writers);
                  i += writers
                )
                  rows.push(row(i));
                const tick = performance.now();
                await client.query("BEGIN");
                await client.query(
                  "SELECT risk.accept('risk.evaluations',value,'writer-benchmark') FROM jsonb_array_elements($1::jsonb)",
                  [JSON.stringify(rows)],
                );
                await client.query("COMMIT");
                durations.push(performance.now() - tick);
              }
            } finally {
              client.release();
            }
          }),
        );
      } finally {
        stopped = true;
        await monitor;
      }
      const elapsed = performance.now() - started;
      const {
        rows: [actual],
      } = await pool.query(`SELECT
        (SELECT count(*)::int FROM risk.assessments) AS assessments,
        (SELECT sum(payments)::int FROM risk.time_buckets) AS payments,
        (SELECT count(*)::int FROM risk.incidents) AS incidents`);
      assert.deepEqual(actual, {
        assessments: count,
        payments: count,
        incidents: 0,
      });
      durations.sort((a, b) => a - b);
      results.push({
        writers,
        rows: count,
        elapsed_ms: elapsed,
        rows_per_second: (count * 1000) / elapsed,
        batch_p50_ms: durations[Math.ceil(durations.length * 0.5) - 1],
        batch_p95_ms: durations[Math.ceil(durations.length * 0.95) - 1],
        lock_samples: samples,
        samples_with_blocked_writer: blockedSamples,
        max_blocked_writers: maxBlocked,
        reconciliation: actual,
      });
      console.log(JSON.stringify(results.at(-1)));
    } finally {
      await pool.end();
      await admin.query(`DROP DATABASE ${name}`);
    }
  }
} finally {
  await admin.end();
}
await mkdir("artifacts", { recursive: true });
await writeFile(
  "artifacts/projection-writers.json",
  JSON.stringify(
    {
      scope:
        "Direct PostgreSQL acceptance, batches of 250, 999 customers with fixed partition affinity, three source partitions, shared topic; excludes Kafka/Flink",
      qualification:
        "Short shared-host concurrency experiment; fixed fixture-derived assessment payloads, not a capacity qualification",
      schema_sha256: hash(schema),
      results,
    },
    null,
    2,
  ) + "\n",
);
