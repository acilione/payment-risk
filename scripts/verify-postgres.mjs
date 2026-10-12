// Isolated database: duplicates/conflicts never contaminate the running showcase.
import assert from "node:assert/strict";
import { readFile, mkdir, writeFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
import { postgresPool } from "../web/server/postgres-store.mjs";
import { createPostgresReader } from "../web/server/postgres-reader.mjs";

const options = {
  host: process.env.PGHOST || "127.0.0.1",
  port: Number(process.env.PGPORT || 25432),
  user: "risk",
  password: process.env.RISK_DB_PASSWORD,
  database: "risk",
  statement_timeout: 120000,
};
const admin = postgresPool(options);
const name = "risk_verify_" + randomUUID().replaceAll("-", "");
let pool;
const evidence = { checks: [], stages: [] };
try {
  await admin.query(`CREATE DATABASE ${name}`);
  pool = postgresPool({ ...options, database: name });
  await pool.query(
    await readFile(
      new URL("../infrastructure/docker/postgres.sql", import.meta.url),
      "utf8",
    ),
  );
  await pool.query("UPDATE risk.projection_status SET ready=true");
  const now = Date.now();
  function payment(id, changes = {}) {
    return {
      transaction_id: "tx-" + id,
      evaluation_id: "eval-" + id,
      event_id: "event-" + id,
      assessment_request_id: "initial",
      transaction_facts: JSON.stringify({
        transaction_id: "tx-" + id,
        amount_minor: 100,
      }),
      customer_id: "customer-" + String(Number(id) % 100).padStart(3, "0"),
      decision_id: "risk_tx-" + id,
      amount_minor: 100,
      currency: "EUR",
      decision: "REVIEW",
      risk_score: 40,
      engine_version: "v1",
      policy_id: "p",
      policy_version: 1,
      policy_snapshot: "{}",
      rules_fingerprint: "r",
      rule_evidence: "[]",
      matched_rules: ["R001"],
      reason_codes: [],
      input_sha256: "input-" + id,
      event_time: now - 1000,
      processed_at: now,
      source_topic: "verify",
      source_partition: 0,
      source_offset: Number(id),
      ...changes,
    };
  }
  const accept = (p, client = pool) =>
    client.query(
      "SELECT risk.accept('risk.evaluations',$1::jsonb,'bank') AS result",
      [JSON.stringify(p)],
    );
  const counts = async () =>
    (
      await pool.query(`SELECT
    (SELECT count(*) FROM risk.assessments)::int AS assessments,
    (SELECT coalesce(sum(payments),0) FROM risk.time_buckets)::int AS payments,
    (SELECT coalesce(sum(amount_minor),0) FROM risk.time_buckets)::int AS amount`)
    ).rows[0];
  await Promise.all(Array.from({ length: 8 }, () => accept(payment(1))));
  assert.deepEqual(await counts(), {
    assessments: 1,
    payments: 1,
    amount: 100,
  });
  await accept(
    payment(1, { source_offset: 100001, processed_at: now + 20000 }),
  );
  await accept(
    payment(1, {
      event_id: "republished-event",
      evaluation_id: "republished-eval",
      input_sha256: "transport-changed",
      source_offset: 100002,
    }),
  );
  assert.deepEqual(await counts(), {
    assessments: 1,
    payments: 1,
    amount: 100,
  });
  evidence.checks.push(
    "concurrent same-delivery retries",
    "new Kafka coordinate",
    "new event and evaluation IDs for identical transaction",
  );
  await accept(
    payment(1, {
      engine_version: "v2",
      evaluation_id: "new-engine-eval",
      source_offset: 100003,
    }),
  );
  assert.equal(
    (await pool.query("SELECT reason FROM risk.incidents WHERE NOT resolved"))
      .rows[0].reason,
    "ASSESSMENT_CONTENT_CONFLICT",
  );
  const reader = createPostgresReader(pool, { now: () => now });
  await assert.rejects(reader.overview("15m"), /blocked/);
  await pool.query("UPDATE risk.incidents SET resolved=true");
  await accept(payment(2, { evaluation_id: "eval-1", source_offset: 100004 }));
  assert.equal(
    (await pool.query("SELECT reason FROM risk.incidents WHERE NOT resolved"))
      .rows[0].reason,
    "IDENTITY_ASSIGNED_TO_ANOTHER_TRANSACTION",
  );
  await pool.query("UPDATE risk.incidents SET resolved=true");
  await accept(payment(1, { amount_minor: 999 }));
  assert.equal(
    (await pool.query("SELECT reason FROM risk.incidents WHERE NOT resolved"))
      .rows[0].reason,
    "DELIVERY_CONTENT_CONFLICT",
  );
  await pool.query("UPDATE risk.incidents SET resolved=true");
  evidence.checks.push(
    "changed engine is not a new payment",
    "conflicting delivery persisted",
    "evaluation ID collision",
    "API fails closed on incidents",
  );
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await accept(payment(3), client);
    await client.query("ROLLBACK");
    assert.deepEqual(await counts(), {
      assessments: 1,
      payments: 1,
      amount: 100,
    });
    await accept(payment(3));
    await accept(payment(3));
  } finally {
    client.release();
  }
  evidence.checks.push(
    "rollback leaves no partial counters",
    "commit followed by redelivery leaves totals unchanged",
  );

  let previous = 3;
  for (const size of (process.env.PROJECTION_STAGES || "1000,10000,50000")
    .split(",")
    .map(Number)) {
    const begun = performance.now();
    for (let start = previous + 1; start <= size; start += 250) {
      const batch = Array.from(
        { length: Math.min(250, size - start + 1) },
        (_, i) => payment(start + i),
      );
      await pool.query(
        "SELECT risk.accept('risk.evaluations',value,'bank') FROM jsonb_array_elements($1::jsonb)",
        [JSON.stringify(batch)],
      );
    }
    previous = size;
    await pool.query(
      "ANALYZE risk.assessments; ANALYZE risk.customer_totals; ANALYZE risk.customer_versions; ANALYZE risk.time_buckets",
    );
    const timings = [];
    let overview;
    for (let i = 0; i < 5; i++) {
      const started = performance.now();
      overview = await reader.overview("15m");
      timings.push(performance.now() - started);
    }
    assert.equal(overview.totals.transactions, size - 1); // transaction 2 was a rejected identity collision
    assert.equal(overview.totals.amountMinor, (size - 1) * 100);
    const first = await reader.customers({});
    assert.equal(first.items.length, 20);
    const timeline = await reader.timeline({ customer: "customer-001" });
    assert.ok(timeline.items.length > 0);
    const plan = (
      await pool.query(`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
      SELECT payload FROM risk.assessments WHERE customer_id='customer-001'
      ORDER BY event_time DESC,transaction_id DESC LIMIT 21`)
    ).rows[0]["QUERY PLAN"];
    assert.ok(
      JSON.stringify(plan).includes("assessment_customer_time"),
      "Timeline must use its customer index",
    );
    evidence.stages.push({
      assessments: size - 1,
      insertMs: performance.now() - begun,
      overviewMs: timings,
      bucketRows: Number(
        (await pool.query("SELECT count(*) FROM risk.time_buckets")).rows[0]
          .count,
      ),
      timelinePlan: plan,
      identityIndexBytes: Number(
        (
          await pool.query(
            "SELECT pg_relation_size('risk.assessments_pkey') AS bytes",
          )
        ).rows[0].bytes,
      ),
    });
  }
  const first = await reader.timeline({ customer: "customer-001" });
  const oldCount = first.customer.payments;
  await accept(
    payment(200001, {
      customer_id: "customer-001",
      event_time: now + 1,
      source_offset: 200001,
    }),
  );
  const next = await reader.timeline({
    customer: "customer-001",
    cursor: first.nextCursor,
  });
  assert.equal(next.customer.payments, oldCount);
  assert.ok(!next.items.some((x) => x.transaction_id === "tx-200001"));
  await assert.rejects(
    reader.timeline({ customer: "another", cursor: first.nextCursor }),
    /scope/,
  );
  evidence.checks.push(
    "indexed timeline at growing history",
    "snapshot pagination excludes newly committed assessments",
    "cursor scope validation",
    "exact totals from compact buckets",
  );
  assert.equal((await reader.customers({ q: "customer-00" })).items.length, 10);
  assert.equal((await reader.customers({ q: "customer-%" })).items.length, 0);
  const older = await pool.connect();
  try {
    await older.query("BEGIN");
    await older.query("SELECT txid_current()");
    await accept(
      payment(300001, { customer_id: "commit-order", source_offset: 300001 }),
    );
    await accept(
      payment(300002, { customer_id: "commit-order", source_offset: 300002 }),
      older,
    );
    await older.query("COMMIT");
  } finally {
    older.release();
  }
  assert.equal(
    (await reader.timeline({ customer: "commit-order" })).customer.payments,
    2,
  );
  evidence.checks.push(
    "literal indexed prefix search",
    "customer versions follow update order, not transaction ID allocation order",
  );
  evidence.result = "PASS";
  await mkdir(new URL("../artifacts/", import.meta.url), { recursive: true });
  await writeFile(
    new URL("../artifacts/postgres-projection.json", import.meta.url),
    JSON.stringify(evidence, null, 2) + "\n",
  );
  console.log(JSON.stringify(evidence));
} finally {
  if (pool) await pool.end();
  await admin.query(`DROP DATABASE IF EXISTS ${name}`);
  await admin.end();
}
