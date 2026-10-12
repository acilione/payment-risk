import { test } from "node:test";
import assert from "node:assert/strict";
import { createServingReader } from "./serving-reader.mjs";
import { publishAnalytics } from "./analytics-publisher.mjs";
const config = {
  clickhouse: "http://database",
  user: "test",
  password: "test-only",
};
const response = (data) => ({
  ok: true,
  json: async () => ({ data }),
  text: async () => "",
});

test("unavailable, blocked, expired and incomplete generations cannot be served", async () => {
  for (const rows of [
    [],
    [{ status: "BLOCKED", started_at: 1000, generation: "g" }],
    [{ status: "READY", started_at: 1000, generation: "g" }],
  ]) {
    const reader = createServingReader(config, async () => response(rows), {
      now: () => 200000,
    });
    await assert.rejects(reader.overview("1h"));
  }
  const reader = createServingReader(
    config,
    async (_url, options) =>
      response(
        options.body.includes("serving_publications")
          ? [{ status: "READY", started_at: 1000, generation: "g" }]
          : [],
      ),
    { now: () => 1001 },
  );
  await assert.rejects(reader.overview("1h"), /Incomplete/);
});

test("pagination keeps its publication and filter even when a new generation appears", async () => {
  let latest = "first";
  const queries = [];
  const reader = createServingReader(
    config,
    async (url, options) => {
      const p = new URL(url).searchParams;
      if (options.body.includes("serving_publications"))
        return response([
          {
            status: "READY",
            started_at: 1000,
            generation: p.get("param_generation") || latest,
          },
        ]);
      queries.push(p.get("param_generation"));
      return response(
        Array.from({ length: 21 }, (_, i) => ({
          customer_id: `c${i}`,
          payments: "1",
          alerts: "0",
          maxScore: "0",
          lastEvent: "100",
        })),
      );
    },
    { now: () => 1001 },
  );
  const first = await reader.customers({ q: "c" });
  latest = "second";
  await reader.customers({ q: "c", cursor: first.nextCursor });
  assert.deepEqual(queries, ["first", "first"]);
  await assert.rejects(
    reader.customers({ q: "other", cursor: first.nextCursor }),
    /scope/,
  );
});

test("partial candidates, conflicting identities and quarantine never get a READY marker", async () => {
  for (const failure of ["partial", "content", "transaction", "quarantine"]) {
    const statuses = [];
    await assert.rejects(
      publishAnalytics(config, async (url, options) => {
        const q = options.body,
          p = new URL(url).searchParams;
        if (q.includes("INSERT INTO risk.serving_publications")) {
          statuses.push(p.get("param_status"));
          return response([]);
        }
        if (q.startsWith("SELECT toUnix")) return response([{ time: 1000 }]);
        if (q.includes("AS ambiguous_transactions"))
          return response([
            {
              assessments: "1",
              deliveries: "2",
              conflicts: failure === "content" ? "1" : "0",
              ambiguous_transactions: failure === "transaction" ? "1" : "0",
            },
          ]);
        if (q.includes("materializer_rejections"))
          return response([{ n: failure === "quarantine" ? 1 : 0 }]);
        if (q.includes("INSERT INTO risk.serving_customers"))
          throw new Error("connection lost after insert");
        return response([]);
      }),
    );
    assert.deepEqual(statuses, ["BLOCKED"]);
  }
});
