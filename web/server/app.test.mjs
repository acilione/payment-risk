import { test } from "node:test";
import assert from "node:assert/strict";
import { buildApp } from "./app.mjs";
import { createReader } from "./overview.mjs";

test("only allowlisted windows reach the reader; responses are cached", async () => {
  let calls = 0;
  const app = await buildApp({
    reader: async (window) => {
      calls++;
      return { window };
    },
  });
  try {
    assert.equal(
      (await app.inject("/api/overview?window=1h%27%3BDROP")).statusCode,
      400,
    );
    assert.equal(calls, 0);
    assert.deepEqual((await app.inject("/api/overview?window=15m")).json(), {
      window: "15m",
    });
    await app.inject("/api/overview?window=15m");
    assert.equal(calls, 1);
  } finally {
    await app.close();
  }
});
test("upstream failures never expose credentials or fabricate healthy data", async () => {
  const app = await buildApp({
    reader: async () => {
      throw new Error("password=secret");
    },
  });
  try {
    const response = await app.inject("/api/overview");
    assert.equal(response.statusCode, 503);
    assert.doesNotMatch(response.body, /secret/);
    assert.match(
      response.headers["content-security-policy"],
      /default-src 'self'/,
    );
  } finally {
    await app.close();
  }
});
test("invalid windows cannot be interpolated into ClickHouse SQL", async () => {
  let calls = 0;
  const reader = createReader({}, async () => {
    calls++;
  });
  await assert.rejects(reader("__proto__"), /Invalid time window/);
  assert.equal(calls, 0);
});

test("invalid source telemetry is unavailable; analytics normalize ClickHouse numeric strings", async () => {
  const reader = createReader(
    {
      clickhouse: "http://analytics",
      flink: "http://flink",
      prometheus: "http://metrics",
      user: "risk",
      password: "test-only",
    },
    async (url, options) => {
      let body;
      if (url.startsWith("http://metrics"))
        body = { status: "success", data: { result: [{ value: [0, "NaN"] }] } };
      else if (url.startsWith("http://flink")) body = { jobs: [] };
      else {
        assert.match(
          options.body,
          /SETTINGS readonly=1, max_execution_time=4 FORMAT JSON$/,
        );
        assert.equal(options.headers["X-ClickHouse-Key"], "test-only");
        body = {
          data: options.body.includes("AS transactions")
            ? [
                {
                  transactions: "2",
                  amountMinor: "1299",
                  approved: "1",
                  review: "1",
                  rejected: "0",
                  finalizationP95: "21000",
                },
              ]
            : [],
        };
      }
      return { ok: true, json: async () => body };
    },
  );
  const result = await reader("1h");
  assert.equal(result.totals.amountMinor, 1299);
  assert.equal(result.totals.transactions, 2);
  assert.equal(
    result.services.find((s) => s.name === "Kafka").status,
    "unavailable",
  );
  assert.equal(
    result.services.find((s) => s.name === "ClickHouse").status,
    "healthy",
  );
  assert.equal(result.job, null);
  assert.equal(result.checkpoint, null);
});
