import { test } from "node:test";
import assert from "node:assert/strict";
import { buildApp } from "./app.mjs";
import { createInvestigationReader } from "./investigation.mjs";
const config = {
  clickhouse: "http://analytics",
  user: "risk",
  password: "test-only",
};
test("investigation inputs are bounded and invalid cursors never reach storage", async () => {
  let calls = 0;
  const app = await buildApp({
    reader: async () => ({}),
    investigationReader: {
      customers: async (q) => {
        calls++;
        return q;
      },
      timeline: async (q) => {
        calls++;
        return q;
      },
    },
  });
  try {
    for (const path of [
      "customers?decision=INVALID",
      "customers?cursor=invalid",
      "timeline",
      "timeline?customer=",
      "customers?q=" + "a".repeat(257),
      "timeline?customer=x&cursor=" +
        Buffer.from('{"time":-1,"id":"x"}').toString("base64url"),
    ])
      assert.equal(
        (await app.inject("/api/investigations/" + path)).statusCode,
        400,
      );
    assert.equal(calls, 0);
    const response = await app.inject("/api/investigations/customers?q=demo");
    assert.equal(response.statusCode, 200);
    assert.equal(response.headers["cache-control"], "no-store");
    assert.equal(response.json().q, "demo");
  } finally {
    await app.close();
  }
});
test("customer search uses parameters, exact counts and bounded keyset pages", async () => {
  const query = "x' OR 1=1 --";
  const calls = [];
  const reader = createInvestigationReader(config, async (url, options) => {
    calls.push({ url, options });
    return {
      ok: true,
      json: async () => ({
        data: options.body.includes("AS unresolved")
          ? [{ unresolved: "0" }]
          : Array.from({ length: 21 }, (_, i) => ({
              customer_id: "c" + i,
              payments: "2",
              alerts: "1",
              maxScore: 30,
              lastEvent: "100",
            })),
      }),
    };
  });
  const result = await reader.customers({ q: query });
  assert.equal(result.items.length, 20);
  assert.equal(result.items[0].payments, 2);
  assert.equal(
    JSON.parse(Buffer.from(result.nextCursor, "base64url")).id,
    "c19",
  );
  assert.equal(new URL(calls[1].url).searchParams.get("param_q"), query);
  assert.ok(!calls[1].options.body.includes(query));
  assert.match(calls[1].options.body, /LIMIT 21/);
  assert.match(calls[1].options.body, /readonly=1/);
});
test("timeline keeps source offsets exact, parses all evidence and carries a stable tie-breaker", async () => {
  const calls = [];
  const rows = Array.from({ length: 21 }, (_, i) => ({
    evaluation_id: "e" + (30 - i),
    amount_minor: "90000",
    risk_score: 30,
    event_time: "100",
    processed_at: "200",
    source_offset: "9007199254740993",
    rule_evidence: '[{"rule_id":"R004","matched":true}]',
  }));
  const reader = createInvestigationReader(config, async (url, options) => {
    calls.push({ url, options });
    return {
      ok: true,
      json: async () => ({
        data: options.body.includes("AS unresolved")
          ? [{ unresolved: 0 }]
          : options.body.includes("AS payments")
            ? []
            : rows,
      }),
    };
  });
  const first = await reader.timeline({ customer: "c" });
  assert.equal(first.items[0].source_offset, "9007199254740993");
  assert.equal(first.items[0].rule_evidence[0].rule_id, "R004");
  await reader.timeline({ customer: "c", cursor: first.nextCursor });
  assert.match(
    calls.at(-1).options.body,
    /\(event_time, evaluation_id\) < \(\{time:Int64\}, \{id:String\}\)/,
  );
  assert.equal(new URL(calls.at(-1).url).searchParams.get("param_id"), "e11");
});
test("integrity incidents prevent timeline publication; errors do not reveal upstream secrets", async () => {
  let calls = 0;
  const reader = createInvestigationReader(config, async () => {
    calls++;
    return { ok: true, json: async () => ({ data: [{ unresolved: 1 }] }) };
  });
  await assert.rejects(reader.timeline({ customer: "c" }), /Integrity/);
  assert.equal(calls, 1);
  const app = await buildApp({
    reader: async () => ({}),
    investigationReader: {
      customers: async () => {
        throw new Error("password=private");
      },
    },
  });
  try {
    const r = await app.inject("/api/investigations/customers");
    assert.equal(r.statusCode, 503);
    assert.doesNotMatch(r.body, /private/);
  } finally {
    await app.close();
  }
});
