import { test } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createHash } from "node:crypto";

test("portable snapshot retains complete, unique histories and recorded input fingerprints", async () => {
  const snapshot = JSON.parse(
    await readFile(
      new URL("../src/showcase-data.json", import.meta.url),
      "utf8",
    ),
  );
  const { provenance: run, cases, policies } = snapshot;
  assert.equal(run.status, "COMPLETE");
  assert.equal(run.stored_decisions, run.expected_payments);
  assert.equal(
    run.archived_records,
    run.acknowledged_deliveries + run.control_records,
  );
  assert.equal(
    run.acknowledged_deliveries,
    run.expected_payments + run.transport_retries,
  );
  const ids = new Set();
  for (const story of cases) {
    const customer = run.customers.find(
      (c) => c.id === story.target_payment.customer_id,
    );
    assert.equal(story.timeline.length, customer.payments);
    assert.equal(
      story.decision.evaluation_id,
      story.timeline.at(-1).decision.evaluation_id,
    );
    for (const { payment, decision } of story.timeline) {
      assert.equal(ids.has(decision.evaluation_id), false);
      ids.add(decision.evaluation_id);
      assert.equal(payment.event_id, decision.event_id);
      assert.equal(payment.customer_id, decision.customer_id);
      assert.equal(decision.source_topic, run.decision_topic);
      assert.ok(decision.rule_evidence.length > 0);
      assert.ok(JSON.parse(policies[decision.policy_ref]).rules.length > 0);
      const facts = Object.fromEntries(
        Object.entries(payment)
          .filter(([key]) => key !== "producer_time")
          .sort(([a], [b]) => a.localeCompare(b)),
      );
      assert.equal(
        createHash("sha256").update(JSON.stringify(facts)).digest("hex"),
        decision.input_sha256,
      );
    }
  }
  assert.equal(ids.size, run.exported_payments);
  assert.equal(cases.length, run.exported_customers);
  assert.ok(ids.size <= 1000);
});
