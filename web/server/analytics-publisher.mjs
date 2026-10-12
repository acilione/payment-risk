import { randomUUID } from "node:crypto";
import { analyticsStore } from "./analytics-store.mjs";

const columns =
  "decision_id,event_id,transaction_id,customer_id,amount_minor,currency,risk_score,decision,matched_rules,reason_codes,rules_fingerprint,event_time,processed_at,source_partition,source_offset,source_topic,evaluation_id,engine_version,policy_id,policy_version,policy_snapshot,rule_evidence,input_sha256,ingested_at".split(
    ",",
  );
const content = columns.filter(
  (c) =>
    ![
      "processed_at",
      "source_partition",
      "source_offset",
      "source_topic",
      "ingested_at",
    ].includes(c),
);

// A candidate is never readable until all validation and derived writes succeed.
export async function publishAnalytics(config, fetcher = fetch) {
  const sql = analyticsStore(config, fetcher, { writable: true });
  const generation = randomUUID();
  const started = (
    await sql("SELECT toUnixTimestamp64Milli(now64(3)) AS time", {}, true)
  )[0].time;
  const p = { generation, started };
  const source =
    "risk.serving_evaluations WHERE generation={generation:String}";
  let assessments = 0,
    deliveries = 0,
    offsets = "[]";
  async function publication(status, reason = "") {
    await sql(
      `INSERT INTO risk.serving_publications (generation,started_at,status,assessments,deliveries,source_offsets,build_ms,reason)
      SELECT {generation:String},{started:Int64},{status:String},{assessments:UInt64},{deliveries:UInt64},{offsets:String},
      toUInt64(greatest(0,toUnixTimestamp64Milli(now64(3))-{started:Int64})),{reason:String}`,
      { ...p, status, assessments, deliveries, offsets, reason },
    );
  }
  try {
    await sql(
      `INSERT INTO risk.serving_evaluations
      SELECT {generation:String}, now(), ${columns.map((c, i) => `row.${i + 1}`).join(",")}, variants, copies
      FROM (SELECT argMin(tuple(${columns.join(",")}),tuple(processed_at,source_topic,source_partition,source_offset)) AS row,
      uniqExact(tuple(${content.join(",")})) AS variants, count() AS copies
      FROM risk.evaluations WHERE ingested_at <= fromUnixTimestamp64Milli({started:Int64}) GROUP BY evaluation_id)`,
      p,
    );
    const check = (
      await sql(
        `SELECT count() AS assessments, sum(delivery_count) AS deliveries,
      countIf(content_variants != 1) AS conflicts,
      count()-uniqExact(transaction_id) AS ambiguous_transactions FROM ${source}`,
        p,
        true,
      )
    )[0];
    assessments = check.assessments;
    deliveries = check.deliveries;
    const rejected = (
      await sql(
        "SELECT count() AS n FROM risk.materializer_rejections FINAL",
        {},
        true,
      )
    )[0].n;
    if (
      Number(check.conflicts) ||
      Number(check.ambiguous_transactions) ||
      Number(rejected)
    )
      throw new Error("Integrity conflict or quarantined output");
    // Coordinates describe included representative deliveries, not an input-completeness claim.
    offsets = JSON.stringify(
      await sql(
        `SELECT source_topic,source_partition,toString(max(source_offset)) AS max_offset
      FROM ${source} GROUP BY source_topic,source_partition`,
        p,
        true,
      ),
    );
    await sql(
      `INSERT INTO risk.serving_customers SELECT {generation:String},now(),customer_id,count(),
      countIf(decision!='APPROVE'),max(risk_score),max(event_time),countIf(decision='APPROVE'),
      countIf(decision='REVIEW'),countIf(decision='REJECT') FROM ${source} GROUP BY customer_id`,
      p,
    );
    for (const [window, seconds] of [
      ["15m", 900],
      ["1h", 3600],
      ["24h", 86400],
    ]) {
      const where = `${source} AND processed_at >= {started:Int64}-${seconds * 1000}`;
      const totals = (
        await sql(
          `SELECT count() AS transactions,sum(amount_minor) AS amountMinor,
        countIf(decision='APPROVE') AS approved,countIf(decision='REVIEW') AS review,countIf(decision='REJECT') AS rejected,
        if(count()=0,0,quantileTDigest(0.95)(greatest(0,processed_at-event_time))) AS finalizationP95 FROM ${where}`,
          p,
          true,
        )
      )[0];
      const series = await sql(
        `SELECT intDiv(processed_at,${(seconds / 30) * 1000})*${(seconds / 30) * 1000} AS time,
        countIf(decision='APPROVE') AS approved,countIf(decision='REVIEW') AS review,countIf(decision='REJECT') AS rejected
        FROM ${where} GROUP BY time ORDER BY time`,
        p,
        true,
      );
      const rules = await sql(
        `SELECT arrayJoin(matched_rules) AS id,count() AS matches FROM ${where} GROUP BY id ORDER BY id`,
        p,
        true,
      );
      const decisions = await sql(
        `SELECT transaction_id,event_id,customer_id,amount_minor,risk_score,decision,matched_rules,
        reason_codes,rules_fingerprint,event_time,processed_at,source_partition,toString(source_offset) AS source_offset
        FROM ${where} ORDER BY processed_at DESC,transaction_id LIMIT 100`,
        p,
        true,
      );
      const payload = JSON.stringify({ totals, series, rules, decisions });
      await sql(
        "INSERT INTO risk.serving_overview SELECT {generation:String},now(),{window:String},{payload:String}",
        { ...p, window, payload },
      );
    }
    await publication("READY");
    return {
      generation,
      startedAt: Number(started),
      assessments: Number(assessments),
      deliveries: Number(deliveries),
    };
  } catch (error) {
    // If the database is unreachable the previous generation expires at the reader's age bound.
    await publication("BLOCKED", error.message).catch(() => {});
    throw error;
  }
}
