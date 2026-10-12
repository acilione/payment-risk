import { analyticsStore } from "./analytics-store.mjs";
import { decodeCursor, pageSize } from "./investigation.mjs";
const encode = (value) =>
  Buffer.from(JSON.stringify(value)).toString("base64url");
const summary = "customer_id,payments,alerts,maxScore,lastEvent";
const normalize = (row) =>
  Object.fromEntries(
    Object.entries(row).map(([k, v]) => [
      k,
      [
        "payments",
        "alerts",
        "maxScore",
        "lastEvent",
        "amount_minor",
        "risk_score",
        "event_time",
        "processed_at",
      ].includes(k)
        ? Number(v)
        : v,
    ]),
  );

export function createServingReader(
  config,
  fetcher = fetch,
  { now = Date.now, maxAgeMs = 120000 } = {},
) {
  const sql = analyticsStore(config, fetcher);
  async function publication(requested) {
    const latest = await sql(
      "SELECT * FROM risk.serving_publications ORDER BY started_at DESC,published_at DESC LIMIT 1",
      {},
      true,
    );
    if (latest.length !== 1 || latest[0].status !== "READY")
      throw new Error("Analytics publication unavailable");
    let row = latest[0];
    if (requested && requested !== row.generation) {
      const saved = await sql(
        "SELECT * FROM risk.serving_publications WHERE generation={generation:String} ORDER BY published_at DESC LIMIT 1",
        { generation: requested },
        true,
      );
      if (saved.length !== 1 || saved[0].status !== "READY")
        throw new Error("Unknown analytics generation");
      row = saved[0];
    }
    const age = now() - Number(row.started_at);
    if (!Number.isFinite(age) || age < -5000 || age > maxAgeMs)
      throw new Error("Analytics publication expired");
    return {
      generation: row.generation,
      asOf: new Date(Number(row.started_at)).toISOString(),
      publishedAt: row.published_at,
    };
  }
  return {
    async overview(window) {
      const boundary = await publication();
      const rows = await sql(
        "SELECT payload FROM risk.serving_overview WHERE generation={generation:String} AND window={window:String}",
        { generation: boundary.generation, window },
        true,
      );
      if (rows.length !== 1) throw new Error("Incomplete analytics generation");
      return {
        ...JSON.parse(rows[0].payload),
        generatedAt: boundary.asOf,
        analytics: boundary,
      };
    },
    async customers({ q = "", decision = "ALL", cursor = "" }) {
      const after = decodeCursor(cursor, "customers");
      if (
        after &&
        (after.q !== q ||
          after.decision !== decision ||
          typeof after.generation !== "string")
      )
        throw new Error("Cursor scope changed");
      const boundary = await publication(after?.generation);
      const filter = {
        ALL: "1",
        APPROVE: "approved>0",
        REVIEW: "review>0",
        REJECT: "rejected>0",
      }[decision];
      if (!filter) throw new Error("Invalid decision");
      const rows = await sql(
        `SELECT ${summary} FROM risk.serving_customers WHERE generation={generation:String}
        AND startsWith(customer_id,{q:String}) AND customer_id>{after:String} AND ${filter} ORDER BY customer_id LIMIT 21`,
        { generation: boundary.generation, q, after: after?.id || "" },
        true,
      );
      const items = rows.slice(0, pageSize).map(normalize);
      return {
        generatedAt: boundary.asOf,
        analytics: boundary,
        items,
        nextCursor:
          rows.length > pageSize
            ? encode({
                id: items.at(-1).customer_id,
                generation: boundary.generation,
                q,
                decision,
              })
            : null,
      };
    },
    async timeline({ customer, cursor = "" }) {
      const after = decodeCursor(cursor, "timeline");
      if (
        after &&
        (after.customer !== customer || typeof after.generation !== "string")
      )
        throw new Error("Cursor scope changed");
      const boundary = await publication(after?.generation);
      const p = {
        generation: boundary.generation,
        customer,
        time: after?.time || 0,
        id: after?.id || "",
      };
      const totals = await sql(
        `SELECT ${summary} FROM risk.serving_customers WHERE generation={generation:String} AND customer_id={customer:String}`,
        p,
        true,
      );
      const rows = await sql(
        `SELECT evaluation_id,event_id,transaction_id,customer_id,amount_minor,currency,risk_score,decision,event_time,processed_at,
        engine_version,policy_id,toString(policy_version) AS policy_version,policy_snapshot,rule_evidence,rules_fingerprint,source_topic,
        source_partition,toString(source_offset) AS source_offset FROM risk.serving_evaluations
        WHERE generation={generation:String} AND customer_id={customer:String}
        ${after ? "AND (event_time,evaluation_id)<({time:Int64},{id:String})" : ""}
        ORDER BY event_time DESC,evaluation_id DESC LIMIT 21`,
        p,
        true,
      );
      const items = rows.slice(0, pageSize).map((row) => ({
        ...normalize(row),
        rule_evidence: JSON.parse(row.rule_evidence || "[]"),
      }));
      return {
        generatedAt: boundary.asOf,
        analytics: boundary,
        customer: totals.length ? normalize(totals[0]) : null,
        items,
        nextCursor:
          rows.length > pageSize
            ? encode({
                time: items.at(-1).event_time,
                id: items.at(-1).evaluation_id,
                generation: boundary.generation,
                customer,
              })
            : null,
      };
    },
  };
}
