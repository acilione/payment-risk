// Bounded, parameterized reads over the same integrity-checked decision view as overview.
export const pageSize = 20;
export function decodeCursor(value, kind) {
  if (!value) return null;
  try {
    const c = JSON.parse(Buffer.from(value, "base64url").toString("utf8"));
    if (kind === "customers" && typeof c.id === "string" && c.id.length <= 256)
      return c;
    if (
      kind === "timeline" &&
      typeof c.id === "string" &&
      c.id.length <= 256 &&
      Number.isSafeInteger(c.time) &&
      c.time >= 0
    )
      return c;
  } catch {
    /* Invalid cursors are rejected before a database call. */
  }
  throw new Error("Invalid cursor");
}
const encode = (value) =>
  Buffer.from(JSON.stringify(value)).toString("base64url");
export function createInvestigationReader(
  { clickhouse, user, password },
  fetcher = fetch,
) {
  async function sql(query, parameters = {}) {
    const url = new URL(clickhouse);
    for (const [key, value] of Object.entries(parameters))
      url.searchParams.set(`param_${key}`, String(value));
    const response = await fetcher(url.toString(), {
      method: "POST",
      headers: {
        "X-ClickHouse-User": user,
        "X-ClickHouse-Key": password,
        "Content-Type": "text/plain",
      },
      signal: AbortSignal.timeout(5000),
      body:
        query +
        " SETTINGS readonly=1, max_execution_time=4, max_memory_usage=268435456, max_bytes_before_external_group_by=67108864, max_bytes_before_external_sort=67108864 FORMAT JSON",
    });
    if (!response.ok) throw new Error("Investigation data unavailable");
    return (await response.json()).data;
  }
  async function integrity() {
    const rows = await sql(
      "SELECT (SELECT count() FROM risk.integrity_conflicts) + (SELECT count() FROM risk.materializer_rejections FINAL) AS unresolved",
    );
    if (rows.length !== 1 || Number(rows[0].unresolved) !== 0)
      throw new Error("Integrity review required");
  }
  const summary =
    "customer_id, count() AS payments, countIf(decision != 'APPROVE') AS alerts, max(risk_score) AS maxScore, max(event_time) AS lastEvent";
  const normalize = (row) => ({
    ...row,
    payments: Number(row.payments),
    alerts: Number(row.alerts),
    maxScore: Number(row.maxScore),
    lastEvent: Number(row.lastEvent),
  });
  return {
    async customers({ q = "", decision = "ALL", cursor = "" }) {
      const after = decodeCursor(cursor, "customers");
      await integrity();
      const rows = await sql(
        `SELECT ${summary} FROM risk.decisions_current WHERE startsWith(customer_id, {q:String}) AND customer_id > {after:String} GROUP BY customer_id HAVING {decision:String} = 'ALL' OR countIf(decision = {decision:String}) > 0 ORDER BY customer_id LIMIT 21`,
        { q, after: after?.id || "", decision },
      );
      const items = rows.slice(0, pageSize).map(normalize);
      return {
        generatedAt: new Date().toISOString(),
        items,
        nextCursor:
          rows.length > pageSize
            ? encode({ id: items.at(-1).customer_id })
            : null,
      };
    },
    async timeline({ customer, cursor = "" }) {
      const after = decodeCursor(cursor, "timeline");
      await integrity();
      const parameters = {
        customer,
        time: after?.time || 0,
        id: after?.id || "",
      };
      const totals = await sql(
        `SELECT ${summary} FROM risk.decisions_current WHERE customer_id = {customer:String} GROUP BY customer_id`,
        parameters,
      );
      const rows = await sql(
        `SELECT evaluation_id,event_id,transaction_id,customer_id,amount_minor,currency,risk_score,decision,event_time,processed_at,engine_version,policy_id,toString(policy_version) AS policy_version,policy_snapshot,rule_evidence,rules_fingerprint,source_topic,source_partition,toString(source_offset) AS source_offset FROM risk.decisions_current WHERE customer_id = {customer:String} ${after ? "AND (event_time, evaluation_id) < ({time:Int64}, {id:String})" : ""} ORDER BY event_time DESC, evaluation_id DESC LIMIT 21`,
        parameters,
      );
      const items = rows.slice(0, pageSize).map((row) => ({
        ...row,
        amount_minor: Number(row.amount_minor),
        risk_score: Number(row.risk_score),
        event_time: Number(row.event_time),
        processed_at: Number(row.processed_at),
        rule_evidence: JSON.parse(row.rule_evidence || "[]"),
      }));
      return {
        generatedAt: new Date().toISOString(),
        customer: totals.length ? normalize(totals[0]) : null,
        items,
        nextCursor:
          rows.length > pageSize
            ? encode({
                time: items.at(-1).event_time,
                id: items.at(-1).evaluation_id,
              })
            : null,
      };
    },
  };
}
