import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import { decodeCursor, pageSize } from "./investigation.mjs";
import { readTransaction } from "./postgres-store.mjs";

const windows = { "15m": 900, "1h": 3600, "24h": 86400 };
const encode = (value) =>
  Buffer.from(JSON.stringify(value)).toString("base64url");
function prefixEnd(prefix) {
  const points = Array.from(prefix, (c) => c.codePointAt(0));
  while (points.length) {
    const last = points.pop();
    if (last < 0x10ffff)
      return String.fromCodePoint(
        ...points,
        last === 0xd7ff ? 0xe000 : last + 1,
      );
  }
  return null;
}
const summary = (row) =>
  row && {
    customer_id: row.customer_id,
    payments: Number(row.payments),
    alerts: Number(row.review) + Number(row.rejected),
    maxScore: Number(row.max_score),
    lastEvent: Number(row.last_event),
  };
const decision = (row) => ({
  ...row,
  policy_version: String(row.policy_version),
  source_offset: String(row.source_offset),
  rule_evidence:
    typeof row.rule_evidence === "string"
      ? JSON.parse(row.rule_evidence)
      : row.rule_evidence,
});

export function createPostgresReader(
  pool,
  { now = Date.now, secret = randomBytes(32) } = {},
) {
  function seal(value) {
    const body = encode(value);
    return (
      body + "." + createHmac("sha256", secret).update(body).digest("base64url")
    );
  }
  function unseal(value) {
    if (typeof value !== "string") throw new Error("Invalid snapshot");
    const [body, signature = ""] = value.split(".");
    const expected = createHmac("sha256", secret).update(body).digest();
    const supplied = Buffer.from(signature, "base64url");
    if (
      supplied.length !== expected.length ||
      !timingSafeEqual(supplied, expected)
    )
      throw new Error("Invalid snapshot");
    const result = JSON.parse(Buffer.from(body, "base64url").toString());
    if (now() - result.at > 120000 || result.at > now() + 5000)
      throw new Error("Snapshot expired");
    return result;
  }
  async function read(cursor, action) {
    return readTransaction(pool, async (db) => {
      const {
        rows: [status],
      } = await db.query(`SELECT ready,
        EXISTS(SELECT 1 FROM risk.incidents WHERE NOT resolved) AS blocked,
        txid_current_snapshot()::text AS snapshot FROM risk.projection_status WHERE id`);
      if (!status?.ready || status.blocked)
        throw new Error("Projection unavailable or blocked");
      const boundary = cursor
        ? unseal(cursor)
        : { snapshot: status.snapshot, at: now() };
      return action(db, boundary, {
        generation: cursor || seal(boundary),
        asOf: new Date(boundary.at).toISOString(),
        store: "PostgreSQL",
        consistency: "committed database snapshot",
      });
    });
  }
  return {
    async overview(window) {
      if (!Object.hasOwn(windows, window))
        throw new Error("Invalid time window");
      return read(null, async (db, boundary, analytics) => {
        const start =
          Math.floor((boundary.at - windows[window] * 1000) / 10000) * 10000;
        const end = Math.floor(boundary.at / 10000) * 10000;
        const { rows: buckets } = await db.query(
          `SELECT bucket,decision,payments,amount_minor FROM risk.time_buckets
          WHERE bucket >= $1 AND bucket <= $2 AND currency='EUR' ORDER BY bucket`,
          [start, end],
        );
        const totals = {
          transactions: 0,
          amountMinor: 0,
          approved: 0,
          review: 0,
          rejected: 0,
          finalizationP95: 0,
        };
        const series = new Map();
        const names = {
          APPROVE: "approved",
          REVIEW: "review",
          REJECT: "rejected",
        };
        const width = (windows[window] * 1000) / 30;
        for (const row of buckets) {
          const n = Number(row.payments),
            label = names[row.decision];
          totals.transactions += n;
          totals.amountMinor += Number(row.amount_minor);
          totals[label] += n;
          const time = Math.floor(Number(row.bucket) / width) * width;
          const point = series.get(time) || {
            time,
            approved: 0,
            review: 0,
            rejected: 0,
          };
          point[label] += n;
          series.set(time, point);
        }
        if (
          !Number.isSafeInteger(totals.amountMinor) ||
          !Number.isSafeInteger(totals.transactions)
        )
          throw new Error("Dashboard numeric range exceeded");
        const { rows: histogram } = await db.query(
          `SELECT upper_ms,sum(samples) AS n FROM risk.latency_buckets
          WHERE bucket >= $1 AND bucket <= $2 GROUP BY upper_ms ORDER BY upper_ms`,
          [start, end],
        );
        let cumulative = 0;
        for (const row of histogram) {
          cumulative += Number(row.n);
          if (cumulative >= Math.ceil(totals.transactions * 0.95)) {
            totals.finalizationP95 = Number(row.upper_ms);
            break;
          }
        }
        const { rows: rules } = await db.query(
          `SELECT rule_id AS id,sum(matches)::float8 AS matches FROM risk.rule_buckets
          WHERE bucket >= $1 AND bucket <= $2 GROUP BY rule_id ORDER BY rule_id`,
          [start, end],
        );
        const { rows: recent } = await db.query(
          `SELECT payload FROM risk.assessments WHERE processed_at >= $1
          AND processed_at < $2 ORDER BY processed_at DESC,transaction_id DESC LIMIT 100`,
          [start, end + 10000],
        );
        return {
          totals,
          series: [...series.values()],
          rules,
          decisions: recent.map((r) => decision(r.payload)),
          generatedAt: analytics.asOf,
          analytics: {
            ...analytics,
            bucketResolutionMs: 10000,
            latencyStatistic:
              "p95 histogram upper bound (powers of two milliseconds)",
          },
        };
      });
    },
    async customers({ q = "", decision: label = "ALL", cursor = "" }) {
      const after = decodeCursor(cursor, "customers");
      if (after && (after.q !== q || after.decision !== label))
        throw new Error("Cursor scope changed");
      const field = {
        ALL: null,
        APPROVE: "approved",
        REVIEW: "review",
        REJECT: "rejected",
      }[label];
      if (field === undefined) throw new Error("Invalid decision");
      return read(after?.generation, async (db, boundary, analytics) => {
        const { rows } = await db.query(
          `SELECT v.payload FROM risk.customer_totals c
          CROSS JOIN LATERAL (SELECT payload FROM risk.customer_versions v
            WHERE v.customer_id=c.customer_id AND txid_visible_in_snapshot(v.xid,$1::txid_snapshot)
            ORDER BY revision DESC LIMIT 1) v
          WHERE c.customer_id > $2 AND c.customer_id >= $3 AND ($4::text IS NULL OR c.customer_id < $4)
            AND starts_with(c.customer_id,$3)
            ${field ? `AND c.${field}>0 AND (v.payload->>'${field}')::bigint > 0` : ""}
          ORDER BY c.customer_id LIMIT 21`,
          [boundary.snapshot, after?.id || "", q, prefixEnd(q)],
        );
        const items = rows.slice(0, pageSize).map((r) => summary(r.payload));
        return {
          generatedAt: analytics.asOf,
          analytics,
          items,
          nextCursor:
            rows.length > pageSize
              ? encode({
                  id: items.at(-1).customer_id,
                  generation: analytics.generation,
                  q,
                  decision: label,
                })
              : null,
        };
      });
    },
    async timeline({ customer, cursor = "" }) {
      const after = decodeCursor(cursor, "timeline");
      if (after && after.customer !== customer)
        throw new Error("Cursor scope changed");
      return read(after?.generation, async (db, boundary, analytics) => {
        const { rows: totals } = await db.query(
          `SELECT payload FROM risk.customer_versions
          WHERE customer_id=$1 AND txid_visible_in_snapshot(xid,$2::txid_snapshot) ORDER BY revision DESC LIMIT 1`,
          [customer, boundary.snapshot],
        );
        const { rows } = await db.query(
          `SELECT payload,transaction_id FROM risk.assessments
          WHERE customer_id=$1 AND txid_visible_in_snapshot(inserted_xid,$2::txid_snapshot)
          ${after ? "AND (event_time,transaction_id)<($3,$4)" : ""}
          ORDER BY event_time DESC,transaction_id DESC LIMIT 21`,
          after
            ? [customer, boundary.snapshot, after.time, after.id]
            : [customer, boundary.snapshot],
        );
        const items = rows.slice(0, pageSize).map((r) => decision(r.payload));
        return {
          generatedAt: analytics.asOf,
          analytics,
          customer: totals.length ? summary(totals[0].payload) : null,
          items,
          nextCursor:
            rows.length > pageSize
              ? encode({
                  time: items.at(-1).event_time,
                  id: items.at(-1).transaction_id,
                  generation: analytics.generation,
                  customer,
                })
              : null,
        };
      });
    },
  };
}
