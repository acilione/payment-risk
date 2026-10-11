import type { DecisionKind, Overview, WindowKey } from "./types";
import { demoDecisions, demoEventTime } from "./investigation-demo";

export function demoOverview(window: WindowKey): Overview {
  // Use the fixture's event-time horizon so an archived demo remains reproducible.
  const span = { "15m": 900000, "1h": 3600000, "24h": 86400000 }[window];
  const bucket = span / 30;
  const lastBucket = Math.floor(demoEventTime / bucket) * bucket;
  const decisions = demoDecisions
    .filter((d) => d.event_time >= demoEventTime - span)
    .map((d) => ({ ...d, decision: d.decision as DecisionKind }))
    .sort(
      (a, b) =>
        b.event_time - a.event_time ||
        b.evaluation_id.localeCompare(a.evaluation_id),
    );
  const series = Array.from({ length: 31 }, (_, i) => ({
    time: lastBucket - (30 - i) * bucket,
    approved: 0,
    review: 0,
    rejected: 0,
  }));
  const counts = { approved: 0, review: 0, rejected: 0 };
  const kinds = {
    APPROVE: "approved",
    REVIEW: "review",
    REJECT: "rejected",
  } as const;
  const ruleCounts = new Map<string, number>();
  for (const d of decisions) {
    const key = kinds[d.decision];
    counts[key]++;
    const point = series.find(
      (s) => s.time === Math.floor(d.event_time / bucket) * bucket,
    );
    if (point) point[key]++;
    for (const id of d.matched_rules)
      ruleCounts.set(id, (ruleCounts.get(id) || 0) + 1);
  }
  return {
    generatedAt: new Date(demoEventTime).toISOString(),
    window,
    totals: {
      transactions: decisions.length,
      amountMinor: decisions.reduce((sum, d) => sum + d.amount_minor, 0),
      ...counts,
      finalizationP95: null,
    },
    series,
    decisions,
    rules: [...ruleCounts]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([id, matches]) => ({ id, matches })),
    services: ["Kafka", "Apache Flink", "ClickHouse", "Checkpoints"].map(
      (name) => ({
        name,
        status: "idle",
        detail: "Not measured in the static demo",
      }),
    ),
    checkpoint: null,
    job: null,
  };
}
