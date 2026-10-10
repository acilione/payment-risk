import type { DecisionKind, Overview, WindowKey } from "./types";
import { demoDecisions, demoEventTime } from "./investigation-demo";

export const ruleCatalog = [
  {
    id: "R001",
    name: "Payment frequency",
    description: "More than 5 payments by the same customer within 2 minutes.",
    score: 25,
  },
  {
    id: "R002",
    name: "Total payment amount",
    description:
      "More than €3,000 in payments by the same customer within 10 minutes.",
    score: 35,
  },
  {
    id: "R003",
    name: "Multiple devices",
    description:
      "Payments from at least 3 devices for the same customer within 15 minutes.",
    score: 20,
  },
  {
    id: "R004",
    name: "New device",
    description:
      "A payment of at least €800 from a device not seen for that customer in the previous 30 days.",
    score: 30,
  },
  {
    id: "R005",
    name: "Approval after declines",
    description:
      "An approved payment after at least 4 declines for the same customer within 10 minutes.",
    score: 40,
  },
];

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
