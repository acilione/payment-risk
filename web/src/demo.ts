import type { DecisionKind, Overview, WindowKey } from "./types";

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
  const now = Date.now();
  const span = { "15m": 900000, "1h": 3600000, "24h": 86400000 }[window];
  const bucket = span / 30;
  const lastBucket = Math.floor(now / bucket) * bucket;
  const series = Array.from({ length: 30 }, (_, i) => ({
    time: lastBucket - (29 - i) * bucket,
    approved: Math.round(
      210 +
        Math.sin(i * 0.55) * 55 +
        (i > 11 && i < 20 ? 170 : 0) +
        (i % 4) * 15,
    ),
    review: 12 + ((i * 7) % 26),
    rejected: 5 + ((i * 3) % 13),
  }));
  const approved = series.reduce((n, p) => n + p.approved, 0);
  const review = series.reduce((n, p) => n + p.review, 0);
  const rejected = series.reduce((n, p) => n + p.rejected, 0);
  const decisions = Array.from({ length: 48 }, (_, i) => {
    const kind: DecisionKind =
      i % 7 === 0 ? "REJECT" : i % 4 === 0 ? "REVIEW" : "APPROVE";
    const matched_rules =
      kind === "REJECT"
        ? ["R001", "R002", "R003"]
        : kind === "REVIEW"
          ? ["R004"]
          : [];
    return {
      transaction_id: `txn_demo_${(782041 + i).toString(16).toUpperCase()}`,
      event_id: `evt_demo_${782041 + i}`,
      customer_id: `customer_${(1284 + i * 37) % 9999}`,
      amount_minor:
        kind === "REVIEW"
          ? 89000
          : [124900, 8450, 22990, 3600, 89000, 12500, 68200][i % 7],
      risk_score: kind === "REJECT" ? 80 : kind === "REVIEW" ? 30 : 0,
      decision: kind,
      matched_rules,
      reason_codes: matched_rules.map((id) => `${id}_MATCH`),
      rules_fingerprint: "demo-policy-7d29c3a1",
      event_time: now - (i * span) / 60 - 14000,
      processed_at: now - (i * span) / 60,
      source_partition: i % 3,
      source_offset: 128400 + i,
    };
  });
  return {
    generatedAt: new Date(now).toISOString(),
    window,
    totals: {
      transactions: approved + review + rejected,
      amountMinor: 92486520,
      approved,
      review,
      rejected,
      finalizationP95: 11257,
    },
    series,
    decisions,
    rules: [
      { id: "R001", matches: 628 },
      { id: "R002", matches: 416 },
      { id: "R003", matches: 284 },
      { id: "R004", matches: 792 },
      { id: "R005", matches: 196 },
    ],
    services: [
      {
        name: "Kafka",
        status: "healthy",
        detail: "Sample Kafka status",
      },
      {
        name: "Apache Flink",
        status: "healthy",
        detail: "Sample Flink status",
      },
      {
        name: "ClickHouse",
        status: "healthy",
        detail: "Sample ClickHouse status",
      },
      {
        name: "Checkpoints",
        status: "healthy",
        detail: "Sample checkpoint status",
      },
    ],
    checkpoint: {
      id: 212,
      duration: 357,
      bytes: 324865,
      completedAt: now - 8000,
    },
    job: { id: "demo-job", state: "RUNNING" },
  };
}
