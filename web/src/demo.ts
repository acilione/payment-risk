import type { DecisionKind, Overview, WindowKey } from "./types";

export const ruleCatalog = [
  {
    id: "R001",
    name: "Payment velocity",
    description: "More than 5 payments within 2 minutes.",
    score: 25,
  },
  {
    id: "R002",
    name: "Amount velocity",
    description: "More than €3,000 within 10 minutes.",
    score: 35,
  },
  {
    id: "R003",
    name: "Device diversity",
    description: "At least 3 devices within 15 minutes.",
    score: 20,
  },
  {
    id: "R004",
    name: "Unseen device",
    description: "A device unseen for 30 days with a payment of at least €800.",
    score: 30,
  },
  {
    id: "R005",
    name: "Decline → approval",
    description: "An approval after at least 4 declines within 10 minutes.",
    score: 40,
  },
];

export function demoOverview(window: WindowKey): Overview {
  const now = Date.now();
  const span = { "15m": 900000, "1h": 3600000, "24h": 86400000 }[window];
  const series = Array.from({ length: 30 }, (_, i) => ({
    time: now - span + (i * span) / 29,
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
      rules_fingerprint: "demo-illustrative-policy-7d29c3a1",
      event_time: now - i * 37000 - 14000,
      processed_at: now - i * 37000,
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
        detail: "Illustrative source telemetry",
      },
      {
        name: "Apache Flink",
        status: "healthy",
        detail: "Illustrative processing state",
      },
      {
        name: "ClickHouse",
        status: "healthy",
        detail: "Illustrative analytical storage",
      },
      {
        name: "Checkpoints",
        status: "healthy",
        detail: "Illustrative checkpoint telemetry",
      },
    ],
    checkpoint: {
      id: 212,
      duration: 357,
      bytes: 324865,
      completedAt: now - 8000,
    },
    job: { id: "illustrative-demo-job", state: "RUNNING" },
  };
}
