export type DecisionKind = "APPROVE" | "REVIEW" | "REJECT";
export type WindowKey = "15m" | "1h" | "24h";
export interface Decision {
  transaction_id: string;
  event_id: string;
  customer_id: string;
  amount_minor: number;
  risk_score: number;
  decision: DecisionKind;
  matched_rules: string[];
  reason_codes: string[];
  rules_fingerprint: string;
  event_time: number;
  processed_at: number;
  source_partition: number;
  source_offset: number;
}
export interface Overview {
  generatedAt: string;
  window: WindowKey;
  totals: {
    transactions: number;
    amountMinor: number;
    approved: number;
    review: number;
    rejected: number;
    finalizationP95: number;
  };
  series: {
    time: number;
    approved: number;
    review: number;
    rejected: number;
  }[];
  rules: { id: string; matches: number }[];
  decisions: Decision[];
  services: {
    name: string;
    status: "healthy" | "unavailable" | "idle";
    detail: string;
  }[];
  checkpoint: {
    id: number;
    duration: number;
    bytes: number;
    completedAt: number;
  } | null;
  job: { id: string; state: string } | null;
}
