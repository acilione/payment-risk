export interface RuleEvidence {
  rule_id: string;
  rule_version: number;
  type: string;
  enabled: boolean;
  window_seconds: number;
  threshold: number;
  observed: number;
  eligible: boolean;
  matched: boolean;
  score_contribution: number;
}
export interface InvestigationDecision {
  evaluation_id: string;
  event_id: string;
  transaction_id: string;
  customer_id: string;
  amount_minor: number;
  currency: string;
  risk_score: number;
  decision: string;
  event_time: number;
  processed_at: number;
  engine_version: string;
  policy_id: string;
  policy_version: string | number;
  policy_snapshot: string;
  rules_fingerprint: string;
  rule_evidence: RuleEvidence[];
  source_topic?: string;
  source_partition?: number;
  source_offset?: string;
}
export interface CustomerSummary {
  customer_id: string;
  payments: number;
  alerts: number;
  maxScore: number;
  lastEvent: number;
}
export interface CustomerPage {
  generatedAt: string;
  items: CustomerSummary[];
  nextCursor: string | null;
}
export interface TimelinePage {
  generatedAt: string;
  customer: CustomerSummary | null;
  items: InvestigationDecision[];
  nextCursor: string | null;
}
