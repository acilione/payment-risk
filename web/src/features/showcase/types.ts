export type ShowcaseReport = {
  run_id: string;
  status: string;
  updated_at: string;
  source_commit: string;
  source_dirty: boolean;
  generator_version?: string;
  expected_payments?: number;
  acknowledged_deliveries?: number;
  transport_retries?: number;
  control_records?: number;
  archived_records?: number;
  stored_decisions?: number;
  decision_counts?: Record<string, number>;
  verification?: string;
  error?: string;
  storage_unavailable?: boolean;
  reconciliation?: {
    checked_at: string;
    verified: boolean;
    scope: string;
    archived: number;
    evaluated: number;
    missing_archive: number;
    without_decision: number;
    missing_payment_examples?: string[];
    missing_input_examples?: { partition: number; offset: string }[];
  };
  recovery?: {
    stage: string;
    acknowledged_during_outage?: number;
    resume_in_seconds?: number;
    offsets_unchanged?: boolean;
    verification?: string;
    replay?: { records: number; status: string };
  };
  config: {
    seed: number;
    customers: number;
    min_payments: number;
    max_payments: number;
    history_minutes: number;
    publish_rate: number;
    retry_percent: number;
    profiles: Record<string, number>;
  };
  customers?: { id: string; profile: string; payments: number }[];
};
