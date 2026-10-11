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
