CREATE DATABASE IF NOT EXISTS risk;
CREATE TABLE IF NOT EXISTS risk.decisions (
  decision_id String, event_id String, transaction_id String, customer_id String,
  amount_minor Int64, currency LowCardinality(String), risk_score UInt8,
  decision LowCardinality(String), matched_rules Array(String), reason_codes Array(String),
  rules_fingerprint String, event_time Int64, processed_at Int64,
  source_partition UInt32, source_offset UInt64
) ENGINE = ReplacingMergeTree(source_offset)
ORDER BY transaction_id;
-- No time partition: retries cannot escape deduplication by crossing a partition boundary.
CREATE VIEW IF NOT EXISTS risk.decisions_current AS SELECT * FROM risk.decisions FINAL;
