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
CREATE VIEW IF NOT EXISTS risk.decisions_legacy AS SELECT * FROM risk.decisions FINAL;

-- Append-only deliveries retain every evaluation and every retry. Offsets are provenance only.
CREATE TABLE IF NOT EXISTS risk.evaluations (
  decision_id String, event_id String, transaction_id String, customer_id String,
  amount_minor Int64, currency LowCardinality(String), risk_score UInt8,
  decision LowCardinality(String), matched_rules Array(String), reason_codes Array(String),
  rules_fingerprint String, event_time Int64, processed_at Int64,
  source_partition UInt32, source_offset UInt64, source_topic LowCardinality(String),
  evaluation_id String, engine_version String, policy_id String, policy_version UInt64,
  policy_snapshot String, rule_evidence String, input_sha256 String,
  ingested_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree ORDER BY (evaluation_id, ingested_at);
-- Retry coordinates and processing time never define business revisions.
CREATE OR REPLACE VIEW risk.evaluations_logical AS
SELECT evaluation_id,
  argMin(tuple(decision_id,event_id,transaction_id,customer_id,amount_minor,currency,risk_score,
    decision,matched_rules,reason_codes,rules_fingerprint,event_time,processed_at,source_partition,
    source_offset,source_topic,engine_version,policy_id,policy_version,policy_snapshot,rule_evidence,input_sha256),
    tuple(processed_at,source_topic,source_partition,source_offset)) AS row,
  count() AS delivery_count,
  uniqExact(tuple(decision_id,event_id,transaction_id,customer_id,amount_minor,currency,risk_score,decision,
    matched_rules,reason_codes,rules_fingerprint,event_time,engine_version,policy_id,policy_version,policy_snapshot,rule_evidence,input_sha256)) AS content_variants
FROM risk.evaluations GROUP BY evaluation_id;
-- No lifecycle/revision contract exists in v1. Multiple evaluations for a transaction require review.
-- Work from raw deliveries so a conflicting evaluation cannot conceal another transaction_id.
CREATE OR REPLACE VIEW risk.transaction_integrity AS
SELECT transaction_id, uniqExact(evaluation_id) AS evaluations,
  max(content_variants) AS content_variants
FROM risk.evaluations AS d
INNER JOIN risk.evaluations_logical AS l USING evaluation_id
GROUP BY transaction_id;
CREATE OR REPLACE VIEW risk.decisions_current AS
SELECT evaluation_id, row.1 AS decision_id, row.2 AS event_id, row.3 AS transaction_id,
  row.4 AS customer_id, row.5 AS amount_minor, row.6 AS currency, row.7 AS risk_score,
  row.8 AS decision, row.9 AS matched_rules, row.10 AS reason_codes,
  row.11 AS rules_fingerprint, row.12 AS event_time, row.13 AS processed_at,
  row.14 AS source_partition, row.15 AS source_offset, row.16 AS source_topic,
  row.17 AS engine_version, row.18 AS policy_id, row.19 AS policy_version,
  row.20 AS policy_snapshot, row.21 AS rule_evidence, row.22 AS input_sha256
FROM risk.evaluations_logical
WHERE content_variants = 1 AND row.3 IN
  (SELECT transaction_id FROM risk.transaction_integrity WHERE evaluations = 1 AND content_variants = 1);
CREATE OR REPLACE VIEW risk.integrity_conflicts AS
SELECT * FROM risk.transaction_integrity WHERE evaluations > 1 OR content_variants > 1;
CREATE TABLE IF NOT EXISTS risk.materializer_rejections (
  source_topic LowCardinality(String), source_partition UInt32, source_offset UInt64,
  source_timestamp Int64, error_code LowCardinality(String), payload_sha256 String,
  payload_bytes UInt64, observed_at Int64
) ENGINE = ReplacingMergeTree(observed_at) ORDER BY (source_topic,source_partition,source_offset);
