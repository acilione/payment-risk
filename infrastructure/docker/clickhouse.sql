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
CREATE OR REPLACE VIEW risk.evaluation_conflicts AS
SELECT evaluation_id FROM risk.evaluations
GROUP BY evaluation_id
HAVING uniqExact(tuple(decision_id,event_id,transaction_id,customer_id,amount_minor,currency,risk_score,decision,
  matched_rules,reason_codes,rules_fingerprint,event_time,engine_version,policy_id,policy_version,policy_snapshot,rule_evidence,input_sha256)) > 1;
CREATE OR REPLACE VIEW risk.transaction_integrity AS
SELECT transaction_id, uniqExact(evaluation_id) AS evaluations,
  1 + max(evaluation_id IN (SELECT evaluation_id FROM risk.evaluation_conflicts)) AS content_variants
FROM risk.evaluations
GROUP BY transaction_id;
CREATE OR REPLACE VIEW risk.integrity_conflicts AS
SELECT * FROM risk.transaction_integrity WHERE evaluations > 1 OR content_variants > 1;
-- Group keys let customer and event-ID predicates apply before full evidence aggregation.
CREATE OR REPLACE VIEW risk.decisions_current AS
SELECT evaluation_id, event_id, customer_id, event_time,
  row.1 AS decision_id, row.2 AS transaction_id, row.3 AS amount_minor,
  row.4 AS currency, row.5 AS risk_score, row.6 AS decision, row.7 AS matched_rules,
  row.8 AS reason_codes, row.9 AS rules_fingerprint, row.10 AS processed_at,
  row.11 AS source_partition, row.12 AS source_offset, row.13 AS source_topic,
  row.14 AS engine_version, row.15 AS policy_id, row.16 AS policy_version,
  row.17 AS policy_snapshot, row.18 AS rule_evidence, row.19 AS input_sha256
FROM (
  SELECT evaluation_id, event_id, customer_id, event_time,
    argMin(tuple(decision_id,transaction_id,amount_minor,currency,risk_score,decision,
      matched_rules,reason_codes,rules_fingerprint,processed_at,source_partition,
      source_offset,source_topic,engine_version,policy_id,policy_version,policy_snapshot,rule_evidence,input_sha256),
      tuple(processed_at,source_topic,source_partition,source_offset)) AS row
  FROM risk.evaluations
  WHERE transaction_id NOT IN (SELECT transaction_id FROM risk.integrity_conflicts)
  GROUP BY customer_id, event_time, event_id, evaluation_id
);
-- Small analytical rows avoid aggregating policy snapshots and evidence for charts and counts.
-- No precomputed sums: each query resolves retries before counting business payments.
CREATE OR REPLACE VIEW risk.decisions_summary AS
SELECT evaluation_id, customer_id, event_time,
  row.1 AS transaction_id, row.2 AS event_id, row.3 AS amount_minor,
  row.4 AS risk_score, row.5 AS decision, row.6 AS matched_rules,
  row.7 AS reason_codes, row.8 AS rules_fingerprint, row.9 AS processed_at,
  row.10 AS source_partition, row.11 AS source_offset
FROM (
  SELECT evaluation_id, customer_id, event_time,
    argMin(tuple(transaction_id,event_id,amount_minor,risk_score,decision,matched_rules,
      reason_codes,rules_fingerprint,processed_at,source_partition,source_offset),
      tuple(processed_at,source_topic,source_partition,source_offset)) AS row
  FROM risk.evaluations
  WHERE transaction_id NOT IN (SELECT transaction_id FROM risk.integrity_conflicts)
  GROUP BY customer_id, event_time, evaluation_id
);
CREATE TABLE IF NOT EXISTS risk.materializer_rejections (
  source_topic LowCardinality(String), source_partition UInt32, source_offset UInt64,
  source_timestamp Int64, error_code LowCardinality(String), payload_sha256 String,
  payload_bytes UInt64, observed_at Int64
) ENGINE = ReplacingMergeTree(observed_at) ORDER BY (source_topic,source_partition,source_offset);

-- Independent source archive: exact wire data, including late, invalid and duplicate deliveries.
-- Retries may create physical copies. Source coordinates identify a Kafka record, not a payment.
CREATE TABLE IF NOT EXISTS risk.payment_ingress (
  source_topic LowCardinality(String), source_partition UInt32, source_offset UInt64,
  source_timestamp Int64, timestamp_type LowCardinality(String),
  key_is_null Bool, value_is_null Bool, key_base64 String CODEC(ZSTD),
  payload_base64 String CODEC(ZSTD), payload_bytes UInt64, payload_sha256 String,
  headers_json String CODEC(ZSTD), archived_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree ORDER BY (source_topic, source_partition, source_offset);

-- Delivery coordinates preserve retries without hiding contradictory side-output evidence.
CREATE TABLE IF NOT EXISTS risk.processing_outcomes (
  source_topic LowCardinality(String), source_partition UInt32, source_offset UInt64,
  outcome LowCardinality(String), input_topic String, input_partition Int64, input_offset Int64,
  payload_sha256 String, event_id String, input_sha256 String,
  reason String, watermark Int64, lateness_ms Int64, observed_at Int64,
  ingested_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree ORDER BY (source_topic, source_partition, source_offset);
