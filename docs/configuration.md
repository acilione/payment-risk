# Configuration reference

Application values are read from environment variables. Flink runtime/checkpoint settings are provided by Compose or Helm. Secret values are never included in ordinary config files.

| Variable | Default |
|---|---|
| KAFKA_BOOTSTRAP_SERVERS | localhost:29092 |
| SCHEMA_REGISTRY_URL | http://localhost:28081/apis/ccompat/v7 |
| KAFKA_GROUP | payment-risk-v1 (payments/rules suffixes added) |
| TRANSACTIONAL_PREFIX | payment-risk-local-v1; production must override |
| PAYMENTS_TOPIC / RULES_TOPIC / DECISIONS_TOPIC | payments.raw / risk.rules / risk.decisions |
| DLQ_TOPIC / LATE_TOPIC | payments.dlq / payments.late |
| STARTUP_OFFSETS | earliest; production requires committed |
| OUT_OF_ORDER_MS / IDLE_TIMEOUT_MS | 10000 / 60000 |
| DEDUP_TTL_MS | 86400000 |
| HISTORY_MS / DEVICE_HISTORY_MS | 3600000 / 2592000000 |
| MAX_EVENTS_PER_CUSTOMER / MAX_DEVICES_PER_CUSTOMER | 100000 / 10000 |
| REVIEW_THRESHOLD / REJECT_THRESHOLD | 30 / 70 |
| APP_ENVIRONMENT | local |
| KAFKA_PROPERTIES_FILE | Optional mounted Java properties; required TLS settings in production |
| REGISTRY_BEARER_TOKEN | Optional secret |
| CLICKHOUSE_URL / CLICKHOUSE_USER | localhost:28123 / risk; Compose supplies internal URL |
| CLICKHOUSE_PASSWORD | Required secret |
| MATERIALIZER_GROUP | risk-clickhouse-v1 |
| KAFKA_REPLICATION_FACTOR / KAFKA_MIN_ISR | 1 / 1 for local bootstrap; use 3 / 2 in production |

Retained history must cover packaged windows (at least fifteen minutes and thirty days of device history). Rules may request shorter horizons dynamically. State history is capped at one year by configuration validation. Count/score settings reject overflow and invalid ordering. Topic names must be distinct and valid Kafka names.

Runtime workers only look up existing registry schemas. `PlatformCli bootstrap` creates topics and registers schema versions and should run under a separate provisioning identity. The registry enforces backward compatibility; CI additionally checks all historical writer schemas. Register contracts before submitting a job with a new output schema.

The production chart consumes externally provisioned Kafka, registry and object storage. Mount SASL/TLS properties through the existing Secret and use workload identity for S3. Keep the same topic mapping in job, generator and materializer deployments.
