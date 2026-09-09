# Real-Time Payment Risk Platform
## Functional & Technical Implementation Reference

**Document status:** Implementation baseline  
**Target audience:** Developer / reviewer / interviewer  
**Primary technology:** Apache Flink DataStream API  
**Baseline Flink version:** 2.2.1  
**Language:** Java 17  
**Last reviewed:** 2026-09-09  

---

## 1. Purpose

This document defines the functional and technical specification for a portfolio-grade, production-oriented real-time payment risk platform built with Apache Flink.

The project is intended to demonstrate practical competence in:

- stateful stream processing;
- event-time processing and watermarks;
- Kafka source/sink integration;
- deterministic risk evaluation;
- dynamic rule distribution with Broadcast State;
- state TTL and cleanup;
- duplicate-event handling;
- late and malformed event handling;
- checkpointing, recovery, and savepoints;
- exactly-once delivery to Kafka;
- schema evolution;
- observability and alerting;
- automated testing;
- failure injection;
- containerized local development;
- Kubernetes deployment using the Flink Kubernetes Operator;
- CI/CD and infrastructure-as-code;
- measurable performance and documented operational trade-offs.

This document is the implementation reference. Code, configuration, infrastructure, tests, dashboards, and README documentation should remain consistent with it.

---

## 2. Product Summary

The platform consumes a continuous stream of payment transactions, evaluates each transaction using current customer activity and dynamically managed risk rules, and publishes an explainable risk decision.

The platform must operate continuously and recover safely from process or infrastructure failures without corrupting risk state.

### 2.1 Core flow

```text
                           ┌─────────────────────┐
                           │  Risk Rule Producer │
                           └──────────┬──────────┘
                                      │
                                      ▼
                                Kafka: risk.rules
                                      │
                                      ▼
┌──────────────────┐        ┌───────────────────────────┐
│ Payment Generator│───────►│ Kafka: payments.raw       │
└──────────────────┘        └─────────────┬─────────────┘
                                          │
                                          ▼
                              ┌──────────────────────────┐
                              │      Apache Flink        │
                              │                          │
                              │ deserialize / validate   │
                              │ timestamp + watermarks   │
                              │ deduplicate              │
                              │ keyBy(customer_id)       │
                              │ stateful risk features   │
                              │ dynamic rule evaluation  │
                              │ risk decision            │
                              └───────┬────────┬─────────┘
                                      │        │
                         valid output │        │ side outputs
                                      │        │
                                      ▼        ▼
                       Kafka: risk.decisions  Kafka:
                                      │        payments.dlq
                                      │        payments.late
                                      ▼
                                ClickHouse
                                      │
                                      ▼
                                   Grafana
```

---

## 3. Goals

### G-01 — Correct stateful processing

Risk decisions must use recent customer behavior rather than evaluating a transaction in isolation.

### G-02 — Event-time correctness

Business-time calculations must use `event_time`, not arrival/processing time, so moderately out-of-order events are handled correctly.

### G-03 — Explainable decisions

Every output decision must include a risk score, decision classification, matched rule IDs, and the rule-set version used.

### G-04 — Runtime rule updates

Risk rules must be updateable without redeploying or restarting the Flink job.

### G-05 — Failure recovery

The job must recover from TaskManager/process failures using checkpoints while maintaining consistent application state.

### G-06 — Production visibility

The system must expose sufficient metrics and logs to detect lag, backpressure, checkpoint problems, invalid input, late input, duplicates, and risk-processing anomalies.

### G-07 — Reproducibility

A reviewer must be able to run the project locally using documented commands and reproduce integration and failure scenarios.

### G-08 — Safe deployment evolution

Stateful job upgrades must use explicit operator UIDs and savepoints.

---

## 4. Non-Goals

The initial implementation will not attempt to provide:

- a real banking or payment authorization system;
- a machine-learning fraud model;
- cardholder personal data;
- PCI-DSS certification;
- a multi-region active-active architecture;
- a generic CEP/rules language;
- a full web-based rule-management product;
- a microservice for every component;
- guaranteed exactly-once persistence to every downstream system.

ClickHouse is an analytical materialization target. The Kafka decision topic is the authoritative streaming output.

---

## 5. Actors and External Systems

### 5.1 Actors

| Actor | Responsibility |
|---|---|
| Payment Producer | Produces transaction events |
| Risk Analyst | Defines and updates risk rules |
| Platform Operator | Deploys and operates Kafka/Flink infrastructure |
| Application Developer | Changes stream processing logic |
| Data Consumer | Consumes `risk.decisions` |
| Reviewer | Runs and evaluates the portfolio project |

### 5.2 Systems

| System | Purpose |
|---|---|
| Apache Kafka | Durable event transport |
| Apache Flink | Stateful real-time computation |
| Schema Registry | Avro schema versioning/compatibility |
| MinIO | Local S3-compatible checkpoint/savepoint storage |
| S3-compatible object store | Production checkpoint/savepoint storage |
| ClickHouse | Analytical decision store |
| Prometheus | Metrics collection |
| Grafana | Operational and business dashboards |
| Kubernetes | Runtime platform |
| Flink Kubernetes Operator | Flink lifecycle management |
| GitHub Actions | CI/CD |

---

# PART I — FUNCTIONAL SPECIFICATION

## 6. Functional Requirements

Requirements use the identifier format `FR-xxx`.

### FR-001 — Consume transaction events

The application must consume transaction events from Kafka topic:

```text
payments.raw
```

The source consumer group must be configurable.

### FR-002 — Deserialize using a versioned schema

Transaction messages must use Avro with Schema Registry.

A deserialization failure must not terminate the whole job when the original Kafka record can be classified and routed safely.

### FR-003 — Validate mandatory fields

At minimum, reject records when:

- `event_id` is missing/blank;
- `transaction_id` is missing/blank;
- `customer_id` is missing/blank;
- `merchant_id` is missing/blank;
- `event_time` is missing or invalid;
- `amount <= 0`;
- `currency` is missing or invalid;
- `country` is missing/invalid when required by the test fixture.

Rejected records must be emitted to `payments.dlq` with an error code.

### FR-004 — Use event time

Risk-window calculations must use the transaction's `event_time`.

The source must assign watermarks using a bounded-out-of-orderness policy.

Initial default:

```text
max_out_of_orderness = 10 seconds
source_idle_timeout  = 60 seconds
```

Both must be configurable.

### FR-005 — Deduplicate events

Events sharing the same `event_id` within the configured deduplication retention period must be treated as duplicates.

Default deduplication retention:

```text
24 hours
```

Duplicate records must:

- not contribute a second time to customer state;
- not produce a second normal risk decision;
- increment a duplicate metric.

Optional diagnostic output:

```text
payments.duplicates
```

is allowed but not required for MVP.

### FR-006 — Partition customer state

Risk processing must key the validated transaction stream by:

```text
customer_id
```

All customer-scoped velocity and behavior state must therefore be managed as Flink keyed state.

### FR-007 — Evaluate transaction-count velocity

The application must support a rule that evaluates transaction count within a configurable event-time horizon.

Example default:

```text
R001: transaction_count(2m) > 5
```

### FR-008 — Evaluate amount velocity

The application must support cumulative payment amount within a configurable event-time horizon.

Example:

```text
R002: total_amount(10m) > 3000 EUR-equivalent
```

For MVP, either:

1. only generate EUR transactions, or
2. treat thresholds per currency.

Do not silently sum unrelated currencies.

### FR-009 — Detect recent unique devices

The application must support rules based on the number of unique devices used within a configurable horizon.

Example:

```text
R003: unique_devices(15m) >= 3
```

### FR-010 — Detect a new/recently unseen device

The application must support a rule that identifies when a device has not been observed for the customer within a configured history horizon.

Example:

```text
R004:
new_device(30d) AND amount >= 800
```

The exact retained device-history duration must be configurable.

### FR-011 — Detect decline/approval sequences

The system should support a stateful rule such as:

```text
R005:
at least 4 DECLINED transactions within 10m
followed by an APPROVED transaction
```

This requirement is recommended for the full portfolio version and optional for the first MVP.

### FR-012 — Consume dynamic rules

The application must consume rule updates from:

```text
risk.rules
```

Rules must be distributed to all parallel risk-evaluation subtasks using Flink Broadcast State.

### FR-013 — Version rules

Every accepted rule update must include:

- `rule_id`;
- `version`;
- `enabled`;
- rule-specific parameters;
- `updated_at`.

For a given `rule_id`, stale versions must not overwrite newer rule state.

### FR-014 — Disable rules at runtime

A rule can be disabled via a rule update. Disabled rules must stop contributing to risk decisions without requiring a deployment.

### FR-015 — Produce deterministic risk score

Matched rules contribute configured score weights.

Example:

```text
R001 +25
R002 +35
R003 +20
R004 +30
R005 +40
```

The total may be capped at 100.

Given the same:

- transaction stream;
- event ordering semantics;
- initial state;
- rule versions;

the resulting decision must be deterministic.

### FR-016 — Classify decision

Default score mapping:

| Score | Decision |
|---:|---|
| 0–29 | `APPROVE` |
| 30–69 | `REVIEW` |
| 70–100 | `REJECT` |

Thresholds should be configurable but must be versioned or deployment-controlled.

### FR-017 — Explain each decision

Every decision must include:

- input transaction ID;
- customer ID;
- score;
- decision;
- matched rule IDs;
- human-readable or machine-readable reason codes;
- active rule-set fingerprint/version;
- event time;
- processing time.

### FR-018 — Handle late records

A transaction whose event timestamp is behind the application's accepted event-time boundary must be handled according to a documented policy.

Initial policy:

- events within normal out-of-order allowance are processed;
- events classified as too late for correct risk-state mutation are emitted to `payments.late`;
- late events must increment metrics;
- very late events must not silently mutate already-finalized state.

The exact implementation depends on the chosen state/timer design and must be tested.

### FR-019 — Route malformed records

Malformed or invalid records must be emitted to:

```text
payments.dlq
```

The DLQ record must include:

- source topic;
- source partition;
- source offset where available;
- ingestion timestamp;
- raw payload or safe representation;
- error code;
- error message;
- schema/version metadata if available.

### FR-020 — Publish risk decisions

Normal output topic:

```text
risk.decisions
```

Kafka message key:

```text
customer_id
```

or, if downstream partition locality requires otherwise:

```text
transaction_id
```

The chosen key must be documented and consistent.

Recommended default: `customer_id`.

### FR-021 — Materialize decisions

A separate sink/consumer path must store risk decisions in ClickHouse for analytics and dashboarding.

The ClickHouse persistence layer must use `transaction_id` as an idempotency/business key or otherwise tolerate replay.

### FR-022 — Generate synthetic load

The project must contain a load generator capable of producing:

- normal transactions;
- high-velocity customer bursts;
- high-value transactions;
- device changes;
- duplicate events;
- out-of-order events;
- deliberately late events;
- malformed events.

### FR-023 — Support failure demonstrations

The repository must expose commands/scripts to demonstrate at least:

- TaskManager termination;
- duplicate injection;
- late-event injection;
- high-load spike;
- controlled savepoint;
- restart/restore.

---

## 7. User Stories

### US-01 — Risk analyst updates a rule

As a risk analyst, I want to publish an updated threshold so that risk behavior changes without deploying the Flink application.

Acceptance:

1. Existing job remains running.
2. New rule version appears in Broadcast State.
3. Subsequent eligible transactions use the new version.
4. Decision contains the new rule version/fingerprint.
5. Old rule update cannot overwrite a newer version.

### US-02 — Operator recovers from a worker failure

As an operator, I want a failed Flink worker to recover automatically so transaction processing resumes from consistent checkpointed state.

Acceptance:

1. Job becomes unhealthy/failing.
2. Flink restarts affected execution.
3. State is restored from a successful checkpoint.
4. Kafka source resumes consistently.
5. No committed duplicate output is visible to `read_committed` consumers when Kafka exactly-once mode is enabled.

### US-03 — Reviewer sees why a transaction was rejected

As a reviewer, I want a decision to contain matched rules and reasons so the result is explainable.

### US-04 — Operator detects a stuck stream

As an operator, I want metrics for Kafka lag, records in/out, watermarks, backpressure, and checkpoints so I can determine why output is delayed.

### US-05 — Developer upgrades stateful code

As a developer, I want to trigger a savepoint and deploy a compatible version of the job without losing keyed state.

---

## 8. Domain Model

### 8.1 Transaction

Logical fields:

| Field | Type | Required | Description |
|---|---|---:|---|
| `event_id` | string/UUID | yes | Unique producer event ID |
| `transaction_id` | string/UUID | yes | Business transaction ID |
| `customer_id` | string | yes | Customer key |
| `merchant_id` | string | yes | Merchant identifier |
| `amount_minor` | long | yes | Amount in minor currency units |
| `currency` | string | yes | ISO-4217 code |
| `country` | string | yes | ISO-3166 alpha-2 |
| `device_id` | string | yes | Synthetic device identifier |
| `status` | enum | yes | `APPROVED`, `DECLINED` |
| `event_time` | timestamp | yes | Business event time |
| `producer_time` | timestamp | no | Producer emission time |
| `schema_version` | int | no | Optional envelope metadata |

Use integer minor units for currency to avoid floating-point monetary errors.

Example:

```json
{
  "event_id": "evt_d1a7bc1d",
  "transaction_id": "txn_91827",
  "customer_id": "cust_1023",
  "merchant_id": "merchant_87",
  "amount_minor": 82050,
  "currency": "EUR",
  "country": "IT",
  "device_id": "device_883",
  "status": "APPROVED",
  "event_time": "2026-09-09T10:31:43.281Z",
  "producer_time": "2026-09-09T10:31:43.450Z"
}
```

### 8.2 Risk Rule

Common fields:

| Field | Type |
|---|---|
| `rule_id` | string |
| `version` | long |
| `type` | enum |
| `enabled` | boolean |
| `score` | int |
| `parameters` | typed record/map |
| `updated_at` | timestamp |

Suggested `type` values:

```text
TX_COUNT_VELOCITY
AMOUNT_VELOCITY
UNIQUE_DEVICE_VELOCITY
NEW_DEVICE_HIGH_AMOUNT
DECLINE_THEN_APPROVAL
```

Avoid building a fully generic expression engine for the first implementation. Prefer typed rule classes/strategies.

Example:

```json
{
  "rule_id": "R001",
  "version": 4,
  "type": "TX_COUNT_VELOCITY",
  "enabled": true,
  "score": 25,
  "parameters": {
    "window_seconds": 120,
    "threshold": 5
  },
  "updated_at": "2026-09-09T10:30:00Z"
}
```

### 8.3 Risk Decision

| Field | Type |
|---|---|
| `decision_id` | string |
| `transaction_id` | string |
| `customer_id` | string |
| `risk_score` | int |
| `decision` | enum |
| `matched_rules` | array |
| `reason_codes` | array |
| `rules_fingerprint` | string |
| `event_time` | timestamp |
| `processed_at` | timestamp |

Example:

```json
{
  "decision_id": "risk_txn_91827",
  "transaction_id": "txn_91827",
  "customer_id": "cust_1023",
  "risk_score": 82,
  "decision": "REJECT",
  "matched_rules": ["R001", "R004", "R003"],
  "reason_codes": [
    "HIGH_2_MIN_TRANSACTION_COUNT",
    "NEW_DEVICE_HIGH_AMOUNT",
    "MULTIPLE_RECENT_DEVICES"
  ],
  "rules_fingerprint": "rules-17-9c70f4",
  "event_time": "2026-09-09T10:31:43.281Z",
  "processed_at": "2026-09-09T10:31:44.012Z"
}
```

### 8.4 Dead Letter Record

```json
{
  "error_id": "err_...",
  "source_topic": "payments.raw",
  "source_partition": 3,
  "source_offset": 883102,
  "error_code": "INVALID_AMOUNT",
  "error_message": "amount_minor must be greater than zero",
  "raw_payload": "...",
  "observed_at": "2026-09-09T10:31:44Z"
}
```

### 8.5 Late Event Record

Include:

- original transaction;
- watermark/late-boundary information if available;
- lateness duration;
- processing timestamp;
- reason.

---

## 9. Kafka Topic Specification

| Topic | Key | Value | Producer | Consumer |
|---|---|---|---|---|
| `payments.raw` | `customer_id` | Transaction Avro | generator | Flink |
| `risk.rules` | `rule_id` | RiskRule Avro | rule CLI | Flink |
| `risk.decisions` | `customer_id` | RiskDecision Avro | Flink | analytics/materializer |
| `payments.dlq` | `event_id` or null | DLQ Avro/JSON | Flink | operator |
| `payments.late` | `customer_id` | LateEvent Avro | Flink | reconciliation |

Optional:

```text
payments.duplicates
```

### 9.1 Topic configuration principles

- Use multiple partitions for `payments.raw`.
- Partition by customer to improve natural key locality, while still relying on Flink `keyBy`.
- Configure replication appropriate to environment.
- Do not enable infinite retention blindly for all topics.
- Keep `risk.rules` compactable if rule history semantics support it.
- Ensure transaction timeout is compatible with Flink checkpoint/restart duration when Kafka exactly-once output is enabled.

---

# PART II — TECHNICAL SPECIFICATION

## 10. Technology Baseline

### 10.1 Runtime

| Component | Baseline |
|---|---|
| Java | 17 |
| Apache Flink | 2.2.1 |
| Flink API | DataStream API |
| Kafka connector | Flink-compatible connector version pinned in Maven |
| Serialization | Avro |
| Build | Maven |
| Container runtime | Docker |
| Orchestration | Kubernetes |
| Flink operator | Current compatible Apache Flink Kubernetes Operator release, explicitly pinned |

### 10.2 Why Flink 2.2.1

Apache Flink 2.3.0 is the newest stable core release as of this document date, while 2.2.1 is also a stable release. This project intentionally pins Flink 2.2.1 so connector and deployment compatibility can be validated as a coherent baseline.

Do not upgrade Flink, connectors, the operator, Java, or Kafka independently without updating the dependency matrix and rerunning integration/recovery tests.

---

## 11. Logical Flink Topology

Recommended logical topology:

```text
Kafka payments.raw
      │
      ▼
[01] KafkaSource<TransactionEnvelope>
      uid: transaction-source-v1
      │
      ▼
[02] Deserialize + Validate
      uid: transaction-validation-v1
      │
      ├──────────── invalid ──────────────► DLQ sink
      │
      ▼
[03] Timestamp/Watermark Strategy
      uid/source-associated where possible
      │
      ▼
[04] Deduplicate by event_id
      uid: transaction-dedup-v1
      │
      ▼
[05] keyBy(customer_id)
      │
      ▼
[06] Stateful Feature/Risk Processor
      uid: customer-risk-state-v1
      ▲
      │ broadcast
      │
Kafka risk.rules
      │
      ▼
[07] Rule Source + validation
      uid: rule-source-v1
      │
      ▼
[08] Broadcast State
      uid: rule-broadcast-v1
      │
      ▼
      connected to [06]
      │
      ├──────────── too late ─────────────► late sink
      │
      ▼
[09] Decision serializer
      │
      ▼
[10] KafkaSink risk.decisions
      uid: risk-decision-sink-v1
```

Physical operator chaining can differ from this logical view. Stable UIDs must be assigned to stateful operators and other important restore points.

---

## 12. Source Design

Use modern Flink Kafka source APIs compatible with the pinned connector.

Source configuration must include:

- bootstrap servers;
- topic;
- consumer group ID;
- startup offset strategy;
- deserializer;
- source name;
- stable UID.

### 12.1 Startup policy

Development:

```text
earliest
```

may be convenient.

Production-like deployment should explicitly choose one of:

- committed group offsets;
- earliest;
- timestamp-specific restore/bootstrap.

This choice must not be implicit.

### 12.2 Source metadata

Where possible preserve:

- Kafka topic;
- partition;
- offset;
- Kafka timestamp;

through validation so DLQ records are operationally useful.

---

## 13. Event Time and Watermarks

### 13.1 Strategy

Initial implementation:

```java
WatermarkStrategy
    .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(10))
    .withTimestampAssigner(
        (event, previousTimestamp) -> event.eventTime().toEpochMilli()
    )
    .withIdleness(Duration.ofSeconds(60));
```

Values must be configuration-driven.

### 13.2 Rationale

Event-time semantics are necessary because:

- Kafka partitions may not deliver globally ordered events;
- producers may retry;
- network latency varies;
- test scenarios deliberately inject out-of-order data.

Idle partition handling is required so inactive source partitions do not indefinitely hold back downstream watermark progress.

### 13.3 Late-event policy

The implementation must define precisely when an event can no longer be incorporated into each maintained feature.

Do not conflate:

```text
out-of-order
```

with:

```text
too late to safely affect finalized state
```

Tests must cover both.

---

## 14. State Design

### 14.1 State backend

Production-like default:

```yaml
state.backend.type: rocksdb
execution.checkpointing.storage: filesystem
execution.checkpointing.dir: s3://<bucket>/flink/checkpoints
```

Local:

```text
S3-compatible MinIO
```

Use `EmbeddedRocksDBStateBackend` via configuration rather than hardcoding it in application code unless a specific implementation reason exists.

Enable incremental checkpoints if validated beneficial for the workload.

### 14.2 Customer keyed state

Possible state model:

```text
ValueState / MapState:
  dedup/event IDs
  recent devices
  recent transaction timestamps
  recent amounts
  decline history
  timer bookkeeping
```

Avoid storing arbitrary unbounded raw history.

### 14.3 Recommended data structures

For portfolio clarity, prefer explicit state structures over hidden magic.

Example conceptual model:

```text
MapState<Long, TransactionBucket> minuteBuckets
MapState<String, Long> deviceLastSeen
ValueState<Long> lastCleanupTimer
```

Where:

```text
minute bucket ->
  transaction_count
  total_amount_minor_by_currency
  declined_count
```

A bucketed model can be more state-efficient than storing every complete transaction, but implementation complexity must remain reasonable.

### 14.4 State TTL

Use Flink State TTL where it correctly matches retention semantics.

Example:

```java
StateTtlConfig ttlConfig =
    StateTtlConfig
        .newBuilder(Duration.ofHours(24))
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .build();
```

Important:

State TTL is a retention/cleanup mechanism, not a substitute for business-time window correctness.

Expiry behavior and cleanup are not necessarily identical to event-time timers. Use event-time timers where deterministic event-time cleanup/evaluation is required.

---

## 15. Deduplication Design

### 15.1 Key choice

Deduplication is based on:

```text
event_id
```

A simple architecture may implement:

```text
keyBy(event_id)
```

before customer processing, then re-key by customer.

Alternative: preserve a customer-scoped dedup map if producer guarantees `event_id` uniqueness per customer.

The selected design must be documented.

### 15.2 Default

Recommended implementation for clarity:

```text
validated stream
  -> keyBy(event_id)
  -> duplicate filter using ValueState + TTL
  -> keyBy(customer_id)
  -> risk processing
```

### 15.3 Retention

Default:

```text
24h
```

The business guarantee is therefore:

> duplicates are suppressed when the same `event_id` is replayed within the configured deduplication horizon.

Do not claim permanent global deduplication.

---

## 16. Dynamic Rule Architecture

### 16.1 Rule stream

```text
Kafka risk.rules
   ↓
deserialize
   ↓
validate version + payload
   ↓
broadcast(MapStateDescriptor<String, RiskRule>)
```

### 16.2 Connection

Use a broadcast-connected keyed stream, for example conceptually:

```java
BroadcastStream<RiskRule> rules = ruleStream.broadcast(ruleStateDescriptor);

BroadcastConnectedStream<Transaction, RiskRule> connected =
    keyedTransactions.connect(rules);
```

Processing should use an appropriate broadcast-state process function.

### 16.3 Rule update semantics

For each rule ID:

```text
if incoming.version > current.version:
    accept
else:
    ignore + metric
```

Rule deletion should be modeled as:

```text
enabled = false
```

rather than physically deleting historical semantics unexpectedly.

### 16.4 Rules fingerprint

Each decision should contain a stable fingerprint representing the active rule state used for evaluation.

A simple implementation:

1. sort active `(rule_id, version)` pairs;
2. serialize canonical representation;
3. hash it;
4. publish a shortened hash.

Example:

```text
rules-9c70f4a8
```

---

## 17. Risk Engine Design

### 17.1 Rule interface

Suggested shape:

```java
public interface RiskRuleEvaluator {
    RuleType type();

    Optional<RuleMatch> evaluate(
        Transaction transaction,
        CustomerRiskFeatures features,
        RiskRule rule
    );
}
```

Keep Flink state access primarily in the processing layer. Keep pure decision logic testable as ordinary Java where possible.

### 17.2 Processing sequence

For each non-duplicate transaction:

1. determine event time;
2. reject/reroute if too late under policy;
3. read relevant customer state;
4. compute features representing history before/current transaction;
5. evaluate active rules;
6. calculate score;
7. create decision;
8. update customer state consistently;
9. register cleanup timers as necessary;
10. emit decision.

Whether step 5 evaluates before or after incorporating the current transaction must be explicit per feature. Tests must enforce it.

### 17.3 Score model

```text
score = min(100, sum(matched_rule.score))
```

No hidden random behavior.

### 17.4 Decision mapping

```java
if (score >= rejectThreshold) REJECT
else if (score >= reviewThreshold) REVIEW
else APPROVE
```

---

## 18. Timers and Cleanup

Use event-time timers when state must be revisited or cleaned according to business event time.

Examples:

- expire a transaction bucket;
- expire sequence state;
- finalize window-sensitive behavior.

Avoid registering one timer per event if bucket-level timers can provide equivalent correctness with much less state/timer pressure.

### 18.1 Timer invariant

A timer must have a documented purpose and corresponding test.

### 18.2 Cleanup invariant

No customer should cause unbounded retained state under normal configured workload.

Load tests must include long-running/high-cardinality scenarios sufficient to detect obvious state leaks.

---

## 19. Kafka Output and Delivery Semantics

### 19.1 Decision sink

Use Kafka Sink with:

```text
DeliveryGuarantee.EXACTLY_ONCE
```

and checkpointing enabled.

Configure a unique transactional ID prefix per independently running application/job.

Example concept:

```text
payment-risk-prod-
```

### 19.2 Consumer requirement

Consumers that require transactional exactly-once visibility must use:

```text
isolation.level=read_committed
```

### 19.3 Important guarantee wording

The project documentation must distinguish:

1. **Flink state consistency** — checkpointed state can be recovered consistently.
2. **Kafka source recovery** — source progress participates in checkpoint/recovery semantics.
3. **Kafka output delivery** — transactional Kafka sink can provide exactly-once visibility.
4. **Business deduplication** — producer-level repeated `event_id` is independently handled by dedup state.
5. **ClickHouse materialization** — must be replay-safe/idempotent; do not automatically claim end-to-end exactly-once to ClickHouse.

---

## 20. Checkpointing

Recommended starting configuration:

```yaml
execution.checkpointing.interval: 30s
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.timeout: 2min
execution.checkpointing.min-pause: 10s
execution.checkpointing.max-concurrent-checkpoints: 1
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
```

These are baseline values, not guaranteed optimal production values.

Tune using observed:

- checkpoint duration;
- state size;
- backpressure;
- recovery requirements;
- Kafka transaction timeout.

### 20.1 Invariant

Kafka transaction timeout must exceed the realistic time an uncommitted transaction may remain open, considering checkpoint duration and restart/recovery behavior.

---

## 21. Savepoints and Upgrades

### 21.1 Stable UIDs

Stateful operators must use explicit IDs:

```java
.uid("customer-risk-state-v1")
```

Do not rely on generated operator IDs for stateful upgrade compatibility.

### 21.2 Upgrade flow

```text
running v1
   │
   ├── trigger savepoint
   ▼
stable savepoint stored in object storage
   │
   ├── deploy v2
   ▼
restore v2 from savepoint
   │
   ├── smoke/integration checks
   ▼
continue processing
```

### 21.3 State schema evolution

Changes to state classes/serializers must be treated as migration-sensitive.

For every stateful model change:

- review serializer compatibility;
- test restore from a previous savepoint fixture;
- document migration strategy.

---

## 22. Restart Strategy

Use a bounded restart strategy suitable for transient failures.

Example conceptual configuration:

```text
restart-strategy.type = exponential-delay
```

or a fixed-delay strategy for simpler demonstration.

Acceptance test:

- kill one TaskManager;
- verify restart;
- verify checkpoint restore;
- verify output continuity;
- verify no visible duplicate committed decisions under the exactly-once Kafka consumption model.

---

## 23. Error Handling

### 23.1 Error classes

Define explicit categories:

```text
DESERIALIZATION_ERROR
SCHEMA_ERROR
MISSING_REQUIRED_FIELD
INVALID_AMOUNT
INVALID_CURRENCY
INVALID_EVENT_TIME
UNKNOWN_RULE_TYPE
INVALID_RULE_CONFIGURATION
STALE_RULE_VERSION
TOO_LATE
INTERNAL_PROCESSING_ERROR
```

### 23.2 Policy

| Error | Policy |
|---|---|
| malformed transaction | DLQ |
| invalid business field | DLQ |
| duplicate | suppress + metric |
| too-late event | late topic |
| invalid rule update | reject + metric/log |
| stale rule update | ignore + metric |
| transient Kafka error | connector/restart behavior |
| unexpected deterministic code bug | fail job rather than silently corrupt state |

Do not catch every exception and continue. Unknown state-corrupting errors should fail fast so Flink recovery can restore a consistent checkpoint.

---

## 24. Serialization and Schema Evolution

### 24.1 Avro

Use Avro schemas committed under:

```text
schemas/
```

Suggested files:

```text
transaction.avsc
risk-rule.avsc
risk-decision.avsc
dead-letter.avsc
late-event.avsc
```

### 24.2 Compatibility

Configure Schema Registry compatibility intentionally.

Recommended starting position:

```text
BACKWARD
```

or stricter if the chosen schema evolution workflow supports it.

CI must test schema compatibility before merge.

### 24.3 Rules

Do not encode arbitrary JSON blobs for every rule unless necessary. Prefer strongly typed schema records/unions that make incompatibilities visible.

---

## 25. ClickHouse Materialization

### 25.1 Principle

Keep ClickHouse outside the critical Flink state-consistency guarantee unless a proven transactional sink strategy is implemented.

Recommended architecture:

```text
risk.decisions Kafka topic
     ↓
independent materializer / Kafka engine
     ↓
ClickHouse
```

### 25.2 Table

Conceptual columns:

```sql
transaction_id String,
customer_id String,
risk_score UInt8,
decision LowCardinality(String),
matched_rules Array(String),
reason_codes Array(String),
rules_fingerprint String,
event_time DateTime64(3, 'UTC'),
processed_at DateTime64(3, 'UTC')
```

Idempotent/replay-safe ingestion should be documented.

---

## 26. Observability

### 26.1 Prometheus

Enable the Flink Prometheus metrics reporter.

### 26.2 Application metrics

Required custom metrics:

```text
risk_transactions_valid_total
risk_transactions_invalid_total
risk_transactions_duplicate_total
risk_transactions_late_total

risk_decisions_total{decision="APPROVE|REVIEW|REJECT"}
risk_rule_matches_total{rule_id="..."}

risk_rule_updates_total
risk_rule_updates_rejected_total
risk_rule_updates_stale_total

risk_evaluation_duration_ms
```

Avoid uncontrolled high-cardinality labels such as `customer_id`, `transaction_id`, or `event_id`.

### 26.3 Flink/runtime metrics to visualize

At minimum:

- records in/out;
- busy time;
- backpressured time;
- idle time;
- current input watermark;
- checkpoint duration;
- checkpoint size;
- failed checkpoints;
- restart count;
- TaskManager availability;
- Kafka source lag where exposed.

### 26.4 Grafana dashboards

Create at least two dashboards.

#### Dashboard A — Streaming Operations

Panels:

- input events/sec;
- output decisions/sec;
- Kafka lag;
- p50/p95/p99 processing latency if instrumented safely;
- current watermark delay;
- backpressure;
- checkpoint duration;
- checkpoint size;
- failed checkpoints;
- job restarts;
- invalid/late/duplicate rate.

#### Dashboard B — Risk Business View

Panels:

- decisions/minute;
- approve/review/reject distribution;
- rule match frequency;
- top reason codes;
- transaction volume;
- value processed.

---

## 27. Logging

Use structured logging.

Recommended fields:

```text
service
job_name
operator
event_id
transaction_id
customer_id_hash (optional)
rule_id
rule_version
error_code
checkpoint_id where relevant
```

Do not log every normal transaction in production mode.

Log levels:

- `INFO`: lifecycle, accepted rule changes, deployment/recovery markers;
- `WARN`: invalid/stale rules, DLQ/late anomalies, repeated checkpoint issues;
- `ERROR`: unrecoverable processing failures;
- `DEBUG`: detailed per-event diagnostics in local development only.

---

## 28. Security

For a portfolio deployment:

### Required principles

- no credentials committed to Git;
- secrets loaded through environment/Kubernetes Secrets;
- containers run as non-root where practical;
- dependency and image vulnerability scanning in CI;
- redact or synthesize all customer/payment data;
- use TLS/SASL configuration placeholders for Kafka production profile;
- use least-privilege object-store credentials.

All data in the project must be synthetic.

---

## 29. Configuration

Use externalized configuration.

Suggested configuration namespaces:

```text
app.kafka.bootstrap-servers
app.kafka.transactions-topic
app.kafka.rules-topic
app.kafka.decisions-topic
app.kafka.dlq-topic
app.kafka.late-topic

app.watermark.max-out-of-orderness
app.watermark.idle-timeout
app.dedup.ttl

app.risk.review-threshold
app.risk.reject-threshold

app.state.device-history-ttl
app.state.bucket-retention

app.environment
```

Secrets must not be part of regular config files.

---

## 30. Local Development Environment

Docker Compose should provide:

```text
Kafka
Schema Registry
MinIO
ClickHouse
Prometheus
Grafana
optional Flink standalone cluster
```

Two supported developer modes are acceptable:

### Mode A — Flink in Docker Compose

Best for one-command demos.

### Mode B — Infrastructure in Docker, Flink job from IDE/Maven

Best for debugging.

Document both if implemented.

### 30.1 Make targets

Recommended:

```bash
make up
make down
make topics
make schemas
make build
make test
make integration-test
make run-job
make generate-load
make inject-duplicates
make inject-late-events
make spike-load
make kill-taskmanager
make savepoint
make dashboards
```

---

## 31. Kubernetes Deployment

Use Apache Flink Kubernetes Operator.

Conceptual resource:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: payment-risk
spec:
  flinkVersion: v2_2

  image: ghcr.io/<owner>/flink-payment-risk:<version>

  flinkConfiguration:
    state.backend.type: rocksdb
    execution.checkpointing.interval: "30s"
    execution.checkpointing.dir: "s3://<bucket>/checkpoints"
    state.savepoints.dir: "s3://<bucket>/savepoints"

  job:
    jarURI: local:///opt/flink/usrlib/risk-engine.jar
    parallelism: 4
    upgradeMode: savepoint
```

Exact CRD fields must be validated against the pinned operator version before implementation.

### 31.1 Deployment principles

- pin image tags by immutable version/SHA;
- do not deploy `latest`;
- set resource requests and limits;
- use PodDisruptionBudgets where relevant;
- configure service account/RBAC minimally;
- store checkpoint/savepoint data outside pod-local storage;
- use Kubernetes Secrets for credentials.

---

## 32. Infrastructure as Code

Terraform should manage environment-specific external infrastructure where practical, for example:

```text
object-store bucket
IAM/service credentials
Kafka resources when provider supports target environment
monitoring namespace/resources where appropriate
```

Do not force Terraform to manage resources better represented as Kubernetes/Helm manifests solely to increase tool count.

Suggested structure:

```text
infrastructure/
├── docker/
├── kubernetes/
│   ├── base/
│   └── overlays/
├── helm/
└── terraform/
```

---

## 33. Repository Structure

Recommended final structure:

```text
flink-payment-risk-platform/
│
├── README.md
├── Makefile
├── pom.xml
│
├── docs/
│   ├── functional-technical-spec.md
│   ├── architecture.md
│   ├── operations-runbook.md
│   ├── failure-recovery.md
│   ├── performance.md
│   └── adr/
│       ├── 001-flink-version.md
│       ├── 002-event-time.md
│       ├── 003-state-backend.md
│       ├── 004-exactly-once-kafka.md
│       └── 005-dynamic-rules.md
│
├── schemas/
│   ├── transaction.avsc
│   ├── risk-rule.avsc
│   ├── risk-decision.avsc
│   ├── dead-letter.avsc
│   └── late-event.avsc
│
├── domain/
│   └── src/
│
├── risk-engine/
│   └── src/
│       ├── main/java/
│       └── test/java/
│
├── event-generator/
│   └── src/
│
├── rule-cli/
│   └── src/
│
├── decision-materializer/
│   └── src/
│
├── integration-tests/
│
├── infrastructure/
│   ├── docker/
│   ├── kubernetes/
│   └── terraform/
│
├── dashboards/
│
├── scripts/
│
└── .github/
    └── workflows/
        ├── ci.yml
        ├── integration.yml
        ├── image.yml
        └── release.yml
```

Keep the number of Maven modules proportional to useful isolation. The Flink job should remain the centerpiece.

---

## 34. Suggested Java Package Structure

```text
com.portfolio.paymentrisk
├── PaymentRiskJob.java
├── config/
├── domain/
│   ├── Transaction.java
│   ├── RiskRule.java
│   ├── RiskDecision.java
│   └── ...
├── source/
├── serialization/
├── validation/
├── watermark/
├── dedup/
├── state/
├── rules/
│   ├── RiskRuleEvaluator.java
│   ├── TransactionCountRuleEvaluator.java
│   ├── AmountVelocityRuleEvaluator.java
│   ├── UniqueDeviceRuleEvaluator.java
│   └── ...
├── processor/
│   └── CustomerRiskProcessFunction.java
├── metrics/
├── sink/
└── util/
```

---

## 35. Testing Strategy

### 35.1 Unit tests

Pure Java tests for:

- validators;
- score calculation;
- decision thresholds;
- each rule evaluator;
- rule version comparison;
- rules fingerprint;
- domain serialization helpers.

### 35.2 Operator/state tests

Test Flink operators with suitable Flink test harness/test utilities.

Required cases:

- first customer transaction;
- multiple transactions update state;
- timer fires;
- expired state removed;
- rule update observed;
- stale rule ignored;
- disabled rule ignored;
- duplicate suppressed;
- out-of-order but accepted event;
- too-late event routed correctly.

### 35.3 Integration tests

Use Testcontainers or equivalent to test at least:

```text
Kafka -> Flink pipeline -> Kafka
```

Prefer a real compatible Kafka broker over mocks for connector semantics.

Required integration scenarios:

#### IT-001 — Normal decision

Given a valid transaction, one `risk.decisions` record is produced.

#### IT-002 — Velocity threshold

Given six qualifying transactions inside the configured window, the threshold rule matches as specified.

#### IT-003 — Dynamic update

Publish rule v1, process event, publish rule v2, process another event; verify version-dependent behavior.

#### IT-004 — Duplicate

Publish the same event ID twice; only one normal decision is produced.

#### IT-005 — Out-of-order

Publish events with arrival order different from event-time order; verify expected risk state.

#### IT-006 — Late

Publish an event behind the configured accepted event-time boundary; verify late output.

#### IT-007 — Invalid input

Publish malformed/invalid input; verify DLQ.

### 35.4 Recovery tests

#### RT-001 — TaskManager failure

1. start traffic;
2. wait for completed checkpoints;
3. kill TaskManager;
4. allow recovery;
5. continue traffic;
6. verify expected decision count/business IDs.

#### RT-002 — Savepoint restore

1. process deterministic fixture;
2. trigger savepoint;
3. stop job;
4. restore compatible version;
5. continue fixture;
6. verify previous customer state remains effective.

#### RT-003 — Kafka interruption

Temporarily stop/restart Kafka in local environment and verify expected recovery behavior.

### 35.5 Schema compatibility tests

CI must fail if an incompatible schema change is introduced under the selected registry compatibility policy.

---

## 36. Test Data

Create deterministic fixtures in addition to randomized load.

Example fixture:

```text
customer = C1

10:00:00 €20 device=A APPROVED
10:00:20 €35 device=A APPROVED
10:00:40 €42 device=A APPROVED
10:01:00 €900 device=B APPROVED
10:01:20 €870 device=C APPROVED
10:01:40 €950 device=C APPROVED
```

Expected feature snapshot at last event should be explicitly asserted according to rule semantics.

Randomized generators are not substitutes for deterministic correctness fixtures.

---

## 37. Performance and Load Testing

### 37.1 Goals

Measure:

- sustainable input throughput;
- records/sec per configured parallelism;
- end-to-end processing latency;
- Kafka lag;
- checkpoint duration;
- checkpoint size;
- recovery duration;
- state growth;
- backpressure.

### 37.2 Workloads

At least:

```text
steady
burst
hot-key
high-cardinality
duplicate-heavy
out-of-order-heavy
```

### 37.3 Hot-key scenario

Generate one customer responsible for a disproportionate share of events.

Purpose:

Demonstrate that `keyBy(customer_id)` cannot parallelize a single hot customer's state across subtasks automatically.

Document the trade-off rather than hiding it.

### 37.4 Report

`docs/performance.md` must state:

- hardware/container limits;
- Kafka partitions;
- Flink parallelism;
- number of TaskManagers/slots;
- state backend;
- checkpoint settings;
- generator settings;
- event count;
- measured results.

Never invent benchmark results.

---

## 38. CI/CD

### 38.1 Pull request pipeline

```text
checkout
  ↓
validate formatting/static analysis
  ↓
compile
  ↓
unit tests
  ↓
operator tests
  ↓
schema compatibility tests
  ↓
integration tests
  ↓
dependency/security scan
```

### 38.2 Main/release pipeline

```text
all PR gates
  ↓
build shaded/application JAR
  ↓
build Flink application image
  ↓
scan image
  ↓
push immutable image tag
  ↓
publish deployment artifacts
```

Deployment to a real cluster may remain manual for the portfolio project, but deployment manifests must be reproducible.

### 38.3 Versioning

Use image tags such as:

```text
0.1.0
0.2.0
git-<short-sha>
```

Avoid `latest` in deployment manifests.

---

## 39. Operational Runbook Requirements

Create `docs/operations-runbook.md` containing at least:

### Job not producing output

Check:

1. Flink job status;
2. source records/sec;
3. Kafka lag;
4. watermark progress;
5. backpressure;
6. checkpoint health;
7. recent rule/config changes.

### Checkpoints failing

Check:

1. object-store connectivity;
2. checkpoint timeout;
3. state size;
4. backpressure/alignment;
5. storage credentials;
6. task failures.

### Kafka exactly-once errors

Check:

1. unique transactional ID prefix;
2. Kafka transaction timeout;
3. checkpoint duration;
4. restart duration;
5. producer fencing errors.

### Watermark stalled

Check:

1. idle partitions;
2. `withIdleness` setting;
3. per-partition source activity;
4. bad/future timestamps.

### State growing unexpectedly

Check:

1. TTL configuration;
2. timer cleanup;
3. cardinality;
4. device/history retention;
5. state backend metrics.

---

## 40. Non-Functional Requirements

### NFR-001 — Availability

Single process/TaskManager failures must be recoverable without manual state reconstruction.

### NFR-002 — Correctness

No accepted transaction may mutate customer state twice due solely to replay of the same `event_id` inside the deduplication horizon.

### NFR-003 — Recoverability

The job must be restorable from a checkpoint after runtime failure and from a savepoint for controlled compatible upgrades.

### NFR-004 — Scalability

Kafka topic partitioning and Flink parallelism must be configurable without code changes.

### NFR-005 — Bounded state

All long-lived keyed state must have an explicit retention/cleanup strategy.

### NFR-006 — Observability

Major failure and data-quality conditions must be externally measurable.

### NFR-007 — Reproducibility

A fresh reviewer environment must be able to run the local stack from version-controlled configuration.

### NFR-008 — Testability

Core rule decisions should be testable without a running Flink cluster when they do not intrinsically depend on Flink state/runtime.

### NFR-009 — Security hygiene

No production credentials or real payment/customer personal information may exist in the repository.

### NFR-010 — Maintainability

Critical architecture decisions must be documented as ADRs.

---

## 41. SLO-Like Portfolio Targets

These are engineering targets, not claims until measured.

| Metric | Initial target |
|---|---:|
| sustained local throughput | >= 10,000 events/sec if hardware permits |
| p95 app processing latency | < 250 ms under baseline load |
| checkpoint success | > 99% during healthy load |
| checkpoint duration | comfortably below interval/timeout |
| recovery after worker kill | automatic |
| duplicate visible decisions | 0 for duplicate IDs within TTL in tested scenario |
| lost committed input in recovery test | 0 in tested scenario |

Any target that cannot be achieved on available hardware must be documented with measured results and explanation.

---

## 42. Failure Semantics Matrix

| Failure | Expected behavior |
|---|---|
| Flink TaskManager killed | job restarts/redeploys task and restores checkpoint |
| Flink JobManager transient failure | HA/deployment layer recovers according to environment |
| Kafka temporary outage | connectors retry/fail job according to configuration; state restored |
| MinIO/S3 outage during checkpoint | checkpoint fails; previous successful checkpoint remains recovery point |
| invalid event | DLQ; job continues |
| invalid rule | reject rule; metric/log; job continues |
| deterministic code exception | job fails; Flink recovery applies |
| duplicate event | suppressed within configured horizon |
| late event | late topic; no silent finalized-state mutation |
| ClickHouse unavailable | Kafka decision stream remains authoritative; materialization catches up |

---

## 43. Architecture Decision Records

At minimum create the following ADRs.

### ADR-001 — Pin Flink 2.2.1

Explain:

- stable baseline;
- connector/operator compatibility;
- reproducibility;
- upgrade process.

### ADR-002 — Event time over processing time

Explain:

- out-of-order transactions;
- deterministic business windows;
- watermarks;
- lateness trade-off.

### ADR-003 — Embedded RocksDB state backend

Explain:

- potentially large state;
- disk-backed local state;
- incremental checkpoint capability;
- higher serialization/IO cost.

### ADR-004 — Kafka decision topic as authoritative output

Explain:

- clear streaming durability boundary;
- transactional exactly-once sink;
- downstream replay;
- ClickHouse becomes derived materialization.

### ADR-005 — Typed dynamic rules via Broadcast State

Explain:

- runtime updates;
- every subtask receives active rules;
- no generic DSL in first release.

### ADR-006 — Business dedup independent of Kafka delivery semantics

Explain why Kafka exactly-once output does not eliminate producer-generated duplicate business events.

---

## 44. Implementation Phases

### Phase 0 — Repository/bootstrap

Deliver:

- Maven project;
- Java 17;
- formatter/static analysis;
- basic CI;
- Docker Compose Kafka stack;
- README boot instructions.

Exit:

```bash
mvn verify
docker compose up
```

works from a clean checkout.

### Phase 1 — Contracts and basic pipeline

Deliver:

- Avro transaction/decision schemas;
- Kafka topics;
- source;
- validation;
- basic pass-through decision;
- Kafka sink.

Exit:

A valid transaction produces one decision.

### Phase 2 — Event time and state

Deliver:

- watermarks;
- customer `keyBy`;
- state backend;
- transaction-count rule;
- amount rule;
- timers/cleanup.

Exit:

Deterministic tests prove event-time window behavior.

### Phase 3 — Dedup and late handling

Deliver:

- event-ID dedup;
- TTL;
- late-event output;
- DLQ.

Exit:

Duplicate, malformed, out-of-order, and too-late fixtures pass.

### Phase 4 — Dynamic rules

Deliver:

- rule schema;
- rule producer/CLI;
- Broadcast State;
- rule versioning;
- score aggregation;
- rules fingerprint.

Exit:

Rule behavior changes at runtime without job restart.

### Phase 5 — Reliability

Deliver:

- checkpoint storage;
- exactly-once Kafka sink;
- stable UIDs;
- savepoint scripts;
- restart strategy;
- recovery tests.

Exit:

TaskManager failure and savepoint restore demonstrations pass.

### Phase 6 — Observability

Deliver:

- Prometheus reporter;
- custom metrics;
- Grafana dashboards;
- structured logs;
- operational runbook.

Exit:

Reviewer can diagnose lag, late events, duplicates, risk decisions, and checkpoint status from dashboards.

### Phase 7 — Analytics

Deliver:

- ClickHouse;
- replay-safe materializer;
- business dashboard.

Exit:

Decision records are queryable without weakening Kafka as authoritative output.

### Phase 8 — Kubernetes

Deliver:

- application image;
- Flink Kubernetes Operator manifests;
- checkpoint/savepoint object storage;
- Kubernetes configuration;
- deployment documentation.

Exit:

The application runs as a managed Flink deployment.

### Phase 9 — Performance/failure portfolio polish

Deliver:

- load scenarios;
- benchmark results;
- failure demo;
- architecture diagram;
- performance report;
- final README screenshots/GIFs.

---

## 45. Definition of Done

The project is considered portfolio-ready only when all items below are satisfied.

### Functional

- [ ] consumes `payments.raw`;
- [ ] validates input;
- [ ] uses event time;
- [ ] handles out-of-order data;
- [ ] suppresses duplicates inside defined horizon;
- [ ] maintains customer keyed state;
- [ ] implements at least four meaningful risk rules;
- [ ] supports runtime rule changes;
- [ ] produces explainable risk decisions;
- [ ] routes invalid input to DLQ;
- [ ] handles too-late input explicitly.

### Reliability

- [ ] checkpointing configured;
- [ ] durable checkpoint location configured;
- [ ] Kafka decision sink uses tested exactly-once configuration;
- [ ] explicit stateful operator UIDs;
- [ ] savepoint upgrade demonstrated;
- [ ] TaskManager failure recovery demonstrated.

### Quality

- [ ] unit tests;
- [ ] state/operator tests;
- [ ] Kafka integration tests;
- [ ] recovery tests;
- [ ] schema compatibility test;
- [ ] deterministic test fixtures.

### Operations

- [ ] Prometheus metrics;
- [ ] Grafana streaming dashboard;
- [ ] risk dashboard;
- [ ] structured logs;
- [ ] operations runbook;
- [ ] failure-injection commands.

### Deployment

- [ ] Docker image pinned/versioned;
- [ ] local Docker Compose environment;
- [ ] Kubernetes manifests;
- [ ] Flink Kubernetes Operator deployment;
- [ ] external checkpoint/savepoint storage.

### Portfolio

- [ ] architecture diagram;
- [ ] technical specification;
- [ ] ADRs;
- [ ] measured performance report;
- [ ] documented trade-offs;
- [ ] screenshots;
- [ ] failure/recovery demo;
- [ ] clear 5–10 minute reviewer quick-start.

---

## 46. README Story for Reviewers

The final README should answer these questions near the top:

1. What business problem does the project solve?
2. Why is Flink appropriate?
3. Where is state stored?
4. How are event time and late data handled?
5. How are duplicates handled?
6. How do risk rules change without deployment?
7. What consistency guarantee is provided?
8. What happens when a TaskManager dies?
9. How is the job upgraded?
10. What throughput/recovery numbers were actually measured?
11. How can I run the demo?

---

## 47. Production Guarantees and Trade-Offs Table

Maintain this table in the final project documentation and update it based on the actual implementation.

| Concern | Chosen mechanism | Boundary / trade-off |
|---|---|---|
| source recovery | Kafka source + Flink checkpoints | depends on compatible connector/checkpointing |
| Flink state | EXACTLY_ONCE checkpoint mode | recovery replays from consistent snapshot |
| output to Kafka | transactional exactly-once sink | consumers should use `read_committed` |
| producer duplicates | `event_id` dedup state | bounded by dedup TTL |
| event ordering | event time + watermarks | bounded lateness assumption |
| very late input | explicit late topic | not silently applied to finalized state |
| dynamic configuration | Broadcast State | ordering/version semantics must be enforced |
| large state | RocksDB backend | additional serialization/disk cost |
| controlled upgrades | savepoints + explicit UIDs | state serializer compatibility still matters |
| analytics persistence | Kafka-derived ClickHouse materialization | independently replay-safe, not automatically EO |
| scale | Kafka partitions + Flink parallelism | hot keys remain a constraint |
| observability | Prometheus + Grafana | cardinality must be controlled |

---

## 48. Implementation Invariants

These rules should be treated as engineering guardrails.

1. **No business-time risk rule may rely solely on processing time.**
2. **No unbounded keyed collection may be added without retention/cleanup design.**
3. **No stateful operator may be committed without an explicit UID.**
4. **No schema change may bypass compatibility tests.**
5. **No new rule may be added without deterministic unit and stateful integration tests.**
6. **No exception should be swallowed when it can leave state semantically corrupted.**
7. **No claim of exactly-once may omit the boundary to which the claim applies.**
8. **No benchmark result may be published without the test environment/configuration.**
9. **No secrets or real customer/card data may enter the repository.**
10. **No version-critical dependency may use an unpinned `latest` tag.**

---

## 49. Open Design Questions to Resolve During Implementation

Record final decisions as ADRs.

### OQ-001 — Dedup topology

Choose:

- separate `keyBy(event_id)` dedup operator followed by `keyBy(customer_id)`;
- or customer-scoped dedup map if event-ID guarantees allow it.

Recommended starting choice: separate event-ID keyed dedup operator for semantic clarity.

### OQ-002 — Exact late-event boundary

Define exactly when a historical event becomes ineligible to mutate feature state.

### OQ-003 — State representation

Choose between:

- per-event sorted state;
- time buckets;
- hybrid structures.

Recommended: time buckets for velocity metrics plus device-last-seen map.

### OQ-004 — Currency policy

Choose:

- EUR-only generator for MVP;
- per-currency rule state;
- external FX normalization.

Recommended: EUR-only first; add explicit multi-currency support only if desired.

### OQ-005 — ClickHouse ingestion

Choose between:

- Kafka Engine + materialized view;
- a small dedicated materializer;
- validated Flink ClickHouse sink.

Recommended: independent replay-safe materializer to preserve clean guarantee boundaries.

---

## 50. Recommended First Vertical Slice

Implement this before advanced infrastructure:

```text
1. Kafka + Schema Registry
2. transaction Avro schema
3. synthetic transaction generator
4. Flink Kafka source
5. validation
6. event-time watermark
7. keyBy(customer_id)
8. 2-minute transaction-count state
9. one risk rule
10. Kafka risk decision sink
11. deterministic integration test
```

Then add complexity only after this works.

The first successful end-to-end scenario should be:

```text
five normal transactions
        ↓
sixth transaction crosses configured velocity threshold
        ↓
risk decision contains R001
        ↓
integration test asserts score + decision + reason
```

This gives the repository a correct, testable backbone before adding Broadcast State, Kubernetes, dashboards, and failure engineering.

---

# PART III — REFERENCE CONFIGURATION

## 51. Example Local Application Configuration

```yaml
app:
  environment: local

  kafka:
    bootstrap-servers: kafka:9092
    transactions-topic: payments.raw
    rules-topic: risk.rules
    decisions-topic: risk.decisions
    dlq-topic: payments.dlq
    late-topic: payments.late
    transaction-consumer-group: payment-risk-v1

  watermark:
    max-out-of-orderness: 10s
    idle-timeout: 60s

  dedup:
    ttl: 24h

  risk:
    review-threshold: 30
    reject-threshold: 70

  state:
    device-history-ttl: 30d
    bucket-retention: 1h
```

---

## 52. Example Flink Configuration Baseline

```yaml
state.backend.type: rocksdb

execution.checkpointing.interval: 30s
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.timeout: 2min
execution.checkpointing.min-pause: 10s
execution.checkpointing.max-concurrent-checkpoints: 1

execution.checkpointing.storage: filesystem
execution.checkpointing.dir: s3://payment-risk/flink/checkpoints

state.savepoints.dir: s3://payment-risk/flink/savepoints
```

Local MinIO requires the appropriate S3 filesystem/plugin and endpoint/path-style configuration.

---

## 53. Example Decision Contract Assertions

For every decision:

```text
risk_score in [0, 100]
decision != null
transaction_id == source.transaction_id
customer_id == source.customer_id
event_time == source.event_time
processed_at >= ingestion/processing start
matched_rules contains unique IDs
reason_codes contains unique codes
rules_fingerprint != blank
```

If:

```text
risk_score >= reject_threshold
```

then:

```text
decision == REJECT
```

If:

```text
review_threshold <= risk_score < reject_threshold
```

then:

```text
decision == REVIEW
```

Otherwise:

```text
decision == APPROVE
```

---

## 54. Reference Failure Demo

A portfolio demo should be reproducible with roughly this narrative:

```text
1. Start infrastructure.
2. Start Flink job.
3. Start steady load.
4. Show Grafana throughput/checkpoints.
5. Publish a dynamic rule update.
6. Show risk distribution change.
7. Inject duplicate events.
8. Show duplicate metric increase without duplicate decisions.
9. Inject out-of-order/late events.
10. Show accepted out-of-order processing and explicit late-event output.
11. Kill a TaskManager.
12. Show job recovery from checkpoint.
13. Confirm processing continues.
14. Trigger a savepoint.
15. Deploy a compatible new job version.
16. Restore from savepoint.
17. Show preserved customer state.
```

This demo is one of the main portfolio deliverables.

---

# PART IV — OFFICIAL REFERENCES

The implementation should be validated against the version-specific Apache documentation, especially before dependency or operator upgrades.

1. Apache Flink downloads  
   https://flink.apache.org/downloads/

2. Flink 2.2 Kafka connector  
   https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/datastream/kafka/

3. Flink connector fault-tolerance guarantees  
   https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/guarantees/

4. Flink 2.2 state  
   https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/fault-tolerance/state/

5. Flink state backends  
   https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/

6. Flink 2.2 watermarks  
   https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/event-time/generating_watermarks/

7. Flink 2.2 savepoints  
   https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/state/savepoints/

8. Flink Kubernetes Operator compatibility  
   https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/deployment/compatibility/

---

## 55. Final Engineering Principle

The project should optimize for demonstrable streaming correctness rather than the largest possible technology list.

A reviewer should be able to inspect the repository and conclude that the implementation deliberately addresses:

```text
state
event time
out-of-order data
late data
duplicates
dynamic configuration
checkpointing
delivery semantics
failure recovery
stateful upgrades
bounded state
observability
testing
deployment
```

Those are the capabilities that make this a serious Apache Flink portfolio project rather than a Kafka-to-Flink tutorial.
