# Architecture decision records

Accepted on 2026-09-09. Implementation detail and guarantee boundaries are in [architecture](../architecture.md).

| ADR | Decision and reason | Consequence / alternative |
|---|---|---|
| 001 | Flink 2.2.1, Kafka connector 5.0.0-2.2 and Operator 1.15.0 form the supported compatibility set. Java 21 runtime with Java 17 bytecode supports local development. | Latest Flink core alone is not the upgrade criterion. Upgrade the set after state and failure qualification; see [version policy](../version-policy.md). |
| 002 | Event time, per-split watermarks and ordered event-time timers define business windows. Finalized events cannot be amended by very late arrivals. | Ten seconds of disorder delays finality; an entirely idle stream leaves a pending tail. Processing-time evaluation cannot provide the same history semantics. |
| 003 | Embedded RocksDB with incremental durable checkpoints and explicit managed serializers stores customer and dedup state. | Serialization and disk I/O are costs; exact-history scans are auditable but require aggregation before large hot-key workloads. Event-time cleanup and processing-time dedup TTL have distinct meanings. |
| 004 | Transactional Kafka decisions are authoritative. A read-committed consumer synchronously inserts into ClickHouse before committing offsets. | Kafka checkpoints provide exactly-once visible output. ClickHouse independently resolves replay by business key and source version; it is outside the Kafka transaction. |
| 005 | Five typed, versioned rules use Broadcast State. Admission snapshots freeze rules for already buffered events. | Bounded configuration and labels simplify validation. Independent rule/payment logs have no global activation order; historical deterministic re-evaluation needs stronger effective-time contracts. |
| 006 | A separate event-ID keyed TTL operator performs business deduplication before customer partitioning. | One additional shuffle buys global event-ID semantics. Kafka delivery guarantees alone cannot remove duplicates intentionally published under different offsets. Dedup expires after 24 hours. |
| 007 | Integer minor units and an explicit EUR-only contract avoid implicit FX aggregation. | Multi-currency inputs are rejected until per-currency state or a versioned FX normalization contract is implemented. |
| 008 | Stable UIDs, max parallelism 128, versioned state names and savepoint upgrades define the restoration contract. | State JSON is still a schema and requires migration tests. Restore never permits silently dropping unmatched state. |
