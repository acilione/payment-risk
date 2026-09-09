# Performance protocol and limits

No production capacity claim is inferred from compilation, a producer rate or a correctness fixture. Run steady, burst, hot-key, high-cardinality, duplicate-heavy and out-of-order workloads with unique run IDs. Keep all input partitions advancing and measure across completed checkpoints.

Record CPU/RAM, container limits, image digest, partitions, parallelism/slots, backend, watermark/checkpoint settings, exact generator command, acknowledged count, committed IDs, lag trend, p50/p95/p99 arrival-to-visible-output latency, evaluation time, state size, checkpoint duration and recovery interval. Include a long-running or accelerated-horizon state-expiry test.

Local defaults: three payment partitions; parallelism 2; one worker/two slots; worker 2048m and JobManager 1600m process memory; RocksDB; ten-second checkpoints; two-minute checkpoint timeout; fifteen-minute Kafka transactions; one-hour exact history and thirty-day devices. The synchronous-ack generator itself limits throughput.

Exact-history scans cost O(retained events per customer); equal-time buckets rewrite JSON arrays. A hot customer cannot be split among subtasks. Hard state caps fail visibly. Cleanup uses one additional timer per customer. The next optimization is aggregated buckets plus exact boundary fragments, proven against current semantics and savepoint migration tests.

Actual local results belong in `acceptance.md` and ignored `artifacts/`. CI acceptance is not a capacity benchmark.

## Measured baseline — 2026-09-09

Command: `python3 scripts/benchmark.py --count 1000 --rate 100`. WSL2 Linux 6.6.87.2, 20 logical CPUs, 15.46 GiB guest memory, Java 17.0.20 producer, Java 21.0.12 container runtime, and the configuration above. Other project services were running; this was a shared development machine. Image and job identifiers plus raw metrics are recorded in `artifacts/benchmark.json`.

| Measurement | Observed |
|---|---:|
| Acknowledged / committed / duplicate IDs | 1,000 / 1,000 / 0 |
| Acknowledged producer rate | 99.99 events/sec over 10.001 seconds |
| Arrival to read-committed visibility, p50 / p95 / p99 | 16.329 / 20.793 / 21.192 seconds |
| Event time to finalized evaluation, p50 / p95 / p99 | 10.797 / 11.257 / 11.334 seconds |
| Rolling rule-evaluation p95, subtasks 0 / 1 | 541.6 / 530 microseconds |
| Completed / failed checkpoints during measurement | 4 / 0 |
| Last checkpoint duration / full state bytes | 357 ms / 324,865 bytes |
| End-of-run maximum partition lag / backpressure | 0 / 0 ms per second |

The evaluation histogram excludes buffering, state preparation and Kafka transaction visibility. Its per-subtask quantiles are not a global end-to-end percentile. The specification's 250 ms target must distinguish these quantities: ten-second event-time finality cannot provide sub-250 ms committed authorization responses. Lower latency requires a deliberate disorder/finality and checkpoint trade-off, or a separate provisional authorization path.

This bounded 100 events/sec run does not establish the 10,000 events/sec capacity target or long-run state stability. The acknowledged synchronous generator and exact customer-history scans are explicit bottlenecks to address before that qualification. The worker recovery fixture restored checkpoint 205 and subsequently completed checkpoint 209, reconciling all six IDs with zero duplicates. The 52.971-second recovery-test interval includes reconciliation observation time and is an upper bound, not a precise time-to-first-recovered-decision measurement.
