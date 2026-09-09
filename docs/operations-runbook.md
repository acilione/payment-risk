# Operations runbook

## Output stops

Check the Flink UI and `docker compose logs --tail=100 jobmanager taskmanager`. Inspect running vertices, validation input, Kafka lag, watermarks, backpressure and checkpoints. Transactional output becomes visible after checkpoints. An idle finite source leaves a watermark tail; advancing valid events are required.

Check registry and Kafka reachability separately. Registry outages fail/retry instead of flooding the DLQ. Missing committed production offsets require intentional group bootstrap or a savepoint, not an automatic earliest fallback.

## Invalid and late input

Inspect DLQ source positions and error codes. Evidence is capped at 4096 bytes, base64 encoded, with a truncation flag. Fix producers before re-emitting corrected records with new event identities. Late events have already passed deduplication, so reusing their event IDs inside TTL is suppressed.

Check producer clocks, timestamp distribution, source partition imbalance and disorder allowance. Events exactly at the watermark are late. Late data is reconciliation input and does not revise previous decisions.

## Checkpoint failures

Check object-store DNS/TLS/identity, S3 plugin, bucket availability, disk pressure, state size and backpressure. Local state is under `s3://payment-risk/checkpoints`. Never delete individual incremental checkpoint objects: newer checkpoints may reference them. Preserve the complete state graph.

Broker transaction.max.timeout.ms must admit 900000 ms. Recovery must finish inside this transaction budget. Repeated restarts beyond it require deliberate restore and reconciliation.

## Failure demonstration

`make kill-taskmanager` requires a checkpoint, kills only this Compose project's worker, restarts it and waits for running vertices. `make recovery-test` also reconciles committed output during worker failure. `python3 scripts/operations.py kafka-interruption` exercises broker restart.

`make idle-resume-test` waits 75 seconds with no producers, then reconciles resumed traffic. Run it in isolation from load producers to exercise the all-idle path.

Standalone Compose is not JobManager HA. A JobManager loss requires explicit resubmission from retained state. Kubernetes uses the operator and durable HA metadata.

## Stateful upgrades

`make stop-job` takes a non-draining savepoint and records its location in ignored `artifacts/last-savepoint.txt`. Non-draining stop preserves pending timers and customer history. Deploy a compatible image and `make restore`. `make savepoint` snapshots without stopping; never start a second copy under the same transaction prefix while the original remains active.

Keep UIDs, descriptor names, serializer semantics and max parallelism stable. Restore without allowNonRestoredState. Test the old-to-new image transition and compare exact output before retiring rollback assets. Kafka retention must cover required source offsets.

## State growth

Inspect active customer cardinality, horizons, hot keys, pending watermarks and checkpoint size. Caps fail rather than evicting evidence. Increasing Kafka partitions cannot divide a single customer's keyed state. Raise limits only with measured memory/checkpoint capacity.

## Materialization

Failed ClickHouse inserts leave consumer offsets uncommitted. Query `risk.decisions_current`, not raw physical rows, for business counts. Partition-count changes and customer remapping need a versioning migration. Production requires materializer lag alerts, backups and separate insert/read identities.

## Credentials and shutdown

Local credentials are generated in ignored `.env`; do not copy them to logs. Production properties come from an existing Secret and object storage uses workload identity. `make down` preserves volumes. No automated volume deletion is provided. Compose uses rotating local-driver logs (three 10 MB files per service) to bound log disk usage.

## This workstation's image-cache recovery

The prior disk-full incident damaged an unpacked Docker base layer. The local verified filesystem is retained as `payment-risk:recovered-base`; `artifacts/local-rebuild.Dockerfile` can package a newly tested JAR with `docker build -f artifacts/local-rebuild.Dockerfile -t payment-risk:0.1.0 .`. This is an ignored workstation repair artifact. Fresh environments and CI use the normal version-controlled Dockerfile. Stateful image changes still require stop-with-savepoint and restore.
