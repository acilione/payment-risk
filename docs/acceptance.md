# Verification and release evidence

| Requirement area | Evidence / command |
|---|---|
| Five rules, score, boundaries and validation | `RiskEngineTest` |
| Ordered timers, late output, snapshots, dedup TTL | `StateOperatorsTest` |
| Avro framing and historical reader compatibility | `SchemaCompatibilityTest` |
| Kafka -> Flink -> committed Kafka, duplicates, malformed and late | `make integration-test` |
| Worker interruption and committed ID reconciliation | `make recovery-test` |
| Non-draining savepoint stop/restore | `make stop-job restore integration-test` |
| Persistent deployment and topic/schema bootstrap | `make up run-job` |
| Alert behavior | `promtool test rules alerts.test.yml` |
| CRD schema validation | `python3 scripts/verify-deployment.py` |
| Cloud infrastructure and HA | Target environment qualification |
| Sustained load, rescaling, multi-version state migration | Release qualification |

## Local execution — 2026-09-09

- `mvn spotless:check verify cyclonedx:makeAggregateBom`: 16 rule, state, source-validation and schema tests pass. The optional Kafka MiniCluster test also passed against the real broker and registry before container qualification.
- Kafka/registry bootstrap passed against Kafka 4.3.1 and Apicurio 3.3.2 with durable PostgreSQL storage. Compatibility API reports BACKWARD; CI checks every stored historical writer against the current reader.
- Container integration passes: six expected committed decisions, zero duplicates, all five rules on the sixth transaction, exact source-position DLQ evidence and explicitly late output.
- Live rule update, disable, stale rejection and baseline-parameter restoration pass without restarting the job.
- SIGKILL worker recovery restored checkpoint 205 and resumed checkpoints. Exact fixture reconciliation passes; `artifacts/recovery.json` and `recovery-test.jsonl` retain evidence.
- Non-draining stop and savepoint restore pass, including an updated application image. The final restore used savepoint 212 at `s3://payment-risk/savepoints/savepoint-d72184-9f30a5d1e28b`; post-restore integration and live-rule assertions pass. This is not a general cross-version or rescaling compatibility certification.
- Replaying Kafka decisions into ClickHouse twice increased physical rows from 18 to 36 while logical rows remained 18.
- Both Grafana dashboards are provisioned and all 18 panel queries return live series. Prometheus alert tests pass.
- Helm rendering validates against the official Operator 1.15.0 CRD. No Kubernetes cluster or cloud account has been deployed from this workspace.
- Bounded benchmark: 1,000 acknowledged and committed IDs, zero duplicates, 99.99 events/sec; see [performance measurements](performance.md).

The recovery fixture exposed a test-ordering issue: elapsed time during downtime did not prove watermark finalization. The corrected fixture observes committed decisions before injecting late data. A separate regression test proves that checkpointed customer finality survives source watermark reset. Extended verification also exposed unsafe maximum-watermark emission from the rule input during payment idleness; the control stream now emits idleness instead. State tests preserve pending events and history through idle/resume, and the real 75-second idle/resume scenario passes with six committed decisions, zero duplicates and exactly the deliberately late fixture classified late. CI repeats this scenario.

The earlier full-disk incident corrupted Docker's unpacked Flink base. A clean isolated builder verified the Dockerfile, and a locally flattened copy of that verified filesystem avoided the damaged runtime snapshot. The running local image contains the tested JAR and logging configuration; release CI builds the normal Dockerfile in a clean runner. The temporary repair builder was removed without pruning other projects.

Additional checks: broker SIGKILL/restart resumed checkpointing in 19.912 seconds. Terraform 1.16.1 with locked AWS provider 6.63.0 passes initialization, formatting and validation with zero warnings; no plan/apply was executed. Trivy 0.74.0 found an older transitive Jackson core in the first image. Jackson BOM alignment to 2.21.4 remediated it; the rebuilt image scan reports zero **fixable HIGH/CRITICAL** findings across OS and detected Java packages. This filtered scan is not a claim of zero vulnerabilities at every severity. Raw reports are in `artifacts/image-scan.json` and `target/bom.json`.

## Website acceptance

Pulse provides a responsive React/TypeScript website with a read-only Fastify API and a separately built static demo. Browser acceptance reconciled 1,601 live decisions against the API, exercised transaction search/filtering and the decision inspector, validated rule/architecture navigation and keyboard dismissal, checked 390px mobile layouts, and verified explicit behavior during upstream failure. Four backend boundary tests and both production builds pass. npm reports no known dependency vulnerabilities at the time of this run. Screenshots contain explicitly labeled demo data; see [website operations](website.md).

## Target qualification still required

Production TLS and workload identity, Kubernetes worker/JobManager HA, rescaling and historical release migrations, sustained 10,000 events/sec capacity and long-running state behavior require the target environment. Deployment assets and bounded local checks do not imply those qualifications have passed.
