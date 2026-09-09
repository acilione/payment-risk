# Deployment reference

Compose uses one broker, one JobManager, one two-slot TaskManager, durable registry storage, MinIO and ClickHouse. It coexists with the sibling project's ports and containers.

Install Apache Flink Kubernetes Operator **1.15.0** and its CRDs in `flink-system`, then render this application's Helm chart. The application chart does not install the operator, Kafka or production analytics infrastructure.

```bash
helm repo add flink-operator https://downloads.apache.org/flink/flink-kubernetes-operator-1.15.0/
helm upgrade --install flink-kubernetes-operator flink-operator/flink-kubernetes-operator \
  --version 1.15.0 --namespace flink-system --create-namespace
helm template payment-risk infrastructure/helm/payment-risk \
  --namespace payment-risk --values environment-values.yaml > rendered.yaml
kubectl apply --dry-run=server -f rendered.yaml
```

Required values: immutable image digest, TLS broker addresses, HTTPS registry endpoint, checkpoint bucket, deployment transaction prefix, Secret name and endpoint CIDRs. The Secret contains `kafka.properties` and optionally `REGISTRY_BEARER_TOKEN`; configure SASL credentials and truststores through the owning environment. Restrict topic/transactional-ID permissions and use a separate rule producer identity.

Production payment groups require pre-existing committed offsets or a savepoint. Rule state bootstraps from earliest compacted history. The chart includes RocksDB, external state, Kubernetes HA metadata, savepoint upgrades, resource budgets, non-root pods, capability drops, restricted egress and disruption budgeting. The official image needs writable Flink configuration directories; a read-only filesystem needs a separately tested configuration bootstrap.

Terraform provides a protected, versioned, encrypted S3 bucket and denies insecure transport. Attach its IAM policy output to a federated workload identity. It creates no static access keys or age-based checkpoint deletion. Plan/apply has not been run against a cloud account.

Terraform 1.16.1 initialization and validation passed locally with the committed AWS 6.63.0 provider lock. CI repeats initialization with `-backend=false -lockfile=readonly`, formatting checks and validation. Production Prometheus must discover the annotated Flink pods or use an environment-managed PodMonitor.

Target qualification must include CRD validation, TLS/workload identity, worker and JobManager failure, old-to-new savepoint restore, rescaling, workload capacity, alert metric verification, materializer monitoring and release vulnerability evidence.
