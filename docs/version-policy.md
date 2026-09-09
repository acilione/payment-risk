# Dependency policy — verified 2026-09-09

CI uses Helm 4.2.4, PyYAML 6.0.3 and jsonschema 4.26.0. GitHub actions are pinned to commit SHAs resolved from current stable releases. Local image scanning uses Trivy 0.74.0. Tool release sources: [Helm](https://github.com/helm/helm/releases), [setup-helm](https://github.com/Azure/setup-helm/releases), [Trivy](https://github.com/aquasecurity/trivy/releases).

| Component | Pin |
|---|---|
| Flink | 2.2.1 |
| Kafka connector | 5.0.0-2.2 |
| Kubernetes Operator | 1.15.0 |
| Java | 21 container; 17 bytecode |
| Kafka broker | 4.3.1 |
| Avro / Jackson | 1.12.1 / 2.21.4 |
| Apicurio / PostgreSQL | 3.3.2 / 18.6 |
| ClickHouse | 26.8.2.7 LTS |
| Prometheus / Grafana | 3.14.0 / 13.2.0 |
| Local MinIO | RELEASE.2025-09-07T16-13-09Z |

Flink 2.3.0 is newer, but the official current Kafka connector and Operator 1.15 matrix list support through 2.2. This project chooses the latest supported combination. Do not upgrade core, connector, runtime and operator independently. The connector owns the Kafka client dependency.

References: [Flink compatibility matrix](https://flink.apache.org/downloads/), [Java compatibility](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/java_compatibility/), [Kafka releases](https://kafka.apache.org/community/downloads/), [Apicurio](https://www.apicur.io/registry/getting-started/), [ClickHouse packages](https://packages.clickhouse.com/), [Prometheus](https://prometheus.io/download/), [Grafana](https://grafana.com/grafana/download).

Maven versions are explicit and test dependencies share a JUnit BOM. CI generates an SBOM and scans images/dependencies. Local images use concrete tags; production Helm requires an application digest. Resolve and commit infrastructure provider lock files and platform-specific image digests in the owning deployment environment.
