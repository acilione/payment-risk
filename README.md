# Payment risk

A Java application that uses Apache Flink to evaluate payment events from Kafka. It checks five risk rules, records an approve, review, or reject decision, and stores the results in ClickHouse for the dashboard.

## Dashboard

**[Open the demo](https://acilione.github.io/payment-risk/)**

The dashboard shows decision totals, transaction details, rule matches, and service status. The hosted demo uses synthetic data. When run locally, it reads results from ClickHouse and status information from Flink and Prometheus.

![Payment risk dashboard with demo data](docs/screenshots/website.png)

After `make up`, open **http://localhost:23001**. Use `make website` to add the dashboard to an already running stack.

## Run locally

Use Linux or WSL with Docker Compose, Python 3, Java 17 or later, Maven, and Make. Allow at least 8 GB of available memory and approximately 15 GB of free disk space for images, build cache, and local data. On WSL, check free space on the Windows host as well.

```bash
make test
make up
make run-job
make integration-test
make generate-load
```

`make up` creates local credentials in the ignored `.env` file and starts the services. `make run-job` submits the Flink job. `make down` stops the stack and preserves its data volumes.

| Service | Local address |
|---|---|
| Dashboard | http://localhost:23001 |
| Flink | http://localhost:28082 |
| Grafana | http://localhost:23000 |
| Prometheus | http://localhost:29090 |
| Schema Registry API | http://localhost:28081/apis/ccompat/v7 |
| ClickHouse | http://localhost:28123 |
| MinIO console | http://localhost:29001 |
| Kafka | localhost:29092 |

Grafana uses `admin` and the `GRAFANA_ADMIN_PASSWORD` value in `.env`. It includes dashboards for risk decisions and pipeline monitoring.

## Documentation

The [technical guide](docs/technical-guide.md) covers the [architecture](docs/technical-guide.md#architecture), [data model](docs/technical-guide.md#data-model), risk rules, configuration, operations, deployment, and recorded test results. It also includes website development instructions and troubleshooting.

## Repository

| Path | Contents |
|---|---|
| `src/main/java/com/portfolio/paymentrisk` | Flink job, risk rules, codecs, CLI, and ClickHouse consumer |
| `src/test` | Rule, state, recovery, and schema tests |
| `schemas` | Avro schemas |
| `infrastructure` | Docker, Helm, and Terraform configuration |
| `web` | React dashboard and Fastify API |
| `observability`, `dashboards` | Prometheus configuration and Grafana dashboards |
| `scripts` | Operations and verification scripts |
