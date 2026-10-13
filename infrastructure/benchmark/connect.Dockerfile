FROM python:3.12-alpine AS plugin
ARG CONNECTOR_VERSION=1.7.0
ARG CONNECTOR_SHA256=3bbca83137793ff1f75485c4fc0a48ba74e4d5a0826e3e5304eb50bd20b573ab
RUN python -c 'import os, urllib.request, hashlib, io, zipfile; version=os.environ["CONNECTOR_VERSION"]; data=urllib.request.urlopen(f"https://github.com/ClickHouse/clickhouse-kafka-connect/releases/download/v{version}/clickhouse-kafka-connect-v{version}.zip").read(); assert hashlib.sha256(data).hexdigest()==os.environ["CONNECTOR_SHA256"]; zipfile.ZipFile(io.BytesIO(data)).extractall("/plugin")'
FROM confluentinc/cp-kafka-connect:8.2.1
COPY --from=plugin /plugin /usr/share/confluent-hub-components/clickhouse
