"""Apply additive tables and replace analytical views on the local retained database."""
import re
import urllib.request
from showcase import ROOT, settings

config = settings()
schema = (ROOT / "infrastructure/docker/clickhouse.sql").read_text(encoding="utf-8")
statements = [query.strip() for query in re.sub(r"--[^\n]*", "", schema).split(";") if query.strip()]
for query in statements:
    if not query.startswith(("CREATE DATABASE IF NOT EXISTS", "CREATE TABLE IF NOT EXISTS", "CREATE VIEW IF NOT EXISTS", "CREATE OR REPLACE VIEW")):
        raise ValueError("Unexpected schema operation")
    request = urllib.request.Request("http://127.0.0.1:28123/?wait_end_of_query=1", data=query.encode(),
        headers={"X-ClickHouse-User": config.get("CLICKHOUSE_USER", "risk"), "X-ClickHouse-Key": config["CLICKHOUSE_PASSWORD"]})
    with urllib.request.urlopen(request, timeout=30) as response:
        response.read()
print("Analytical schema applied; existing payment and evaluation rows retained.")
