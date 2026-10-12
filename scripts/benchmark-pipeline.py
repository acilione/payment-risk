"""Measure a generated showcase run, retaining bounded samples and exact reconciliation."""
import argparse
import json
import math
import sqlite3
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

import showcase


def latency_summary(db):
    count = db.execute("SELECT count(*) FROM benchmark_latency").fetchone()[0]
    if not count:
        raise AssertionError("No assessed payment latencies")
    result = {"samples": count}
    for label, fraction in [("p50_ms", .5), ("p95_ms", .95), ("p99_ms", .99), ("max_ms", 1)]:
        rank = math.ceil(count * fraction) - 1
        result[label] = db.execute("SELECT milliseconds FROM benchmark_latency ORDER BY milliseconds LIMIT 1 OFFSET ?", (rank,)).fetchone()[0]
    return result


def measure_latency(db, report):
    db.execute("PRAGMA temp_store=FILE")
    db.execute("CREATE TABLE IF NOT EXISTS benchmark_latency (id TEXT PRIMARY KEY, milliseconds INTEGER NOT NULL)")
    db.execute("DELETE FROM benchmark_latency")
    cursor = ""
    while True:
        rows = showcase.sql("""SELECT event_id, min(toUnixTimestamp64Milli(ingested_at)) AS stored_at
            FROM risk.evaluations WHERE source_topic={topic:String}
            AND startsWith(event_id,{prefix:String}) AND event_id>{after:String}
            GROUP BY event_id ORDER BY event_id LIMIT 200""",
            topic=report["decision_topic"], prefix=report["run_id"] + "-payment-", after=cursor)
        if not rows:
            break
        for row in rows:
            cursor = row["event_id"]
            expected = db.execute("SELECT payload,decision FROM payments WHERE id=?", (cursor,)).fetchone()
            if expected is None or expected[1] is None:
                raise AssertionError("Latency row has no reconciled assessment")
            delay = int(row["stored_at"]) - json.loads(expected[0])["producer_time"]
            if delay < 0:
                raise AssertionError("Negative latency: verify host clocks")
            db.execute("INSERT INTO benchmark_latency VALUES (?,?)", (cursor, delay))
        db.commit()
    result = latency_summary(db)
    if result["samples"] != report["stored_decisions"]:
        raise AssertionError("Latency coverage differs from reconciliation")
    return result


def sample_metrics():
    # Existing aggregated metrics only; no per-customer series or payment payloads.
    query = '{__name__=~"risk_materializer_(records_lag_max|batch_rows_total|batch_bytes_total|batches_total|insert_errors_total|storage_available)"}'
    url = "http://127.0.0.1:29090/api/v1/query?" + urllib.parse.urlencode({"query": query})
    with urllib.request.urlopen(url, timeout=3) as response:
        rows = json.load(response)["data"]["result"]
    if len(rows) > 100:
        raise RuntimeError("Unexpected metric cardinality")
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=showcase.ROOT / "config/showcase-load.json")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--database-outage", action="store_true")
    args = parser.parse_args()
    output = showcase.ROOT / "artifacts/pipeline-load"
    output.mkdir(parents=True, exist_ok=True)
    stamp = str(time.time_ns())
    command = [sys.executable, str(showcase.ROOT / "scripts/showcase.py"), "run", "--config", str(args.config.resolve()), "--include-outcomes"]
    if args.no_build:
        command.append("--no-build")
    if args.database_outage:
        command.append("--database-outage")
    with (output / (stamp + ".log")).open("w") as log, (output / (stamp + "-metrics.jsonl")).open("w") as metrics:
        process = subprocess.Popen(command, cwd=showcase.ROOT, stdout=log, stderr=subprocess.STDOUT)
        try:
            while process.poll() is None:
                sample = {"observed_at": showcase.now()}
                try:
                    sample["metrics"] = sample_metrics()
                except (OSError, ValueError, KeyError, RuntimeError) as error:
                    sample["error"] = str(error)
                metrics.write(json.dumps(sample) + "\n")
                metrics.flush()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass
            if process.returncode:
                raise RuntimeError("Pipeline run failed; inspect " + str(log.name))
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=45)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
    report = json.loads((showcase.WORK / "latest.json").read_text())
    if report["status"] != "COMPLETE" or not report["reconciliation"]["verified"]:
        raise AssertionError("Run did not reconcile")
    with sqlite3.connect(showcase.WORK / report["run_id"] / "ledger.sqlite") as db:
        latency = measure_latency(db, report)
    result = {
        "run_id": report["run_id"], "measured_at": showcase.now(),
        "scope": "Local generated history through Kafka, Flink and ClickHouse; single input partition; not a sustained multi-partition capacity test",
        "latency_definition": "First ClickHouse insert timestamp minus producer_time, including event-time waiting, checkpoint commits and any database outage; requires synchronized clocks",
        "latency": latency, "reconciliation": report["reconciliation"],
        "config": report["config"], "recovery": report.get("recovery"),
        "metrics_file": str(output / (stamp + "-metrics.jsonl")),
        "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=showcase.ROOT, text=True).strip(),
        "source_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=showcase.ROOT, text=True).strip()),
    }
    target = output / (stamp + ".json")
    showcase.write_json(target, result)
    print(json.dumps(result, indent=2))
    print("Report: " + str(target))


if __name__ == "__main__":
    main()
