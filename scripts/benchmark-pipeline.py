"""Measure a generated showcase run, retaining bounded samples and exact reconciliation."""
import argparse
import hashlib
import json
import math
import os
import sqlite3
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

import showcase
from load_analysis import sustained_summary


def latency_summary(db, column="milliseconds"):
    if column not in ("milliseconds", "evaluation_ms", "storage_ms", "ack_ms"):
        raise ValueError("Invalid latency column")
    count = db.execute("SELECT count(*) FROM benchmark_latency").fetchone()[0]
    if not count:
        raise AssertionError("No assessed payment latencies")
    result = {"samples": count}
    for label, fraction in [("p50_ms", .5), ("p95_ms", .95), ("p99_ms", .99), ("max_ms", 1)]:
        rank = math.ceil(count * fraction) - 1
        result[label] = db.execute(f"SELECT {column} FROM benchmark_latency ORDER BY {column} LIMIT 1 OFFSET ?", (rank,)).fetchone()[0]
    return result


def measure_latency(db, report):
    db.execute("PRAGMA temp_store=FILE")
    db.execute("CREATE TABLE IF NOT EXISTS benchmark_latency (id TEXT PRIMARY KEY, milliseconds INTEGER NOT NULL, evaluation_ms INTEGER, storage_ms INTEGER, ack_ms INTEGER)")
    existing = {row[1] for row in db.execute("PRAGMA table_info(benchmark_latency)")}
    for column in ("evaluation_ms", "storage_ms", "ack_ms"):
        if column not in existing:
            db.execute(f"ALTER TABLE benchmark_latency ADD COLUMN {column} INTEGER")
    db.execute("DELETE FROM benchmark_latency")
    db.execute("CREATE TABLE IF NOT EXISTS offered_schedule (id TEXT PRIMARY KEY, scheduled_at INTEGER NOT NULL)")
    db.execute("CREATE TABLE IF NOT EXISTS benchmark_arrivals (id TEXT PRIMARY KEY, scheduled_at INTEGER, producer_at INTEGER, stored_at INTEGER)")
    db.execute("DELETE FROM benchmark_arrivals")
    db.execute("CREATE INDEX IF NOT EXISTS arrival_schedule ON benchmark_arrivals(scheduled_at)")
    db.execute("CREATE INDEX IF NOT EXISTS arrival_storage ON benchmark_arrivals(stored_at)")
    cursor = ""
    while True:
        rows = showcase.sql("""SELECT event_id, (extract(epoch FROM stored_at)*1000)::bigint AS stored_at, processed_at AS evaluated_at
            FROM risk.assessments WHERE source_topic={topic:String}
            AND starts_with(event_id,{prefix:String}) AND event_id COLLATE "C">{after:String}
            ORDER BY event_id COLLATE "C" LIMIT 200""",
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
            producer_time = json.loads(expected[0])["producer_time"]
            scheduled = db.execute("SELECT scheduled_at FROM offered_schedule WHERE id=?", (cursor,)).fetchone()
            if scheduled:
                if scheduled[0] > producer_time:
                    raise AssertionError("Producer clock moved behind its offered schedule")
                db.execute("INSERT INTO benchmark_arrivals VALUES (?,?,?,?)", (cursor, scheduled[0], producer_time, int(row["stored_at"])))
            if not producer_time <= int(row["evaluated_at"]) <= int(row["stored_at"]):
                raise AssertionError("Inconsistent stage latency: verify host clocks")
            ack = db.execute("SELECT min(a.observed_at) FROM acknowledgements a JOIN inputs i USING(partition,offset) WHERE i.event_id=?", (cursor,)).fetchone()[0]
            db.execute("INSERT INTO benchmark_latency VALUES (?,?,?,?,?)", (cursor, delay,
                int(row["evaluated_at"]) - producer_time,
                int(row["stored_at"]) - int(row["evaluated_at"]),
                ack - producer_time if ack is not None else None))
        db.commit()
    result = latency_summary(db)
    if result["samples"] != report["stored_decisions"]:
        raise AssertionError("Latency coverage differs from reconciliation")
    result["stages"] = {"producer_to_evaluation": latency_summary(db, "evaluation_ms"),
                        "evaluation_to_storage_including_checkpoint": latency_summary(db, "storage_ms")}
    if db.execute("SELECT count(*) FROM benchmark_latency WHERE ack_ms IS NULL").fetchone()[0] == 0:
        result["stages"]["producer_to_acknowledgement"] = latency_summary(db, "ack_ms")
    return result


def sample_metrics():
    # Existing aggregated metrics only; no per-customer series or payment payloads.
    query = '{__name__=~"risk_materializer_(records_lag_max|batch_rows_total|batch_bytes_total|batches_total|insert_errors_total|storage_available)"}'
    url = "http://127.0.0.1:29090/api/v1/query?" + urllib.parse.urlencode({"query": query})
    with urllib.request.urlopen(url, timeout=3) as response:
        rows = json.load(response)["data"]["result"]
    if len(rows) > 100:
        raise RuntimeError("Unexpected metric cardinality")
    for name, expression in {
        "source_lag_max": "max(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max)",
        "backpressure_ms_per_second": "max(flink_taskmanager_job_task_backPressuredTimeMsPerSecond)",
        "checkpoint_duration_ms": "max(flink_jobmanager_job_lastCheckpointDuration)",
    }.items():
        endpoint = "http://127.0.0.1:29090/api/v1/query?" + urllib.parse.urlencode({"query": expression})
        with urllib.request.urlopen(endpoint, timeout=3) as response:
            values = json.load(response)["data"]["result"]
        rows.append({"metric": {"__name__": name}, "value": values[0]["value"] if values else None})
    return rows


def summarize_metrics(path):
    result = {"scope": "Entire command, including startup and final drain; not a steady-state saturation test", "sample_errors": 0, "series": {}}
    with path.open() as source:
        for line in source:
            sample = json.loads(line)
            if "error" in sample:
                result["sample_errors"] += 1
            for row in sample.get("metrics", []):
                value = row.get("value")
                if value is None or not math.isfinite(float(value[1])):
                    continue
                name = row["metric"]["__name__"]
                # Preserve label sets instead of mixing independent consumers.
                labels = {k: v for k, v in row["metric"].items() if k != "__name__"}
                key = name + " " + json.dumps(labels, sort_keys=True)
                current = float(value[1])
                series = result["series"].setdefault(key, {"samples": 0, "first": current, "max": current})
                series.update(samples=series["samples"] + 1, last=current, max=max(series["max"], current))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=showcase.ROOT / "config/performance-load.json")
    parser.add_argument("--historical", action="store_true", help="Use compressed showcase history instead of live multi-partition load")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--database-outage", action="store_true")
    parser.add_argument("--checkpoint-interval-ms", type=int, default=10000)
    parser.add_argument("--checkpoint-min-pause-ms", type=int, default=2000)
    parser.add_argument("--warmup-seconds", type=int, default=30)
    parser.add_argument("--window-seconds", type=int, default=20)
    parser.add_argument("--rate", type=int, help="Override the live-load rate together with --duration-seconds")
    parser.add_argument("--duration-seconds", type=int, help="Offered load duration, including warmup")
    args = parser.parse_args()
    if args.warmup_seconds < 0 or args.window_seconds < 1:
        parser.error("Warmup must be nonnegative and window length positive")
    if not 1000 <= args.checkpoint_interval_ms <= 60000 or not 0 <= args.checkpoint_min_pause_ms <= args.checkpoint_interval_ms:
        parser.error("Invalid checkpoint interval or minimum pause")
    if (args.rate is None) != (args.duration_seconds is None):
        parser.error("Supply both --rate and --duration-seconds")
    if args.rate is not None and (args.historical or not 1 <= args.rate <= 1000 or args.duration_seconds < args.warmup_seconds + 4*args.window_seconds):
        parser.error("Live load requires rate 1-1000 and at least four measurement windows after warmup")
    output = showcase.ROOT / "artifacts/pipeline-load"
    output.mkdir(parents=True, exist_ok=True)
    stamp = str(time.time_ns())
    if args.rate is not None:
        config = json.loads(args.config.read_text())
        total = args.rate * args.duration_seconds
        customers = min(2000, max(math.ceil(total/100), min(config["customers"], total//7)))
        payments = math.ceil(total/customers)
        if payments > 100 or customers*payments > 100000:
            parser.error("This generator supports at most 100,000 payments per run; reduce rate or duration")
        config.update(customers=customers, min_payments=payments, max_payments=payments, publish_rate=args.rate)
        args.config = output / (stamp + "-config.json")
        showcase.write_json(args.config, config)
    source_files = ["docker-compose.yml", "config/performance-load.json", "scripts/showcase.py", "scripts/benchmark-pipeline.py", "scripts/load_analysis.py",
                    "src/main/java/com/portfolio/paymentrisk/tools/ShowcaseProducer.java",
                    "src/main/java/com/portfolio/paymentrisk/tools/BoundedPaymentPublisher.java",
                    "src/main/java/com/portfolio/paymentrisk/processor/CustomerRiskProcessor.java",
                    "src/main/java/com/portfolio/paymentrisk/domain/RiskEngine.java",
                    "src/main/java/com/portfolio/paymentrisk/domain/Audit.java",
                    "src/main/java/com/portfolio/paymentrisk/tools/MaterializerRows.java",
                    "src/main/java/com/portfolio/paymentrisk/storage/PostgresWriter.java",
                    "infrastructure/docker/postgres.sql", "web/server/postgres-reader.mjs"]
    source_hashes = {name: hashlib.sha256((showcase.ROOT / name).read_bytes()).hexdigest() for name in source_files}
    manifest = {"source_sha256": source_hashes, "config": json.loads(args.config.read_text()),
                "arguments": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
                "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=showcase.ROOT, text=True).strip(),
                "image_id": subprocess.check_output(["docker", "image", "inspect", "payment-risk:0.1.0", "--format", "{{.Id}}"], text=True).strip(),
                "environment": {"logical_cpus": os.cpu_count(), "memory_bytes": os.sysconf("SC_PAGE_SIZE")*os.sysconf("SC_PHYS_PAGES")},
                "log": str(output / (stamp + ".log"))}
    showcase.write_json(output / (stamp + "-experiment.json"), manifest)
    command = [sys.executable, str(showcase.ROOT / "scripts/showcase.py"), "run", "--config", str(args.config.resolve()), "--include-outcomes"]
    command.extend(["--checkpoint-interval-ms", str(args.checkpoint_interval_ms), "--checkpoint-min-pause-ms", str(args.checkpoint_min_pause_ms)])
    if not args.historical:
        command.append("--load")
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
        steady = sustained_summary(db, report, args.warmup_seconds, args.window_seconds)
    checkpoint_runtime = {"configuration": showcase.operations.rest("/jobs/" + report["job_id"] + "/checkpoints/config"),
                          "statistics": showcase.operations.rest("/jobs/" + report["job_id"] + "/checkpoints")}
    if checkpoint_runtime["configuration"]["interval"] != args.checkpoint_interval_ms or checkpoint_runtime["configuration"]["min_pause"] != args.checkpoint_min_pause_ms:
        raise AssertionError("Running checkpoint configuration differs from the experiment")
    result = {
        "run_id": report["run_id"], "measured_at": showcase.now(),
        "scope": "Historical single-partition showcase" if args.historical else "Bounded asynchronous live inputs across Kafka partitions, Flink and PostgreSQL; finite local load test",
        "publishing": {key: report.get(key) for key in ("load_mode", "max_in_flight", "payment_partitions", "publishing_elapsed_ms", "achieved_payments_per_second", "achieved_deliveries_per_second", "producer_elapsed_ms")},
        "latency_definition": "First PostgreSQL assessment insert timestamp minus producer_time, including event-time waiting, checkpoint commits and any database outage; requires synchronized clocks",
        "latency": latency, "steady_load": steady, "reconciliation": report["reconciliation"],
        "checkpoint_interval_ms": report["checkpoint_interval_ms"], "checkpoint_min_pause_ms": report["checkpoint_min_pause_ms"],
        "checkpoint_runtime": checkpoint_runtime,
        "driver": {"placement": "Separate Compose container, shared host" if not args.historical else "JobManager container", "cpus": 1 if not args.historical else None, "memory_mib": 512 if not args.historical else None},
        "experiment": manifest,
        "config": report["config"], "recovery": report.get("recovery"),
        "metrics_file": str(output / (stamp + "-metrics.jsonl")),
        "metrics_summary": summarize_metrics(output / (stamp + "-metrics.jsonl")),
        "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=showcase.ROOT, text=True).strip(),
        "source_sha256": source_hashes,
        "source_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=showcase.ROOT, text=True).strip()),
    }
    target = output / (stamp + ".json")
    showcase.write_json(target, result)
    print(json.dumps(result, indent=2))
    print("Report: " + str(target))


if __name__ == "__main__":
    main()
