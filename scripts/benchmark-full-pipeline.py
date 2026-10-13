#!/usr/bin/env python3
"""Generated payments -> Kafka -> Flink -> committed Kafka output -> official sink -> ClickHouse."""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import runpy
import shutil
import subprocess
import threading
import time
import urllib.request
import uuid
from datetime import datetime, timezone

import showcase
from load_analysis import sustained_summary

ROOT = showcase.ROOT
SINK = runpy.run_path(str(ROOT / "scripts/benchmark-clickhouse-kafka.py"))
LOAD = runpy.run_path(str(ROOT / "scripts/benchmark-pipeline.py"))
AUDIT = "com.portfolio.paymentrisk.tools.PipelineAudit"
JAR = showcase.JAR


def table_name(run, suffix):
    return run.replace("-", "_") + "_" + suffix


def read_jsonl(path):
    with path.open() as source:
        for line in source:
            if line.startswith("{"):
                yield json.loads(line)


def read_clickhouse(table):
    query = (
        f"SELECT * FROM benchmark.{table} SETTINGS output_format_json_quote_64bit_integers=0, "
        "output_format_json_named_tuples_as_objects=1 FORMAT JSONEachRow"
    )
    request = urllib.request.Request(
        "http://localhost:38123/",
        data=query.encode(),
        headers={
            "X-ClickHouse-User": "risk",
            "X-ClickHouse-Key": SINK["environment"]()["CLICKHOUSE_PASSWORD"],
        },
    )
    with urllib.request.urlopen(request, timeout=120) as source:
        for line in source:
            yield json.loads(line)


def clock_check(latest_producer, report, negative_durations):
    # The last send precedes completion. Its wall-clock span cannot exceed the
    # complete monotonic publication duration unless the clocks have diverged.
    advance = (
        None
        if latest_producer is None
        else max(
            0,
            latest_producer
            - report["publishing_started_at"]
            - report["publishing_elapsed_ms"],
        )
    )
    return {
        "status": (
            "clock_unstable"
            if negative_durations or (advance is not None and advance > 1000)
            else "no_detected_clock_error"
        ),
        "wall_advance_lower_bound_ms": advance,
        "negative_stage_durations": negative_durations,
        "scope": "One-sided clock consistency check, not proof of synchronization",
    }


def verify_buckets(db, table):
    """Check every aggregate key through a disk-backed reference, not only grand totals."""
    db.execute("""CREATE TEMP TABLE expected_buckets AS
        SELECT CAST(json_extract(payload,'$.event_time') / 10000 AS INTEGER) * 10 bucket,
               json_extract(payload,'$.decision') decision,
               json_extract(payload,'$.currency') currency,
               count(*) payments, sum(json_extract(payload,'$.amount_minor')) amount_minor,
               sum(json_extract(payload,'$.risk_score')) risk_score_sum
        FROM outputs WHERE kind='decisions' GROUP BY bucket,decision,currency""")
    db.execute(
        "CREATE UNIQUE INDEX expected_bucket_key ON expected_buckets(bucket,decision,currency)"
    )
    query = f"""SELECT toUnixTimestamp(bucket) bucket,decision,currency,sum(payments) payments,
        sum(amount_minor) amount_minor,sum(risk_score_sum) risk_score_sum
        FROM benchmark.{table}_totals GROUP BY bucket,decision,currency
        SETTINGS output_format_json_quote_64bit_integers=0 FORMAT JSONEachRow"""
    checked = 0
    request = urllib.request.Request(
        "http://localhost:38123/",
        data=query.encode(),
        headers={
            "X-ClickHouse-User": "risk",
            "X-ClickHouse-Key": SINK["environment"]()["CLICKHOUSE_PASSWORD"],
        },
    )
    with urllib.request.urlopen(request, timeout=120) as source:
        for line in source:
            row = json.loads(line)
            key = tuple(row[k] for k in ["bucket", "decision", "currency"])
            expected = db.execute(
                "SELECT payments,amount_minor,risk_score_sum FROM expected_buckets "
                "WHERE bucket=? AND decision=? AND currency=?",
                key,
            ).fetchone()
            assert expected == tuple(
                row[k] for k in ["payments", "amount_minor", "risk_score_sum"]
            ), row
            db.execute(
                "DELETE FROM expected_buckets WHERE bucket=? AND decision=? AND currency=?",
                key,
            )
            checked += 1
    assert db.execute("SELECT count(*) FROM expected_buckets").fetchone()[0] == 0
    db.execute("DROP TABLE expected_buckets")
    return checked


def reconcile(db, folder, run, report, compose):
    db.executescript(
        """CREATE TABLE observed_inputs(partition INTEGER,offset INTEGER,hash TEXT,PRIMARY KEY(partition,offset));
        CREATE TABLE outputs(kind TEXT,partition INTEGER,offset INTEGER,payload TEXT,stored INTEGER DEFAULT 0,
                             PRIMARY KEY(kind,partition,offset));
        CREATE TABLE actual(id TEXT PRIMARY KEY,payload TEXT,stored_at INTEGER);
        CREATE TABLE benchmark_arrivals(id TEXT PRIMARY KEY,scheduled_at INTEGER,producer_at INTEGER,stored_at INTEGER);
        CREATE INDEX arrival_schedule ON benchmark_arrivals(scheduled_at);
        CREATE INDEX arrival_storage ON benchmark_arrivals(stored_at);
        CREATE TABLE stage_latency(id TEXT PRIMARY KEY,evaluation_ms INTEGER,storage_ms INTEGER,total_ms INTEGER);
    """
    )
    for kind, schema in [
        ("payments", "wire"),
        ("decisions", "risk-decision"),
        ("dlq", "dead-letter"),
        ("late", "late-event"),
    ]:
        audit = folder / (kind + "-kafka.jsonl")
        with audit.open("w") as output:
            compose(
                "run",
                "--rm",
                "--no-deps",
                "-T",
                "--entrypoint",
                "java",
                "load-generator",
                "-Xmx256m",
                "-cp",
                JAR,
                AUDIT,
                "read",
                run + "." + kind,
                schema,
                stdout=output,
                stderr=subprocess.DEVNULL,
            )
        for row in read_jsonl(audit):
            if kind == "payments":
                db.execute(
                    "INSERT INTO observed_inputs VALUES(?,?,?)",
                    (row["partition"], row["offset"], row["payload_sha256"]),
                )
            else:
                db.execute(
                    "INSERT INTO outputs(kind,partition,offset,payload) VALUES(?,?,?,?)",
                    (
                        kind,
                        row["partition"],
                        row["offset"],
                        json.dumps(row["value"], sort_keys=True),
                    ),
                )
        db.commit()
    missing = db.execute(
        """SELECT count(*) FROM inputs i LEFT JOIN observed_inputs o USING(partition,offset)
                            WHERE o.hash IS NULL OR o.hash!=i.hash"""
    ).fetchone()[0]
    extra = db.execute(
        """SELECT count(*) FROM observed_inputs o LEFT JOIN inputs i USING(partition,offset)
                          WHERE i.hash IS NULL"""
    ).fetchone()[0]
    assert missing == extra == 0, (missing, extra)
    invalid = late = 0
    for kind in ["decisions", "dlq", "late"]:
        for row in read_clickhouse(table_name(run, kind)):
            prefix = "source_" if kind == "decisions" else "kafka_"
            partition, offset = row.pop(prefix + "partition"), row.pop(
                prefix + "offset"
            )
            assert row.pop(prefix + "topic") == run + "." + kind
            stored_at = row.pop("stored_at")
            expected = db.execute(
                "SELECT payload,stored FROM outputs WHERE kind=? AND partition=? AND offset=?",
                (kind, partition, offset),
            ).fetchone()
            assert (
                expected is not None and not expected[1]
            ), "Unexpected or repeated Kafka delivery in ClickHouse"
            assert (
                json.loads(expected[0]) == row
            ), "ClickHouse changed the committed Flink record"
            db.execute(
                "UPDATE outputs SET stored=1 WHERE kind=? AND partition=? AND offset=?",
                (kind, partition, offset),
            )
            if kind == "decisions":
                if row["event_id"].startswith("showcase-control-"):
                    continue
                stamp = int(
                    datetime.fromisoformat(stored_at)
                    .replace(tzinfo=timezone.utc)
                    .timestamp()
                    * 1000
                )
                db.execute(
                    "INSERT INTO actual VALUES(?,?,?)",
                    (row["event_id"], json.dumps(row), stamp),
                )
            elif kind == "dlq":
                assert row["source_topic"] == run + ".payments"
                expected_input = db.execute(
                    "SELECT hash,kind FROM inputs WHERE partition=? AND offset=?",
                    (row["source_partition"], row["source_offset"]),
                ).fetchone()
                assert expected_input == (
                    row["payload_sha256"],
                    "invalid",
                ), expected_input
                invalid += 1
            else:
                payment = json.loads(row["transaction"])
                expected_input = db.execute(
                    "SELECT payload FROM payments WHERE id=?", (payment["event_id"],)
                ).fetchone()
                assert expected_input and json.loads(expected_input[0]) == payment
                assert row["reason"] == "TOO_LATE" and row["lateness_ms"] > 0
                assert payment["event_id"] == run + "-payment-late"
                late += 1
        db.commit()
    assert (
        db.execute("SELECT count(*) FROM outputs WHERE NOT stored").fetchone()[0] == 0
    )
    assert invalid == late == 1, (invalid, late)
    assert (
        db.execute("SELECT count(*) FROM actual").fetchone()[0]
        == report["expected_payments"] - 1
    )
    assert (
        db.execute(
            "SELECT count(*) FROM actual a LEFT JOIN payments p ON p.id=a.id WHERE p.id IS NULL"
        ).fetchone()[0]
        == 0
    )
    assert (
        db.execute(
            """SELECT count(*) FROM payments p LEFT JOIN actual a USING(id)
        WHERE p.id!=? AND a.id IS NULL""",
            (run + "-payment-late",),
        ).fetchone()[0]
        == 0
    )
    assert (
        db.execute(
            "SELECT count(*) FROM actual WHERE id=?", (run + "-payment-late",)
        ).fetchone()[0]
        == 0
    )
    ordered = folder / "reference-input.jsonl"
    with ordered.open("w") as output:
        for payload, decision, stored_at, scheduled in db.execute("""
            SELECT p.payload,a.payload,a.stored_at,s.scheduled_at FROM payments p JOIN actual a USING(id)
            LEFT JOIN offered_schedule s USING(id)
            ORDER BY p.customer,json_extract(p.payload,'$.event_time'),p.id"""):
            payment = json.loads(payload)
            assessment = json.loads(decision)
            output.write(
                json.dumps({"payment": payment, "decision": assessment}) + "\n"
            )
            producer = payment["producer_time"]
            evaluated = assessment["processed_at"]
            db.execute(
                "INSERT INTO stage_latency VALUES(?,?,?,?)",
                (
                    payment["event_id"],
                    evaluated - producer,
                    stored_at - evaluated,
                    stored_at - producer,
                ),
            )
            if scheduled is not None:
                db.execute(
                    "INSERT INTO benchmark_arrivals VALUES(?,?,?,?)",
                    (payment["event_id"], scheduled, producer, stored_at),
                )
    db.commit()
    verified = compose(
        "run",
        "--rm",
        "--no-deps",
        "-T",
        "-v",
        str(ordered) + ":/audit.jsonl:ro",
        "--entrypoint",
        "java",
        "load-generator",
        "-Xmx256m",
        "-cp",
        JAR,
        AUDIT,
        "verify",
        "/audit.jsonl",
        capture_output=True,
    )
    oracle = json.loads(
        next(line for line in verified.stdout.splitlines() if line.startswith("{"))
    )
    # Compare the incremental totals against committed outputs, including control records if finalized.
    totals = json.loads(
        SINK["sql"](
            f"SELECT sum(payments) payments,sum(amount_minor) amount_minor,sum(risk_score_sum) risk_score_sum FROM benchmark.{table_name(run,'decisions_totals')} FORMAT JSONEachRow"
        )
    )
    expected = db.execute(
        "SELECT count(*),sum(json_extract(payload,'$.amount_minor')),sum(json_extract(payload,'$.risk_score')) FROM outputs WHERE kind='decisions'"
    ).fetchone()
    assert (
        tuple(int(totals[k]) for k in ["payments", "amount_minor", "risk_score_sum"])
        == expected
    )
    buckets = verify_buckets(db, table_name(run, "decisions"))
    latency = {}
    n = oracle["assessments"]
    for column in ["evaluation_ms", "storage_ms", "total_ms"]:
        latency[column] = {
            label: db.execute(
                f"SELECT {column} FROM stage_latency ORDER BY {column} LIMIT 1 OFFSET ?",
                (max(0, math.ceil(n * fraction) - 1),),
            ).fetchone()[0]
            for label, fraction in [("p50", 0.5), ("p95", 0.95), ("p99", 0.99)]
        }
    report["stored_decisions"] = n
    negative = db.execute(
        "SELECT count(*) FROM stage_latency WHERE evaluation_ms<0 OR storage_ms<0"
    ).fetchone()[0]
    timing = clock_check(
        db.execute("SELECT max(producer_at) FROM benchmark_arrivals").fetchone()[0],
        report,
        negative,
    )
    steady = sustained_summary(db, report)
    if timing["status"] == "clock_unstable":
        steady.update(
            unqualified_wall_clock_status=steady["status"],
            status="clock_unstable",
            reason="Clock consistency checks failed; latency/backlog windows cannot qualify this rate",
        )
    return dict(
        reference=oracle,
        latency=latency,
        invalid=invalid,
        late=late,
        exact_expected_event_set=True,
        timing=timing,
        input_wire_hashes=db.execute("SELECT count(*) FROM inputs").fetchone()[0],
        unique_kafka_deliveries=db.execute("SELECT count(*) FROM outputs").fetchone()[
            0
        ],
        incremental_totals=totals,
        verified_aggregate_buckets=buckets,
        steady_load=steady,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rate", type=int, default=200)
    parser.add_argument("--duration-seconds", type=int, default=110)
    parser.add_argument("--historical", action="store_true")
    parser.add_argument("--restart-flink", action="store_true")
    args = parser.parse_args()
    host_disk = Path("/mnt/c") if Path("/mnt/c").is_dir() else ROOT
    if shutil.disk_usage(host_disk).free < 3 * 1024**3:
        parser.error(
            "At least 3 GiB of free host-disk space is required before this test"
        )
    running_materializer = subprocess.check_output(
        [
            "docker",
            "ps",
            "-q",
            "--filter",
            "label=com.docker.compose.project=payment-risk-showcase",
            "--filter",
            "label=com.docker.compose.service=materializer",
        ],
        text=True,
    ).strip()
    if running_materializer:
        parser.error(
            "Stop the showcase materializer before testing the direct ingestion path"
        )
    if (
        not 1 <= args.rate <= 1000
        or not 110 <= args.duration_seconds <= 300
        or args.rate * args.duration_seconds > 100000
    ):
        parser.error(
            "Use rate 1..1000, duration 110..300 seconds and at most 100000 payments"
        )
    try:
        jobs = showcase.operations.rest("/jobs/overview")["jobs"]
    except OSError:
        jobs = []
    if any(j["state"] not in ("FINISHED", "FAILED", "CANCELED") for j in jobs):
        parser.error(
            "Savepoint and stop the active Flink job before starting a fresh benchmark"
        )
    run = "show-" + uuid.uuid4().hex[:12]
    folder = ROOT / "artifacts/full-pipeline" / run
    folder.mkdir(parents=True)
    config = json.loads(
        (
            ROOT
            / (
                "config/showcase.json"
                if args.historical
                else "config/performance-load.json"
            )
        ).read_text()
    )
    if not args.historical:
        customers = math.ceil(args.rate * args.duration_seconds / 100)
        config.update(
            customers=customers,
            min_payments=100,
            max_payments=100,
            publish_rate=args.rate,
        )
    (folder / "config.json").write_text(json.dumps(config))
    env = dict(
        os.environ,
        TRANSACTIONAL_PREFIX=run,
        KAFKA_GROUP=run,
        FLINK_PARALLELISM="3",
        FLINK_TASK_SLOTS="3",
        FLINK_CHECKPOINT_INTERVAL_MS="10000",
        FLINK_CHECKPOINT_MIN_PAUSE_MS="2000",
    )
    for key, suffix in [
        ("PAYMENTS", "payments"),
        ("RULES", "rules"),
        ("DECISIONS", "decisions"),
        ("DLQ", "dlq"),
        ("LATE", "late"),
    ]:
        env[key + "_TOPIC"] = run + "." + suffix
    envfile = folder / "compose.env"
    envfile.write_text(
        "".join(
            k + "=" + env[k] + "\n"
            for k in [
                "TRANSACTIONAL_PREFIX",
                "KAFKA_GROUP",
                "FLINK_PARALLELISM",
                "FLINK_TASK_SLOTS",
                "FLINK_CHECKPOINT_INTERVAL_MS",
                "FLINK_CHECKPOINT_MIN_PAUSE_MS",
                "PAYMENTS_TOPIC",
                "RULES_TOPIC",
                "DECISIONS_TOPIC",
                "DLQ_TOPIC",
                "LATE_TOPIC",
            ]
        )
    )
    command = [
        "docker",
        "compose",
        "-p",
        "payment-risk-showcase",
        "--env-file",
        str(ROOT / ".env"),
        "--env-file",
        str(envfile),
        "-f",
        str(ROOT / "docker-compose.yml"),
    ]

    def compose(*arguments, **kwargs):
        return subprocess.run(
            command + list(arguments),
            cwd=ROOT,
            env=env,
            text=True,
            check=True,
            **kwargs,
        )

    report = {
        "run_id": run,
        "config": config,
        "arguments": vars(args),
        "status": "STARTING",
        "source_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip(),
        "path": "Generated payments / Kafka / Flink / committed Kafka / official ClickHouse sink / incremental view",
        "materializer_running": False,
        "source_dirty": bool(
            subprocess.check_output(["git", "status", "--porcelain"], text=True).strip()
        ),
        "source_sha256": {
            str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in [
                *ROOT.glob("src/main/java/**/*.java"),
                ROOT / "scripts/benchmark-full-pipeline.py",
                ROOT / "scripts/benchmark-clickhouse-kafka.py",
                ROOT / "docker-compose.yml",
                ROOT / "docker-compose.clickhouse-benchmark.yml",
            ]
        },
        "resources": {
            "flink_parallelism": 3,
            "taskmanager_process_memory_mib": 2048,
            "jobmanager_process_memory_mib": 1600,
            "flink_cpu_limit": None,
            "producer_cpus": 1,
            "producer_memory_mib": 512,
        },
    }
    names = []
    stop = threading.Event()
    sampler = None

    def sample():
        with (folder / "metrics.jsonl").open("w") as stream:
            while not stop.is_set():
                row = {"observed_at": showcase.now()}
                try:
                    row["metrics"] = LOAD["sample_metrics"]()
                    row["stored_rows"] = SINK["count"](table_name(run, "decisions"))
                except Exception as error:
                    row["error"] = str(error)
                stream.write(json.dumps(row) + "\n")
                stream.flush()
                stop.wait(5)

    try:
        compose(
            "up",
            "-d",
            "--no-build",
            "jobmanager",
            "taskmanager",
            "prometheus",
            capture_output=True,
        )
        showcase.operations.ensure_no_active_job()
        SINK["compose"](
            "up", "-d", "keeper", "benchmark-clickhouse", "connect", capture_output=True
        )
        deadline = time.monotonic() + 300
        while True:
            try:
                SINK["api"]("connectors")
                break
            except (OSError, RuntimeError):
                if time.monotonic() > deadline:
                    raise
                time.sleep(1)
        SINK["tables"](table_name(run, "decisions"))
        SINK["connector"](table_name(run, "decisions"), run + ".decisions", 3)
        names.append(table_name(run, "decisions"))
        for suffix, schema in [("dlq", "dead-letter"), ("late", "late-event")]:
            name = table_name(run, suffix)
            SINK["sql"](
                f"""CREATE TABLE benchmark.{name} ({SINK['columns'](schema)},kafka_topic String,
                kafka_partition Int32,kafka_offset Int64,stored_at DateTime64(3) DEFAULT now64(3))
                ENGINE=ReplicatedMergeTree('/benchmark/tables/{name}','one') ORDER BY (kafka_partition,kafka_offset)
                SETTINGS replicated_deduplication_window=10000,replicated_deduplication_window_seconds=3600"""
            )
            SINK["connector"](name, run + "." + suffix, 3, "kafka_")
            names.append(name)
        compose(
            "exec", "-T", "jobmanager", "flink", "run", "-d", JAR, capture_output=True
        )
        report["job_id"] = showcase.operations.wait_running()
        report["resources"]["sink"] = SINK["resources"](
            ("connect", "keeper", "benchmark-clickhouse")
        )
        sampler = threading.Thread(target=sample)
        sampler.start()
        with showcase.ledger(folder / "ledger.sqlite") as db, (
            folder / "inputs.jsonl"
        ).open("w") as log, (folder / "producer.log").open("w") as errors:
            db.execute("PRAGMA temp_store=FILE")
            producer = subprocess.Popen(
                command
                + [
                    "run",
                    "--rm",
                    "--no-deps",
                    "-T",
                    "-v",
                    str(folder / "config.json") + ":/config.json:ro",
                    "load-generator",
                    "/config.json",
                    run,
                    "--include-outcomes",
                ]
                + ([] if args.historical else ["--load"]),
                env=env,
                cwd=ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=errors,
            )
            unique = 0
            try:
                for line in producer.stdout:
                    if not line.startswith("{"):
                        continue
                    row = json.loads(line)
                    log.write(line)
                    showcase.record(db, row)
                    if row["kind"] in ("start", "publishing", "complete"):
                        report.update({k: v for k, v in row.items() if k != "kind"})
                    if row["kind"] == "payment" and not row["retry"]:
                        unique += 1
                        if unique % 1000 == 0:
                            db.commit()
                        if (
                            args.restart_flink
                            and not report.get("recovery")
                            and unique
                            >= config["customers"] * config["min_payments"] * 0.8
                        ):
                            before = showcase.operations.rest(
                                "/jobs/" + report["job_id"] + "/checkpoints"
                            )["latest"]["completed"]
                            report["recovery"] = {
                                "checkpoint_before": before["id"],
                                "requested_at": int(time.time() * 1000),
                            }
                            compose(
                                "kill",
                                "-s",
                                "SIGKILL",
                                "taskmanager",
                                capture_output=True,
                            )
                            compose(
                                "up",
                                "-d",
                                "--no-deps",
                                "taskmanager",
                                capture_output=True,
                            )
                if producer.wait() != 0:
                    raise RuntimeError("Generator failed; inspect producer.log")
            finally:
                if producer.poll() is None:
                    producer.terminate()
                    producer.wait(timeout=30)
            db.commit()
            assert "control_records" in report
            SINK["wait_for"](
                table_name(run, "decisions"),
                report["expected_payments"] - 1,
                timeout=300,
            )
            for suffix in ["dlq", "late"]:
                SINK["wait_for"](table_name(run, suffix), 1, timeout=120)
            report["checkpoint_runtime"] = showcase.operations.rest(
                "/jobs/" + report["job_id"] + "/checkpoints"
            )
            report["checkpoint_configuration"] = showcase.operations.rest(
                "/jobs/" + report["job_id"] + "/checkpoints/config"
            )
            report["operators"] = showcase.operations.rest("/jobs/" + report["job_id"])[
                "vertices"
            ]
            assert report["checkpoint_configuration"]["interval"] == 10000
            assert all(vertex["parallelism"] == 3 for vertex in report["operators"])
            if args.restart_flink:
                restored = report["checkpoint_runtime"]["latest"].get("restored")
                assert (
                    restored
                    and restored["restore_timestamp"]
                    >= report["recovery"]["requested_at"]
                )
                report["recovery"]["restored"] = restored
            stop.set()
            sampler.join(timeout=30)
            report["verification"] = reconcile(db, folder, run, report, compose)
            report["metrics"] = LOAD["summarize_metrics"](
                folder / "metrics.jsonl", report["job_id"]
            )
            report["publishing_metrics"] = LOAD["summarize_metrics"](
                folder / "metrics.jsonl",
                report["job_id"],
                report["publishing_started_at"] + 30000,
                report["publishing_started_at"] + report["publishing_elapsed_ms"],
            )
            report["status"] = "COMPLETE"
        print(
            json.dumps(
                {
                    "run_id": run,
                    "status": report["status"],
                    "verification": report["verification"],
                },
                indent=2,
            ),
            flush=True,
        )
    except BaseException as error:
        report.update(status="FAILED", error=str(error))
        raise
    finally:
        stop.set()
        if sampler:
            sampler.join(timeout=30)
        if report.get("job_id"):
            try:
                report["savepoint"] = showcase.operations.savepoint(stop=True)
            except Exception as error:
                report["stop_error"] = str(error)
        for name in names:
            try:
                SINK["api"]("connectors/" + name, method="DELETE")
            except Exception as error:
                report.setdefault("cleanup_errors", []).append(str(error))
        showcase.write_json(folder / "report.json", report)
        print("Report:", folder / "report.json", flush=True)


if __name__ == "__main__":
    main()
