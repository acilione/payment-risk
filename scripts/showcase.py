"""Generate payments through Kafka/Flink and reconcile stored results, without deleting data."""
import argparse
import fcntl
import hashlib
import json
import os
import sqlite3
import subprocess
import time
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path

import operations

ROOT = Path(__file__).resolve().parents[1]
WORK = ROOT / "artifacts/showcase"
PROJECT = "payment-risk-showcase"
CLASS = "com.portfolio.paymentrisk.tools.ShowcaseProducer"
JAR = "/opt/flink/usrlib/risk-engine.jar"
PROFILES = {
    "normal": ("Regular purchases", "LEGITIMATE", "Purchases on a familiar device, spread across the history period."),
    "known_devices": ("A familiar phone and laptop", "LEGITIMATE", "A customer uses an established phone and laptop for purchases spaced across the day. Multiple devices alone do not establish suspicious activity."),
    "checkout_retry": ("A declined checkout, then success", "LEGITIMATE", "A declined purchase is followed by a fresh approved attempt at the same merchant, amount and device. Both authorizations are distinct transactions; transport retries retain the original transaction ID."),
    "new_device": ("A replacement phone", "LEGITIMATE", "A customer makes a larger purchase from a new device. The generated intent is legitimate; the rules only see the payment facts."),
    "takeover": ("Large purchases from an unfamiliar device", "SUSPICIOUS", "After ordinary purchases on a familiar phone, three large purchases arrive within a few minutes from one unfamiliar device. This models possible account takeover, not simultaneous bank logins."),
    "card_testing": ("Small declines, then a purchase", "SUSPICIOUS", "Small declined attempts precede a larger approved payment on the same new device."),
    "low_burst": ("A burst below the score threshold", "SUSPICIOUS", "Several small payments arrive close together on a familiar device. Frequency alone may not reach the review threshold."),
    "trusted_device": ("Misuse of a familiar device", "SUSPICIOUS", "The final payment is labeled suspicious by the generator, but its observable fields resemble ordinary purchases. These rules cannot infer the hidden intent."),
}

def now():
    return datetime.now(timezone.utc).isoformat()

def write_json(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)

def compose_args():
    return ["docker", "compose", "-p", PROJECT, "--env-file", str(ROOT / ".env"),
            "--env-file", str(WORK / "compose.env"), "-f", str(ROOT / "docker-compose.yml"),
            "-f", str(ROOT / "docker-compose.showcase.yml")]

def compose_environment():
    env = os.environ.copy()
    # Explicit run isolation must not be overridden by exported shell variables.
    for line in (WORK / "compose.env").read_text().splitlines():
        key, value = line.split("=", 1)
        env[key] = value
    credentials = settings()
    for key in ("CLICKHOUSE_USER", "CLICKHOUSE_PASSWORD", "REGISTRY_DB_PASSWORD", "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "GRAFANA_ADMIN_PASSWORD"):
        if key in credentials:
            env[key] = credentials[key]
    return env

def compose(*args, **kwargs):
    return subprocess.run([*compose_args(), *args], cwd=ROOT, env=compose_environment(), check=True, **kwargs)

def settings():
    values = {}
    for line in (ROOT / ".env").read_text().splitlines():
        if line.strip() and not line.lstrip().startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key] = value.strip().strip("\"'")
    return values

def sql(query, **parameters):
    env = settings()
    url = "http://127.0.0.1:28123/?" + urllib.parse.urlencode({"param_" + k: v for k, v in parameters.items()})
    request = urllib.request.Request(url, data=(query + " SETTINGS readonly=1, max_execution_time=20, max_memory_usage=268435456 FORMAT JSON").encode(),
        headers={"X-ClickHouse-User": env.get("CLICKHOUSE_USER", "risk"), "X-ClickHouse-Key": env["CLICKHOUSE_PASSWORD"]})
    with urllib.request.urlopen(request, timeout=25) as response:
        return json.load(response)["data"]

def ledger(path):
    db = sqlite3.connect(path)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS inputs (partition INTEGER, offset INTEGER, hash TEXT, PRIMARY KEY(partition, offset));
        CREATE TABLE IF NOT EXISTS payments (id TEXT PRIMARY KEY, customer TEXT, hash TEXT, payload TEXT, decision TEXT);
        CREATE INDEX IF NOT EXISTS payments_customer ON payments(customer);
    """)
    return db

def record(db, row):
    if row["kind"] not in ("payment", "marker"):
        return
    db.execute("INSERT INTO inputs VALUES(?,?,?)", (row["source_partition"], row["source_offset"], row["payload_sha256"]))
    if row["kind"] == "payment" and not row["retry"]:
        payment = row["payment"]
        facts = {k: v for k, v in payment.items() if k != "producer_time"}
        digest = hashlib.sha256(json.dumps(facts, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        db.execute("INSERT INTO payments VALUES(?,?,?,?,NULL)", (payment["event_id"], payment["customer_id"], digest, json.dumps(payment)))


def reconcile(db, report):
    # Compare bounded pages to a disk-backed ledger, including retry/control coordinates.
    cursor = (-1, -1)
    archived = 0
    while True:
        rows = sql("""SELECT source_partition, source_offset, any(payload_sha256) AS hash, uniqExact(payload_sha256) AS variants
            FROM risk.payment_ingress WHERE source_topic = {topic:String}
            AND (source_partition, source_offset) > ({partition:Int64}, {offset:Int64})
            GROUP BY source_partition, source_offset ORDER BY source_partition, source_offset LIMIT 500""",
            topic=report["input_topic"], partition=cursor[0], offset=cursor[1])
        if not rows:
            break
        for row in rows:
            cursor = (int(row["source_partition"]), int(row["source_offset"]))
            expected = db.execute("SELECT hash FROM inputs WHERE partition=? AND offset=?", cursor).fetchone()
            if expected != (row["hash"],) or int(row["variants"]) != 1:
                raise AssertionError("Archived input is unexpected or has conflicting bytes: " + str(cursor))
            archived += 1
    if archived != db.execute("SELECT count(*) FROM inputs").fetchone()[0]:
        raise AssertionError("Acknowledged input missing from archive")
    after, evaluation, decisions = "", "", 0
    while True:
        rows = sql("""SELECT * FROM risk.decisions_current WHERE source_topic = {topic:String}
            AND startsWith(event_id, {prefix:String}) AND (event_id, evaluation_id) > ({after:String}, {evaluation:String}) ORDER BY event_id, evaluation_id LIMIT 200""",
            topic=report["decision_topic"], prefix=report["run_id"] + "-payment-", after=after, evaluation=evaluation)
        if not rows:
            break
        for row in rows:
            after, evaluation = row["event_id"], row["evaluation_id"]
            expected = db.execute("SELECT hash, decision FROM payments WHERE id=?", (after,)).fetchone()
            if expected is None or expected[0] != row["input_sha256"] or expected[1] is not None:
                raise AssertionError("Unexpected, duplicated or mismatched logical decision: " + after)
            for key in ("amount_minor", "risk_score", "event_time", "processed_at"):
                row[key] = int(row[key])
            row["source_offset"] = str(row["source_offset"])
            row["rule_evidence"] = json.loads(row["rule_evidence"])
            db.execute("UPDATE payments SET decision=? WHERE id=?", (json.dumps(row), after))
            decisions += 1
    db.commit()
    if decisions != report["expected_payments"]:
        raise AssertionError("Missing logical decisions")
    return archived, decisions


def export_snapshot(report, db, target):
    if report["status"] != "COMPLETE":
        raise ValueError("Only reconciled runs can be exported")
    # Export whole customer histories, capped at 1,000 payments. Large runs stay in ClickHouse.
    cases, size = [], 0
    for customer in report["customers"]:
        if size + customer["payments"] > 1000:
            continue
        rows = db.execute("SELECT payload, decision FROM payments WHERE customer=? ORDER BY id", (customer["id"],))
        timeline = [{"payment": json.loads(p), "decision": json.loads(d)} for p, d in rows]
        if len(timeline) != customer["payments"]:
            raise AssertionError("Incomplete customer history")
        final = timeline[-1]
        title, label, story = PROFILES[customer["profile"]]
        alert = final["decision"]["decision"] != "APPROVE"
        outcome = ("TRUE_" if alert == (label == "SUSPICIOUS") else "FALSE_") + ("POSITIVE" if alert else "NEGATIVE")
        cases.append({"scenario": customer["id"], "profile": customer["profile"], "title": title,
                      "story": story, "label": label, "outcome": outcome, "target_payment": final["payment"],
                      "decision": final["decision"], "timeline": timeline})
        size += len(timeline)
    policies = {}
    for case in cases:
        for decision in [case["decision"], *(entry["decision"] for entry in case["timeline"])]:
            if "policy_snapshot" in decision:
                policy = decision.pop("policy_snapshot")
                reference = hashlib.sha256(policy.encode()).hexdigest()
                policies[reference] = policy
                decision["policy_ref"] = reference
    write_json(target, {"provenance": {**report, "exported_payments": size, "exported_customers": len(cases)}, "policies": policies, "cases": cases})
    print(f"Exported {size} stored decisions from {len(cases)} complete customer histories to {target}", flush=True)


def run(args):
    WORK.mkdir(parents=True, exist_ok=True)
    config = args.config.resolve()
    json.loads(config.read_text())
    running = subprocess.check_output(["docker", "ps", "-q", "--filter", "label=com.docker.compose.project=" + PROJECT], text=True).strip()
    if running:
        raise RuntimeError("The showcase stack is already running. Use make showcase-stop before creating another run.")
    regular = subprocess.check_output(["docker", "ps", "-q", "--filter", "label=com.docker.compose.project=payment-risk"], text=True).strip()
    if regular:
        raise RuntimeError("The regular payment-risk stack uses the same ports. Stop its job with a savepoint and run make down first.")
    subprocess.run(["python3", "scripts/init-env.py"], cwd=ROOT, check=True)
    run_id = "show-" + uuid.uuid4().hex[:12]
    run_dir = WORK / run_id
    run_dir.mkdir()
    (run_dir / "config.json").write_bytes(config.read_bytes())
    env = {"TRANSACTIONAL_PREFIX": run_id, "KAFKA_GROUP": run_id, "ARCHIVER_GROUP": run_id + "-archive", "MATERIALIZER_GROUP": run_id + "-materializer"}
    for key, suffix in [("PAYMENTS", "payments"), ("RULES", "rules"), ("DECISIONS", "decisions"), ("DLQ", "dlq"), ("LATE", "late")]:
        env[key + "_TOPIC"] = run_id + "." + suffix
    (WORK / "compose.env").write_text("".join(k + "=" + v + "\n" for k, v in env.items()))
    if not args.no_build:
        compose("build", "jobmanager", "website", "minio", "minio-init")
    subprocess.run(["docker", "run", "--rm", "--entrypoint", "java", "-v", str(config) + ":/config.json:ro",
                    "payment-risk:0.1.0", "-cp", JAR, CLASS, "/config.json", "--validate"], check=True)
    report = {"run_id": run_id, "status": "STARTING", "started_at": now(), "config": json.loads(config.read_text()),
              "source": "Kafka / Flink / ClickHouse", "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "source_dirty": bool(subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=normal"], cwd=ROOT, text=True).strip())}
    def publish():
        report["updated_at"] = now()
        write_json(run_dir / "report.json", report)
        write_json(WORK / "latest.json", report)
    publish()
    try:
        compose("up", "-d", "--no-build")
        operations.ensure_no_active_job()
        compose("exec", "-T", "jobmanager", "flink", "run", "-d", JAR)
        report["job_id"] = operations.wait_running()
        report["status"] = "GENERATING"
        publish()
        compose("cp", str(run_dir / "config.json"), "jobmanager:/tmp/showcase-config.json")
        with ledger(run_dir / "ledger.sqlite") as db, (run_dir / "inputs.jsonl").open("w") as output, (run_dir / "producer.log").open("w") as errors:
            process = subprocess.Popen([*compose_args(), "exec", "-T", "jobmanager", "java", "-cp", JAR, CLASS, "/tmp/showcase-config.json", run_id],
                                       cwd=ROOT, env=compose_environment(), stdout=subprocess.PIPE, stderr=errors, text=True)
            last_progress = time.monotonic()
            try:
                for line in process.stdout:
                    if not line.startswith("{"):
                        errors.write(line)
                        continue
                    row = json.loads(line)
                    output.write(line)
                    output.flush()
                    record(db, row)
                    if row["kind"] == "payment":
                        report["acknowledged_deliveries"] = report.get("acknowledged_deliveries", 0) + 1
                        counter = "transport_retries" if row["retry"] else "acknowledged_payments"
                        report[counter] = report.get(counter, 0) + 1
                        if time.monotonic() - last_progress >= 1:
                            db.commit()
                            publish()
                            last_progress = time.monotonic()
                    if row["kind"] in ("start", "complete"):
                        report.update({k: v for k, v in row.items() if k != "kind"})
                        publish()
                if process.wait() != 0:
                    raise RuntimeError("Payment producer failed; see " + str(run_dir / "producer.log"))
            finally:
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=15)
            db.commit()
            if "control_records" not in report:
                raise RuntimeError("Producer did not finish")
            report["status"] = "VERIFYING"
            publish()
            deadline = time.monotonic() + 240
            while True:
                counts = sql("""SELECT
                    (SELECT uniqExact(tuple(source_partition, source_offset)) FROM risk.payment_ingress WHERE source_topic={raw:String}) AS archived,
                    (SELECT count() FROM risk.decisions_current WHERE source_topic={decisions:String} AND startsWith(event_id,{prefix:String})) AS decisions,
                    (SELECT count() FROM risk.integrity_conflicts) + (SELECT count() FROM risk.materializer_rejections FINAL) AS conflicts""",
                    raw=report["input_topic"], decisions=report["decision_topic"], prefix=run_id + "-payment-")[0]
                report["archived_records"] = int(counts["archived"])
                report["stored_decisions"] = int(counts["decisions"])
                publish()
                if int(counts["conflicts"]):
                    raise AssertionError("ClickHouse reports an integrity conflict or quarantined decision")
                if report["archived_records"] >= report["acknowledged_deliveries"] + report["control_records"] and report["stored_decisions"] >= report["expected_payments"]:
                    break
                if time.monotonic() > deadline:
                    raise TimeoutError("Storage reconciliation timed out; inputs and logs are retained")
                time.sleep(3)
            report["archived_records"], report["stored_decisions"] = reconcile(db, report)
            report["decision_counts"] = {row["decision"]: int(row["n"]) for row in sql("SELECT decision, count() AS n FROM risk.decisions_current WHERE source_topic={topic:String} AND startsWith(event_id,{prefix:String}) GROUP BY decision", topic=report["decision_topic"], prefix=run_id + "-payment-")}
            digest = hashlib.sha256()
            with (run_dir / "inputs.jsonl").open("rb") as source:
                for block in iter(lambda: source.read(65536), b""):
                    digest.update(block)
            report["input_log_sha256"] = digest.hexdigest()
            report["verification"] = "Every acknowledged Kafka coordinate and payload hash archived; exactly one matching logical decision per generated payment."
            report["status"] = "COMPLETE"
            publish()
            export_snapshot(report, db, run_dir / "snapshot.json")
        print(json.dumps({k: report[k] for k in ("run_id", "status", "expected_payments", "transport_retries", "archived_records", "stored_decisions", "decision_counts")}, indent=2))
        print("Open http://localhost:23001 and select Showcase. Stop with make showcase-stop; data volumes are retained.")
    except BaseException as error:
        report.update(status="FAILED", error=str(error))
        publish()
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["run", "stop", "export"])
    parser.add_argument("--config", type=Path, default=ROOT / "config/showcase.json")
    parser.add_argument("--no-build", action="store_true", help="Use images already built from this checkout")
    args = parser.parse_args()
    if args.action == "run":
        run(args)
    elif args.action == "stop":
        operations.docker = compose
        own_jobmanager = subprocess.check_output(["docker", "ps", "-q", "--filter", "label=com.docker.compose.project=" + PROJECT,
                                                  "--filter", "label=com.docker.compose.service=jobmanager"], text=True).strip()
        if own_jobmanager:
            active = [job for job in operations.rest("/jobs/overview")["jobs"]
                      if job["state"] not in ("FINISHED", "CANCELED", "FAILED")]
            if len(active) > 1 or any(job["name"] != "payment-risk-v1" for job in active):
                raise RuntimeError("Unexpected active jobs; inspect the showcase stack before stopping it")
            if active:
                operations.savepoint(stop=True)
        compose("down")
    else:
        report = json.loads((WORK / "latest.json").read_text())
        with ledger(WORK / report["run_id"] / "ledger.sqlite") as db:
            export_snapshot(report, db, ROOT / "web/src/showcase-data.json")

if __name__ == "__main__":
    WORK.mkdir(parents=True, exist_ok=True)
    with (WORK / "command.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit("Another showcase command is active. Wait for it or interrupt its terminal first.")
        main()
