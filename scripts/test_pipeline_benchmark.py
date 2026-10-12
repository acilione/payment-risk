import importlib.util
import sqlite3
import unittest
import json
import tempfile
from unittest.mock import patch
from pathlib import Path

spec = importlib.util.spec_from_file_location("pipeline_benchmark", Path(__file__).with_name("benchmark-pipeline.py"))
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)


class PipelineBenchmarkTest(unittest.TestCase):
    def test_sustained_load_distinguishes_stability_backlog_and_driver_stalls(self):
        from load_analysis import sustained_summary
        with sqlite3.connect(":memory:") as db:
            db.execute("CREATE TABLE benchmark_arrivals(id TEXT,scheduled_at INTEGER,producer_at INTEGER,stored_at INTEGER)")
            start = 1000000
            db.executemany("INSERT INTO benchmark_arrivals VALUES (?,?,?,?)",
                           ((str(i), start+i*10, start+i*10, start+i*10+5000) for i in range(10000)))
            report = {"load_mode": True, "stored_decisions": 10000, "publishing_started_at": start,
                      "config": {"publish_rate": 100}, "achieved_payments_per_second": 100}
            stable = sustained_summary(db, report, 20, 20)
            self.assertEqual(stable["status"], "observed_stable")
            self.assertEqual([500]*4, [w["offered_backlog"] for w in stable["windows"]])
            self.assertEqual(stable["backlog_growth_per_second"], 0)
            db.execute("UPDATE benchmark_arrivals SET stored_at=scheduled_at+(scheduled_at-?)+5000", (start,))
            self.assertEqual(sustained_summary(db, report, 20, 20)["status"], "backlog_or_latency_growing")
            # Even with stable storage latency after send, an overloaded producer cannot pass.
            db.execute("UPDATE benchmark_arrivals SET producer_at=scheduled_at+5000,stored_at=scheduled_at+10000")
            limited = sustained_summary(db, report, 20, 20)
            self.assertEqual(limited["status"], "generator_limited")
            self.assertEqual(limited["windows"][0]["scheduled_to_storage_p95_ms"], 10000)
            self.assertEqual(sustained_summary(db, report, 60, 20)["status"], "insufficient_evidence")
            db.execute("DELETE FROM benchmark_arrivals")
            self.assertEqual(sustained_summary(db, report)["status"], "insufficient_evidence")

    def test_stage_latencies_cover_reconciled_payments(self):
        with sqlite3.connect(":memory:") as db:
            db.executescript("CREATE TABLE payments(id TEXT,payload TEXT,decision TEXT); CREATE TABLE inputs(partition INTEGER,offset INTEGER,event_id TEXT); CREATE TABLE acknowledgements(partition INTEGER,offset INTEGER,observed_at INTEGER);")
            db.execute("INSERT INTO payments VALUES (?,?,?)", ("run-payment-1", json.dumps({"producer_time": 1000}), "assessed"))
            db.execute("INSERT INTO inputs VALUES (0,1,'run-payment-1')")
            db.execute("INSERT INTO acknowledgements VALUES (0,1,1010)")
            report = {"run_id": "run", "decision_topic": "decisions", "stored_decisions": 1}
            rows = [{"event_id": "run-payment-1", "evaluated_at": 3000, "stored_at": 8000}]
            with patch.object(benchmark.showcase, "sql", side_effect=[rows, []]):
                result = benchmark.measure_latency(db, report)
            self.assertEqual(result["p95_ms"], 7000)
            self.assertEqual(result["stages"]["producer_to_evaluation"]["p95_ms"], 2000)
            self.assertEqual(result["stages"]["evaluation_to_storage_including_checkpoint"]["p95_ms"], 5000)
            self.assertEqual(result["stages"]["producer_to_acknowledgement"]["p95_ms"], 10)
            rows[0]["evaluated_at"] = 9000
            with patch.object(benchmark.showcase, "sql", return_value=rows), self.assertRaisesRegex(AssertionError, "stage latency"):
                benchmark.measure_latency(db, report)

    def test_missing_telemetry_is_not_reported_as_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "metrics.jsonl"
            path.write_text(json.dumps({"error": "unavailable"}) + "\n" + json.dumps({"metrics": [{"metric": {"__name__": "lag"}, "value": None}]}) + "\n")
            result = benchmark.summarize_metrics(path)
            self.assertEqual(result["sample_errors"], 1)
            self.assertEqual(result["series"], {})

    def test_exact_disk_backed_percentiles_and_empty_failure(self):
        with sqlite3.connect(":memory:") as db:
            db.execute("CREATE TABLE benchmark_latency(id TEXT PRIMARY KEY, milliseconds INTEGER)")
            with self.assertRaises(AssertionError):
                benchmark.latency_summary(db)
            db.executemany("INSERT INTO benchmark_latency VALUES (?,?)", ((str(i), i) for i in range(100, 0, -1)))
            self.assertEqual({"samples": 100, "p50_ms": 50, "p95_ms": 95, "p99_ms": 99, "max_ms": 100}, benchmark.latency_summary(db))


if __name__ == "__main__":
    unittest.main()
