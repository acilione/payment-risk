import importlib.util
import sqlite3
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("pipeline_benchmark", Path(__file__).with_name("benchmark-pipeline.py"))
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)


class PipelineBenchmarkTest(unittest.TestCase):
    def test_exact_disk_backed_percentiles_and_empty_failure(self):
        with sqlite3.connect(":memory:") as db:
            db.execute("CREATE TABLE benchmark_latency(id TEXT PRIMARY KEY, milliseconds INTEGER)")
            with self.assertRaises(AssertionError):
                benchmark.latency_summary(db)
            db.executemany("INSERT INTO benchmark_latency VALUES (?,?)", ((str(i), i) for i in range(100, 0, -1)))
            self.assertEqual({"samples": 100, "p50_ms": 50, "p95_ms": 95, "p99_ms": 99, "max_ms": 100}, benchmark.latency_summary(db))


if __name__ == "__main__":
    unittest.main()
