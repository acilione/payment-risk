import runpy
import io
import json
import sqlite3
import sys
from pathlib import Path
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
BENCHMARK = runpy.run_path(str(Path(__file__).with_name("benchmark-full-pipeline.py")))
CHECK = BENCHMARK["clock_check"]


class ClockConsistencyTest(unittest.TestCase):
    def test_wall_clock_drift_is_not_misreported_as_generator_saturation(self):
        result = CHECK(
            118000, {"publishing_started_at": 1000, "publishing_elapsed_ms": 110000}, 0
        )
        self.assertEqual(result["status"], "clock_unstable")
        self.assertEqual(result["wall_advance_lower_bound_ms"], 7000)

    def test_last_send_can_precede_monotonic_completion(self):
        result = CHECK(
            110000, {"publishing_started_at": 1000, "publishing_elapsed_ms": 110000}, 0
        )
        self.assertEqual(result["status"], "no_detected_clock_error")
        self.assertEqual(CHECK(None, {}, 1)["status"], "clock_unstable")


class AggregateReconciliationTest(unittest.TestCase):
    def test_correct_grand_totals_do_not_hide_wrong_bucket(self):
        with sqlite3.connect(":memory:") as db:
            db.execute("CREATE TABLE outputs(kind TEXT,payload TEXT)")
            db.execute(
                "INSERT INTO outputs VALUES('decisions',?)",
                (
                    json.dumps(
                        {
                            "event_time": 10000,
                            "decision": "REVIEW",
                            "currency": "EUR",
                            "amount_minor": 100,
                            "risk_score": 35,
                        }
                    ),
                ),
            )
            misplaced = {
                "bucket": 20,
                "decision": "REVIEW",
                "currency": "EUR",
                "payments": 1,
                "amount_minor": 100,
                "risk_score_sum": 35,
            }
            verifier = BENCHMARK["verify_buckets"]
            with patch.dict(
                verifier.__globals__["SINK"],
                environment=lambda: {"CLICKHOUSE_PASSWORD": "test"},
            ):
                with patch(
                    "urllib.request.urlopen",
                    return_value=io.BytesIO(json.dumps(misplaced).encode()),
                ):
                    with self.assertRaises(AssertionError):
                        verifier(db, "test")


if __name__ == "__main__":
    unittest.main()
