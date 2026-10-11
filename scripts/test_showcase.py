"""Reconciliation rejects missing inputs, changed bytes and mismatched decisions."""
import json
import unittest
from unittest.mock import patch
import showcase

class ReconciliationTest(unittest.TestCase):
    def setUp(self):
        self.db = showcase.ledger(":memory:")
        self.addCleanup(self.db.close)
        self.payment = {"event_id": "show-test-payment-000000", "customer_id": "show-test-customer-0000", "amount_minor": 100, "producer_time": 123}
        self.record = {"kind": "payment", "retry": False, "source_partition": 0, "source_offset": 0, "payload_sha256": "wirehash", "payment": self.payment}
        showcase.record(self.db, self.record)
        showcase.record(self.db, {**self.record, "retry": True, "source_offset": 1})
        self.report = {"input_topic": "show-test.payments", "decision_topic": "show-test.decisions", "run_id": "show-test", "expected_payments": 1}
        self.archived = [{"source_partition": 0, "source_offset": n, "hash": "wirehash", "variants": 1} for n in (0,1)]
        digest = self.db.execute("SELECT hash FROM payments").fetchone()[0]
        self.decision = {"evaluation_id": "eval-1", "event_id": self.payment["event_id"], "input_sha256": digest, "amount_minor": "100", "risk_score": 0, "event_time": "123", "processed_at": "456", "source_offset": "0", "rule_evidence": "[]"}

    def test_retries_preserve_every_input_but_only_one_decision(self):
        with patch.object(showcase, "sql", side_effect=[self.archived, [], [self.decision], []]):
            self.assertEqual(showcase.reconcile(self.db, self.report), (2,1))
        self.assertEqual(self.db.execute("SELECT count(*) FROM payments").fetchone()[0], 1)

    def test_missing_retry_is_a_data_loss_failure(self):
        with patch.object(showcase, "sql", side_effect=[self.archived[:1], []]):
            with self.assertRaisesRegex(AssertionError, "missing from archive"):
                showcase.reconcile(self.db, self.report)

    def test_conflicting_archive_bytes_fail(self):
        self.archived[0]["variants"] = 2
        with patch.object(showcase, "sql", return_value=self.archived):
            with self.assertRaisesRegex(AssertionError, "conflicting bytes"):
                showcase.reconcile(self.db, self.report)

    def test_same_counts_with_wrong_decision_content_fail(self):
        self.decision["input_sha256"] = "changed"
        with patch.object(showcase, "sql", side_effect=[self.archived, [], [self.decision]]):
            with self.assertRaisesRegex(AssertionError, "mismatched logical decision"):
                showcase.reconcile(self.db, self.report)

    def test_duplicate_event_on_another_page_fails(self):
        other = {**self.decision, "evaluation_id": "eval-2"}
        with patch.object(showcase, "sql", side_effect=[self.archived, [], [self.decision], [other]]):
            with self.assertRaisesRegex(AssertionError, "duplicated"):
                showcase.reconcile(self.db, self.report)

    def test_missing_decision_fails(self):
        with patch.object(showcase, "sql", side_effect=[self.archived, [], []]):
            with self.assertRaisesRegex(AssertionError, "Missing logical decisions"):
                showcase.reconcile(self.db, self.report)

    def test_partial_reconciliation_reports_missing_coordinates_and_payments(self):
        with patch.object(showcase, "sql", side_effect=[self.archived[:1], [], []]):
            self.assertEqual(showcase.reconcile(self.db, self.report, require_complete=False), (1, 0))
        check = self.report["reconciliation"]
        self.assertEqual(check["missing_archive"], 1)
        self.assertEqual(check["without_decision"], 1)
        self.assertEqual(check["missing_input_examples"], [{"partition": 0, "offset": "1"}])
        self.assertEqual(check["missing_payment_examples"], [self.payment["event_id"]])

    def test_reconciliation_can_resume_without_counting_previous_observations_twice(self):
        for _ in range(2):
            with patch.object(showcase, "sql", side_effect=[self.archived, [], [self.decision], []]):
                self.assertEqual(showcase.reconcile(self.db, self.report), (2, 1))
        self.assertEqual(self.report["reconciliation"]["without_decision"], 0)

    def test_offset_comparison_allows_initialization_but_never_consumption(self):
        before = {"archive": [{"topic": "raw", "partition": 0, "committed": -1}]}
        during = {"archive": [{"topic": "raw", "partition": 0, "committed": 0}]}
        showcase.assert_offsets_unchanged(before, during)
        during["archive"][0]["committed"] = 1
        with self.assertRaisesRegex(AssertionError, "committed offsets"):
            showcase.assert_offsets_unchanged(before, during)

if __name__ == "__main__":
    unittest.main()
