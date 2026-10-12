"""Reconciliation rejects missing inputs, changed bytes and mismatched decisions."""
import json
import io
import urllib.error
import unittest
from unittest.mock import patch
import showcase

class ReconciliationTest(unittest.TestCase):
    def setUp(self):
        self.db = showcase.ledger(":memory:")
        self.addCleanup(self.db.close)
        self.publication_check = showcase.verify_publication
        check = patch.object(showcase, "verify_publication")
        check.start()
        self.addCleanup(check.stop)
        self.payment = {"event_id": "show-test-payment-000000", "customer_id": "show-test-customer-0000", "amount_minor": 100, "producer_time": 123}
        self.record = {"kind": "payment", "retry": False, "source_partition": 0, "source_offset": 0, "payload_sha256": "wirehash", "payment": self.payment}
        showcase.record(self.db, self.record)
        showcase.record(self.db, {**self.record, "retry": True, "source_offset": 1})
        self.report = {"input_topic": "show-test.payments", "decision_topic": "show-test.decisions", "run_id": "show-test", "expected_payments": 1, "verification_generation": "test-generation"}
        self.archived = [{"source_partition": 0, "source_offset": n, "hash": "wirehash", "variants": 1} for n in (0,1)]
        digest = self.db.execute("SELECT hash FROM payments").fetchone()[0]
        self.decision = {"evaluation_id": "eval-1", "event_id": self.payment["event_id"], "input_sha256": digest, "amount_minor": "100", "risk_score": 0, "event_time": "123", "processed_at": "456", "source_offset": "0", "rule_evidence": "[]"}

    def test_retries_preserve_every_input_but_only_one_decision(self):
        with patch.object(showcase, "sql", side_effect=[self.archived, [], [{"payload": self.decision}], [], [], []]) as sql:
            self.assertEqual(showcase.reconcile(self.db, self.report), (2,1))
        self.assertIn("risk.assessments", sql.call_args_list[2].args[0])
        self.assertNotIn("GROUP BY", sql.call_args_list[2].args[0])
        self.assertEqual(self.db.execute("SELECT count(*) FROM payments").fetchone()[0], 1)

    def test_verification_rejects_expired_missing_or_blocked_publications(self):
        valid = {"ready": True, "blocked": False}
        with patch.object(showcase, "sql", return_value=[valid]):
            self.publication_check(self.report)
        for rows in ([], [{**valid, "ready": False}], [{**valid, "blocked": True}]):
            with patch.object(showcase, "sql", return_value=rows), self.assertRaisesRegex(AssertionError, "incomplete or blocked"):
                self.publication_check(self.report)

    def test_query_resource_errors_are_not_retried_as_database_outages(self):
        error = urllib.error.HTTPError("http://localhost", 500, "error", {}, io.BytesIO(b"Code: 241. Memory limit exceeded"))
        with patch.object(showcase, "settings", return_value={"CLICKHOUSE_PASSWORD": "test"}), patch.object(showcase.urllib.request, "urlopen", side_effect=error):
            with self.assertRaisesRegex(RuntimeError, "database code 241"):
                showcase.sql("SELECT 1")

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
        with patch.object(showcase, "sql", side_effect=[self.archived, [], [{"payload": self.decision}]]):
            with self.assertRaisesRegex(AssertionError, "mismatched logical decision"):
                showcase.reconcile(self.db, self.report)

    def test_duplicate_event_on_another_page_fails(self):
        other = {**self.decision, "evaluation_id": "eval-2"}
        with patch.object(showcase, "sql", side_effect=[self.archived, [], [{"payload": self.decision}], [{"payload": other}]]):
            with self.assertRaisesRegex(AssertionError, "duplicated"):
                showcase.reconcile(self.db, self.report)

    def test_missing_decision_fails(self):
        with patch.object(showcase, "sql", side_effect=[self.archived, [], [], [], []]):
            with self.assertRaisesRegex(AssertionError, "Missing processing outcomes"):
                showcase.reconcile(self.db, self.report)

    def test_partial_reconciliation_reports_missing_coordinates_and_payments(self):
        with patch.object(showcase, "sql", side_effect=[self.archived[:1], [], [], [], []]):
            self.assertEqual(showcase.reconcile(self.db, self.report, require_complete=False), (1, 0))
        check = self.report["reconciliation"]
        self.assertEqual(check["missing_archive"], 1)
        self.assertEqual(check["without_decision"], 1)
        self.assertEqual(check["missing_input_examples"], [{"partition": 0, "offset": "1"}])
        self.assertEqual(check["missing_payment_examples"], [self.payment["event_id"]])

    def test_reconciliation_can_resume_without_counting_previous_observations_twice(self):
        for _ in range(2):
            with patch.object(showcase, "sql", side_effect=[self.archived, [], [{"payload": self.decision}], [], [], []]):
                self.assertEqual(showcase.reconcile(self.db, self.report), (2, 1))
        self.assertEqual(self.report["reconciliation"]["without_decision"], 0)

    def test_late_payment_is_accounted_for_without_fabricating_an_assessment(self):
        late = {"event_id": self.payment["event_id"], "hash": self.decision["input_sha256"], "reason_value": "LATE", "cutoff": 200, "delay_ms": 77, "variants": 1}
        with patch.object(showcase, "sql", side_effect=[self.archived, [], [], [], [late], []]):
            self.assertEqual(showcase.reconcile(self.db, self.report), (2, 0))
        check = self.report["reconciliation"]
        self.assertEqual((check["late_payments"], check["without_decision"], check["unresolved_inputs"]), (1, 1, 0))

    def test_invalid_records_match_source_coordinates_and_wire_hash(self):
        invalid = [{"input_partition": 0, "input_offset": n, "hash": "wirehash", "variants": 1} for n in (0, 1)]
        with patch.object(showcase, "sql", side_effect=[self.archived, [], [], invalid, [], []]):
            showcase.reconcile(self.db, self.report)
        check = self.report["reconciliation"]
        self.assertEqual((check["invalid_inputs"], check["invalid_payments"], check["unresolved_inputs"]), (2, 1, 0))

    def test_invalid_input_without_event_identity_is_still_accounted_for(self):
        showcase.record(self.db, {"kind": "invalid", "source_partition": 0, "source_offset": 2, "payload_sha256": "invalid-wire"})
        archived = self.archived + [{"source_partition": 0, "source_offset": 2, "hash": "invalid-wire", "variants": 1}]
        invalid = [{"input_partition": 0, "input_offset": 2, "hash": "invalid-wire", "variants": 1}]
        with patch.object(showcase, "sql", side_effect=[archived, [], [{"payload": self.decision}], [], invalid, [], []]):
            self.assertEqual(showcase.reconcile(self.db, self.report), (3, 1))
        self.assertEqual(self.report["reconciliation"]["invalid_inputs"], 1)

    def test_conflicting_late_and_evaluated_outcomes_fail(self):
        late = {"event_id": self.payment["event_id"], "hash": self.decision["input_sha256"], "variants": 1}
        with patch.object(showcase, "sql", side_effect=[self.archived, [], [{"payload": self.decision}], [], [], [late]]):
            with self.assertRaisesRegex(AssertionError, "conflicts with an assessment"):
                showcase.reconcile(self.db, self.report)

    def test_invalid_evidence_with_wrong_bytes_fails(self):
        invalid = [{"input_partition": 0, "input_offset": 0, "hash": "different", "variants": 1}]
        with patch.object(showcase, "sql", side_effect=[self.archived, [], [], invalid]):
            with self.assertRaisesRegex(AssertionError, "conflicts with the ledger"):
                showcase.reconcile(self.db, self.report)

    def test_unexplained_duplicate_delivery_remains_unresolved(self):
        invalid = [{"input_partition": 0, "input_offset": 0, "hash": "wirehash", "variants": 1}]
        with patch.object(showcase, "sql", side_effect=[self.archived, [], [], invalid, [], []]):
            showcase.reconcile(self.db, self.report, require_complete=False)
        self.assertEqual(self.report["reconciliation"]["unresolved_inputs"], 1)
        self.assertEqual(self.report["reconciliation"]["invalid_payments"], 0)

    def test_conflicting_side_output_deliveries_are_not_hidden_by_grouping(self):
        invalid = [{"input_partition": 0, "input_offset": 0, "hash": "wirehash", "variants": 2}]
        with patch.object(showcase, "sql", side_effect=[self.archived, [], [], invalid]):
            with self.assertRaisesRegex(AssertionError, "conflicts with the ledger"):
                showcase.reconcile(self.db, self.report)

    def test_late_evidence_is_reconciled_fresh_after_repeated_checks(self):
        late = {"event_id": self.payment["event_id"], "hash": self.decision["input_sha256"], "reason_value": "TOO_LATE", "cutoff": 200, "delay_ms": 77, "variants": 1}
        for _ in range(2):
            with patch.object(showcase, "sql", side_effect=[self.archived, [], [], [], [late], []]):
                showcase.reconcile(self.db, self.report)
            self.assertEqual(self.report["reconciliation"]["late_payments"], 1)
            self.assertEqual(self.report["reconciliation"]["unresolved_inputs"], 0)

    def test_offset_comparison_allows_initialization_but_never_consumption(self):
        before = {"archive": [{"topic": "raw", "partition": 0, "committed": -1}]}
        during = {"archive": [{"topic": "raw", "partition": 0, "committed": 0}]}
        showcase.assert_offsets_unchanged(before, during)
        during["archive"][0]["committed"] = 1
        with self.assertRaisesRegex(AssertionError, "committed offsets"):
            showcase.assert_offsets_unchanged(before, during)

if __name__ == "__main__":
    unittest.main()
