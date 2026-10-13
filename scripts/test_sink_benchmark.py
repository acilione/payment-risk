"""Check that the storage benchmark does not confuse offered load with accepted load."""
import runpy
from pathlib import Path
import unittest

BENCHMARK = runpy.run_path(str(Path(__file__).with_name('benchmark-clickhouse-kafka.py')))


class BacklogTest(unittest.TestCase):
    def test_only_acknowledged_records_before_the_storage_observation_count(self):
        acknowledgements = [{'at_ms': 1000, 'acknowledged': 100},
                            {'at_ms': 2000, 'acknowledged': 200}]
        result = BENCHMARK['backlog_summary'](
            [[500, 1], [1500, 81], [1900, 151], [2500, 176]], acknowledgements)
        self.assertEqual(result, {'maximum_observed_lower_bound': 25,
                                  'last_observed_lower_bound': 25})

    def test_stored_rows_can_overtake_an_older_acknowledgement_sample(self):
        result = BENCHMARK['backlog_summary'](
            [[1500, 151]], [{'at_ms': 1000, 'acknowledged': 100}])
        self.assertEqual(result['maximum_observed_lower_bound'], 0)


if __name__ == '__main__':
    unittest.main()
