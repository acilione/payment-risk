"""Bounded replay, coordinate reconciliation and PostgreSQL delivery regression tests.
Run only against the local synthetic Compose deployment. Test rows use a separate database.
"""
import hashlib
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def rows(query):
    output = subprocess.check_output(['docker', 'compose', 'exec', '-T', 'website', 'node', 'server/postgres-query.mjs'],
        input=json.dumps({'query': query, 'parameters': []}), cwd=ROOT, text=True)
    return json.loads(output)


def counts():
    return rows('SELECT sum(payments) AS logical, sum(amount_minor) AS amount FROM risk.time_buckets')[0]


def replay(execute):
    command = ['docker', 'compose', 'run', '--rm', '--no-deps', '--entrypoint', 'java', 'materializer',
               '-cp', '/opt/flink/usrlib/risk-engine.jar', 'com.portfolio.paymentrisk.tools.MaterializerReplay']
    if execute:
        command += ['--execute', '--reason', 'synthetic integration replay verification']
    output = subprocess.check_output(command, cwd=ROOT, text=True, timeout=660)
    report = json.loads([line for line in output.splitlines() if line.startswith('{')][-1])
    assert report['status'] == 'COMPLETE' and report['records'] > 0 and report['rejected'] == 0, report
    conditions = ' OR '.join(f"(source_topic='{r['topic']}' AND source_partition={r['partition']} AND source_offset>={r['start']} AND source_offset<{r['end_exclusive']})" for r in report['ranges'])
    actual = rows('SELECT source_topic,source_partition,source_offset FROM risk.deliveries WHERE ' + conditions)
    assert len(actual) == report['records'], (len(actual), report)
    hashes = {}
    for row in sorted(actual, key=lambda r: (r['source_topic'], int(r['source_partition']), int(r['source_offset']))):
        key = f"{row['source_topic']}:{row['source_partition']}"
        hashes.setdefault(key, hashlib.sha256()).update((str(row['source_offset']) + '\n').encode())
    digest_input = json.dumps({key: value.hexdigest() for key, value in sorted(hashes.items())}, separators=(',', ':'))
    assert report['source_positions_digest_format'] == 'partition-offsets-v2'
    assert hashlib.sha256(digest_input.encode()).hexdigest() == report['source_positions_sha256'], report
    return report


# The first replay also waits for all committed decisions to reach the analytical store.
first = replay(True)
baseline = counts()
second = replay(True)
assert first['ranges'] == second['ranges'], 'Run verification while synthetic generation is stopped'
assert counts() == baseline and int(baseline['logical']) > 0, (baseline, counts())
# The default mode must leave physical deliveries untouched.
physical = rows('SELECT sum(deliveries) AS n FROM risk.topic_totals')[0]
dry = replay(False)
assert rows('SELECT sum(deliveries) AS n FROM risk.topic_totals')[0] == physical

# When customer simulation was run, reconcile its complete timelines with the dashboard's view.
simulation_path = ROOT / 'artifacts/customer-simulation.json'
simulation_check = None
if simulation_path.exists():
    simulation = json.loads(simulation_path.read_text())
    expected = {entry['decision']['event_id']: entry['decision']
                for case in simulation['cases'] for entry in case['timeline']}
    run_id = simulation['run_id']
    if not run_id.startswith('sim-') or any(c not in 'sim-0123456789abcdef' for c in run_id):
        raise ValueError('Unexpected simulation run ID')
    stored = [row['payload'] for row in rows("SELECT payload FROM risk.assessments WHERE starts_with(customer_id, '" + run_id + "-')")]
    actual = {row['event_id']: row for row in stored if '-marker-' not in row['event_id']}
    assert len(actual) == len(stored) - sum('-marker-' in row['event_id'] for row in stored)
    assert actual.keys() == expected.keys(), 'Customer simulation coverage differs in PostgreSQL'
    for event_id, row in actual.items():
        for field in ('evaluation_id', 'transaction_id', 'risk_score', 'decision', 'matched_rules'):
            assert row[field] == expected[event_id][field], (event_id, field)
        assert int(row['amount_minor']) == expected[event_id]['amount_minor'], event_id
    simulation_check = {'run_id': run_id, 'unique_payments': len(actual),
                        'amount_minor': sum(int(row['amount_minor']) for row in actual.values())}

# Adversarial identity and rollback cases run in scripts/verify-postgres.mjs.

report = {'result': 'PASS', 'baseline': baseline, 'first_replay': first, 'second_replay': second, 'dry_run': dry,
          'customer_simulation': simulation_check,
          'checks': ['exact source-coordinate reconciliation', 'stable totals after replay', 'dry run has no inserts']}
(ROOT / 'artifacts').mkdir(exist_ok=True)
(ROOT / 'artifacts/materializer-replay.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report))
