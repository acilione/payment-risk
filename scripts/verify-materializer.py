"""Bounded replay, coordinate reconciliation and ClickHouse integrity regression tests.
Run only against the local synthetic Compose deployment. Test rows use a separate database.
"""
import hashlib
import json
import os
import subprocess
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
env = os.environ.copy()
env.update(dict(line.split('=', 1) for line in (ROOT / '.env').read_text().splitlines() if line and not line.startswith('#')))


def sql(query):
    request = urllib.request.Request('http://localhost:28123/?wait_end_of_query=1', data=query.encode(),
        headers={'X-ClickHouse-User': env['CLICKHOUSE_USER'], 'X-ClickHouse-Key': env['CLICKHOUSE_PASSWORD']})
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read().decode()


def rows(query):
    return [json.loads(line) for line in sql(query + ' FORMAT JSONEachRow').splitlines()]


def counts():
    return rows('SELECT count() AS logical, sum(amount_minor) AS amount FROM risk.decisions_current')[0]


def replay(execute):
    command = ['docker', 'compose', 'run', '--rm', '--no-deps', '--entrypoint', 'java', 'materializer',
               '-cp', '/opt/flink/usrlib/risk-engine.jar', 'com.portfolio.paymentrisk.tools.MaterializerReplay']
    if execute:
        command += ['--execute', '--reason', 'synthetic integration replay verification']
    output = subprocess.check_output(command, cwd=ROOT, text=True, timeout=660)
    report = json.loads([line for line in output.splitlines() if line.startswith('{')][-1])
    assert report['status'] == 'COMPLETE' and report['records'] > 0 and report['rejected'] == 0, report
    conditions = ' OR '.join(f"(source_topic='{r['topic']}' AND source_partition={r['partition']} AND source_offset>={r['start']} AND source_offset<{r['end_exclusive']})" for r in report['ranges'])
    actual = rows('SELECT DISTINCT source_topic,source_partition,source_offset FROM risk.evaluations WHERE ' + conditions)
    coordinates = sorted(f"{r['source_topic']}:{r['source_partition']}:{r['source_offset']}" for r in actual)
    assert len(coordinates) == report['records'], (len(coordinates), report)
    assert hashlib.sha256('\n'.join(coordinates).encode()).hexdigest() == report['source_positions_sha256'], report
    return report


# The first replay also waits for all committed decisions to reach the analytical store.
first = replay(True)
baseline = counts()
second = replay(True)
assert first['ranges'] == second['ranges'], 'Run verification while synthetic generation is stopped'
assert counts() == baseline and int(baseline['logical']) > 0, (baseline, counts())
# The default mode must leave physical deliveries untouched.
physical = rows('SELECT count() AS n FROM risk.evaluations')[0]
dry = replay(False)
assert rows('SELECT count() AS n FROM risk.evaluations')[0] == physical

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
    stored = rows("SELECT event_id, evaluation_id, transaction_id, risk_score, decision, matched_rules, amount_minor "
                  "FROM risk.decisions_current WHERE startsWith(customer_id, '" + run_id + "-')")
    actual = {row['event_id']: row for row in stored if '-marker-' not in row['event_id']}
    assert len(actual) == len(stored) - sum('-marker-' in row['event_id'] for row in stored)
    assert actual.keys() == expected.keys(), 'Customer simulation coverage differs in ClickHouse'
    for event_id, row in actual.items():
        for field in ('evaluation_id', 'transaction_id', 'risk_score', 'decision', 'matched_rules'):
            assert row[field] == expected[event_id][field], (event_id, field)
        assert int(row['amount_minor']) == expected[event_id]['amount_minor'], event_id
    simulation_check = {'run_id': run_id, 'unique_payments': len(actual),
                        'amount_minor': sum(int(row['amount_minor']) for row in actual.values())}

name = 'risk_verify_' + uuid.uuid4().hex
assert name.startswith('risk_verify_') and name.isidentifier()
try:
    ddl = (ROOT / 'infrastructure/docker/clickhouse.sql').read_text().replace('risk.', name + '.').replace('DATABASE IF NOT EXISTS risk;', 'DATABASE IF NOT EXISTS ' + name + ';')
    for statement in ddl.split(';'):
        if statement.strip():
            sql(statement)
    base = dict(decision_id='risk_tx1',event_id='event1',transaction_id='tx1',customer_id='c',amount_minor=100,currency='EUR',risk_score=0,
                decision='APPROVE',matched_rules=[],reason_codes=[],rules_fingerprint='rules',event_time=1000,processed_at=2000,
                source_partition=0,source_offset=99,source_topic='synthetic',evaluation_id='eval1',engine_version='v1',policy_id='p',policy_version=1,
                policy_snapshot='{}',rule_evidence='[]',input_sha256='input1')
    def insert(*values):
        sql('INSERT INTO ' + name + '.evaluations FORMAT JSONEachRow\n' + '\n'.join(json.dumps(v) for v in values))
    def count(view):
        return int(rows('SELECT count() AS n FROM ' + name + '.' + view)[0]['n'])
    insert(base, base, dict(base, source_partition=3, source_offset=1, processed_at=9000))
    assert count('evaluations') == 3 and count('decisions_current') == 1
    assert int(rows('SELECT sum(amount_minor) AS n FROM ' + name + '.decisions_current')[0]['n']) == 100
    insert(dict(base, risk_score=80, decision='REJECT'))
    assert count('integrity_conflicts') == 1 and count('decisions_current') == 0
    insert(dict(base, evaluation_id='eval2', event_id='event2', transaction_id='tx2'))
    assert count('decisions_current') == 1
    insert(dict(base, evaluation_id='eval3', event_id='event3', transaction_id='tx2'))
    assert count('integrity_conflicts') == 2 and count('decisions_current') == 0
    # A corrupt identity pointing at another transaction must invalidate both transactions.
    insert(dict(base, transaction_id='tx3'))
    assert count('integrity_conflicts') == 3 and count('decisions_current') == 0
finally:
    sql('DROP DATABASE IF EXISTS ' + name)

report = {'result': 'PASS', 'baseline': baseline, 'first_replay': first, 'second_replay': second, 'dry_run': dry,
          'customer_simulation': simulation_check,
          'checks': ['exact source-coordinate reconciliation', 'stable totals after replay', 'dry run has no inserts',
                     'retry across partitions', 'conflicting results', 'ambiguous transaction identity']}
(ROOT / 'artifacts').mkdir(exist_ok=True)
(ROOT / 'artifacts/materializer-replay.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report))
