"""Prove raw input persistence during a ClickHouse outage, with Flink and registry unavailable."""
import json
import os
import subprocess
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
env = os.environ.copy()
env.update(dict(line.split('=', 1) for line in (ROOT / '.env').read_text().splitlines()
                if line and not line.startswith('#')))


def docker(*args):
    return subprocess.check_output(['docker', 'compose', *args], cwd=ROOT, text=True, timeout=180)


def scenario(*args):
    result = docker('exec', '-T', 'jobmanager', 'java', '-cp', '/opt/flink/usrlib/risk-engine.jar',
                    'com.portfolio.paymentrisk.tools.ArchiveScenario', *args)
    return json.loads([line for line in result.splitlines() if line.startswith('{')][-1])


def sql(query):
    request = urllib.request.Request('http://localhost:28123/', data=(query + ' FORMAT JSONEachRow').encode(),
        headers={'X-ClickHouse-User': env['CLICKHOUSE_USER'], 'X-ClickHouse-Key': env['CLICKHOUSE_PASSWORD']})
    with urllib.request.urlopen(request, timeout=10) as response:
        return [json.loads(line) for line in response.read().decode().splitlines()]


with urllib.request.urlopen('http://localhost:28082/jobs/overview', timeout=10) as response:
    jobs = json.load(response)['jobs']
assert not any(j['state'] not in ('FINISHED', 'CANCELED', 'FAILED') for j in jobs), 'Stop the Flink job before this test'
deadline = time.monotonic() + 120
while True:
    before = scenario('offsets')
    if all(p['committed'] == p['end'] for p in before.values()):
        break
    assert time.monotonic() < deadline, 'Archive did not catch up before fault test'
    time.sleep(2)
try:
    docker('stop', 'clickhouse')
    fixture = scenario()
    docker('stop', 'registry')
    time.sleep(15)
    during = scenario('offsets')
    assert during['0']['committed'] == before['0']['committed'], 'Offsets advanced while database was unavailable'
    docker('up', '-d', '--no-deps', 'clickhouse')
    expected = {int(row['source_offset']): row for row in fixture['expected']}
    offsets = ','.join(str(offset) for offset in expected)
    deadline = time.monotonic() + 180
    while True:
        try:
            stored = sql('SELECT * EXCEPT archived_at FROM risk.payment_ingress '
                         'WHERE source_partition=0 AND source_offset IN (' + offsets + ')')
            if {int(row['source_offset']) for row in stored} == expected.keys():
                break
        except OSError:
            pass
        assert time.monotonic() < deadline, 'Archive failed to persist every input after database recovery'
        time.sleep(2)
    for row in stored:
        source = expected[int(row['source_offset'])]
        for field, value in source.items():
            actual = row[field]
            if field in ('source_partition', 'source_offset', 'source_timestamp', 'payload_bytes'):
                actual = int(actual)
            assert actual == value, (field, row['source_offset'])
    deadline = time.monotonic() + 60
    while True:
        after = scenario('offsets')
        if after['0']['committed'] == after['0']['end']:
            break
        assert time.monotonic() < deadline, 'Archive writes were not followed by offset commit'
        time.sleep(2)
    report = {'result': 'PASS', 'records': len(expected), 'physical_rows': len(stored),
              'flink_stopped': True, 'registry_stopped_during_recovery': True,
              'before': before, 'during_outage': during, 'after': after,
              'checks': ['exact payload/key/header bytes', 'null distinct from empty', 'late and invalid input retained',
                         'transport retries retained as source deliveries', 'no offset advance during outage']}
    (ROOT / 'artifacts/ingress-archive.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report))
finally:
    docker('up', '-d', '--no-deps', 'registry', 'clickhouse')
