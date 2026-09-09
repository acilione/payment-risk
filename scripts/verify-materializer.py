"""Replay the committed decision log twice and assert stable ClickHouse business counts."""
import json
import os
import subprocess
import time
import urllib.request
import uuid
from pathlib import Path

root = Path(__file__).resolve().parents[1]
env = os.environ.copy()
env.update(dict(line.split('=', 1) for line in (root / '.env').read_text().splitlines() if line and not line.startswith('#')))

def counts():
    request = urllib.request.Request('http://localhost:28123', data=b'SELECT (SELECT count() FROM risk.decisions) AS physical, (SELECT count() FROM risk.decisions_current) AS logical FORMAT JSONEachRow',
        headers={'X-ClickHouse-User': env['CLICKHOUSE_USER'], 'X-ClickHouse-Key': env['CLICKHOUSE_PASSWORD']})
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.load(response)

def replay():
    env['MATERIALIZER_GROUP'] = 'materializer-replay-' + uuid.uuid4().hex
    process = subprocess.Popen(['java', '-cp', 'target/risk-engine.jar', 'com.portfolio.paymentrisk.tools.DecisionMaterializer'], cwd=root, env=env)
    try:
        for _ in range(20):
            if process.poll() is not None:
                raise RuntimeError('Materializer terminated before replay completed')
            time.sleep(1)
        return counts()
    finally:
        process.terminate()
        process.wait(timeout=15)

first = replay()
second = replay()
assert first['logical'] > 0, first
assert first['logical'] == second['logical'], (first, second)
report = {'result': 'PASS', 'first_replay': first, 'second_replay': second}
(root / 'artifacts').mkdir(exist_ok=True)
(root / 'artifacts/materializer-replay.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report))
