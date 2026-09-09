"""Measure a bounded local workload and persist its environment and reconciliation evidence."""
import argparse
import json
import os
import platform
import subprocess
import urllib.parse
import urllib.request
from pathlib import Path

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument('--count', type=int, default=1000)
parser.add_argument('--rate', type=int, default=100)
args = parser.parse_args()

def get(url):
    with urllib.request.urlopen(url, timeout=15) as response:
        return json.load(response)

job = next(j for j in get('http://localhost:28082/jobs/overview')['jobs'] if j['state'] == 'RUNNING' and j['name'] == 'payment-risk-v1')
checkpoints_url = 'http://localhost:28082/jobs/' + job['jid'] + '/checkpoints'
before = get(checkpoints_url)
result = subprocess.run(['java', '-cp', 'target/risk-engine.jar', 'com.portfolio.paymentrisk.tools.BenchmarkScenario', str(args.count), str(args.rate)], cwd=root, check=True, stdout=subprocess.PIPE, text=True, timeout=300 + args.count // max(1, args.rate))
report = json.loads(result.stdout.strip().splitlines()[-1])
report['environment'] = {'os': platform.platform(), 'logical_cpus': os.cpu_count(),
    'memory_bytes': os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES'),
    'java': subprocess.check_output(['java', '-version'], stderr=subprocess.STDOUT, text=True).strip(),
    'image': subprocess.check_output(['docker', 'image', 'inspect', 'payment-risk:0.1.0', '--format', '{{.Id}}'], text=True).strip(),
    'job_id': job['jid'], 'parallelism': 2, 'payment_partitions': 3,
    'backend': 'rocksdb', 'disorder_ms': 10000, 'checkpoint_interval_ms': 10000,
    'command': f'python3 scripts/benchmark.py --count {args.count} --rate {args.rate}',
    'qualification': 'shared WSL host; other project services running; bounded correctness and latency measurement'}
after = get(checkpoints_url)
report['checkpoints'] = {'before': before['counts'], 'after': after['counts'], 'summary': after['summary'], 'latest': after['latest']['completed']}
report['metrics'] = {}
for name, expr in {'evaluation_p95_us': 'flink_taskmanager_job_task_operator_risk_evaluation_duration_us{quantile="0.95"}',
        'max_partition_lag': 'max(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max)',
        'backpressure_ms_per_second': 'max(flink_taskmanager_job_task_backPressuredTimeMsPerSecond)'}.items():
    report['metrics'][name] = get('http://localhost:29090/api/v1/query?' + urllib.parse.urlencode({'query': expr}))['data']['result']
(root / 'artifacts').mkdir(exist_ok=True)
(root / 'artifacts/benchmark.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report))
