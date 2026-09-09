"""Local deployment and recovery controls, scoped to this Compose project."""
import argparse
import json
import subprocess
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
URL = 'http://localhost:28082'

def docker(*args, **kwargs):
    return subprocess.run(['docker', 'compose', *args], cwd=ROOT, check=True, **kwargs)

def rest(path, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(URL + path, data=data, headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.load(response)

def job():
    jobs = [j for j in rest('/jobs/overview')['jobs'] if j['name'] == 'payment-risk-v1' and j['state'] not in ('FINISHED', 'CANCELED', 'FAILED')]
    if len(jobs) != 1:
        raise RuntimeError(f'Expected one active payment-risk job, found {len(jobs)}')
    return jobs[0]['jid']

def ensure_no_active_job():
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        try:
            jobs = rest('/jobs/overview')['jobs']
        except OSError:
            time.sleep(2)
            continue
        if any(j['name'] == 'payment-risk-v1' and j['state'] not in ('FINISHED', 'CANCELED', 'FAILED') for j in jobs):
            raise RuntimeError('A payment-risk job is already active; stop it with a savepoint before another submission')
        return
    raise TimeoutError('Flink REST endpoint did not become ready')

def wait_running():
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        try:
            jid = job()
            status = rest('/jobs/' + jid)
            if status['state'] == 'RUNNING' and all(v['status'] == 'RUNNING' for v in status['vertices']):
                return jid
        except (OSError, RuntimeError):
            pass
        time.sleep(2)
    raise TimeoutError('Flink job did not reach RUNNING')

def checkpoint(jid, after=-1):
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        status = rest(f'/jobs/{jid}/checkpoints')
        if (status.get('latest', {}).get('completed') or {}).get('id', -1) > after:
            return status['latest']['completed']
        time.sleep(2)
    raise TimeoutError('No completed checkpoint')

def recovery_evidence(jid, before, started, require_restore=True, filename='recovery.json'):
    wait_running()
    after = checkpoint(jid, before['id'])
    status = rest(f'/jobs/{jid}/checkpoints')
    restored = status.get('latest', {}).get('restored')
    if require_restore and (not restored or restored.get('restore_timestamp', 0) < started):
        raise AssertionError('Worker restart did not restore checkpointed state')
    report = {'job_id': jid, 'checkpoint_before': before['id'], 'checkpoint_after': after['id'],
              'restored': restored, 'recovery_and_checkpoint_ms': int(time.time() * 1000) - started}
    (ROOT / 'artifacts').mkdir(exist_ok=True)
    (ROOT / ('artifacts/' + filename)).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report))

def savepoint(stop=False):
    jid = job()
    endpoint = 'stop' if stop else 'savepoints'
    payload = {'targetDirectory': 's3://payment-risk/savepoints'}
    if stop:
        payload = {'targetDirectory': 's3://payment-risk/savepoints', 'drain': False}
    request = rest(f'/jobs/{jid}/{endpoint}', payload)['request-id']
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        result = rest(f'/jobs/{jid}/savepoints/{request}')
        if result['status']['id'] == 'COMPLETED':
            if 'failure-cause' in result.get('operation', {}):
                raise RuntimeError(result)
            path = result['operation']['location']
            (ROOT / 'artifacts').mkdir(exist_ok=True)
            (ROOT / 'artifacts/last-savepoint.txt').write_text(path + '\n')
            print(path)
            return path
        time.sleep(2)
    raise TimeoutError('Savepoint did not complete')

def cli(*args):
    docker('exec', '-T', 'jobmanager', 'java', '-cp', '/opt/flink/usrlib/risk-engine.jar', 'com.portfolio.paymentrisk.tools.PlatformCli', *args)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['submit', 'savepoint', 'stop', 'restore', 'recover', 'integration', 'recovery-test', 'idle-resume-test', 'kafka-interruption'])
    args = parser.parse_args()
    if args.action in ('submit', 'restore'):
        ensure_no_active_job()
        options = []
        if args.action == 'restore':
            options = ['-s', (ROOT / 'artifacts/last-savepoint.txt').read_text().strip()]
        docker('exec', '-T', 'jobmanager', 'flink', 'run', '-d', *options, '/opt/flink/usrlib/risk-engine.jar')
        jid = wait_running()
        print('Running job:', jid)
        if args.action == 'restore':
            restored = rest(f'/jobs/{jid}/checkpoints')['latest']['restored']
            if not restored or not restored['is_savepoint'] or restored['external_path'] != options[1]:
                raise AssertionError('Job did not restore the requested savepoint')
            (ROOT / 'artifacts/savepoint-restore.json').write_text(json.dumps({'job_id': jid, 'restored': restored}, indent=2) + '\n')
    elif args.action in ('savepoint', 'stop'):
        savepoint(args.action == 'stop')
    elif args.action in ('recover', 'kafka-interruption'):
        jid = wait_running(); before = checkpoint(jid)
        started = int(time.time() * 1000)
        service = 'kafka' if args.action == 'kafka-interruption' else 'taskmanager'
        docker('kill', '-s', 'SIGKILL', service)
        docker('up', '-d', '--no-deps', service)
        recovery_evidence(jid, before, started, service == 'taskmanager', args.action + '.json')
    else:
        jid = wait_running(); before = checkpoint(jid)
        if args.action == 'idle-resume-test':
            print('Waiting 75 seconds for all payment inputs to become idle before resuming.', flush=True)
            for _ in range(75):
                time.sleep(1)
        command = ['docker', 'compose', 'exec', '-T', 'jobmanager', 'java', '-cp', '/opt/flink/usrlib/risk-engine.jar', 'com.portfolio.paymentrisk.tools.IntegrationScenario']
        process = subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE, text=True)
        if args.action == 'recovery-test':
            time.sleep(3)
            started = int(time.time() * 1000)
            docker('kill', '-s', 'SIGKILL', 'taskmanager')
            docker('up', '-d', '--no-deps', 'taskmanager')
        output, _ = process.communicate(timeout=240)
        print(output, end='')
        (ROOT / 'artifacts').mkdir(exist_ok=True)
        (ROOT / ('artifacts/' + args.action + '.jsonl')).write_text(output)
        if process.returncode != 0:
            raise RuntimeError('Integration assertions failed')
        if args.action == 'recovery-test':
            recovery_evidence(jid, before, started)
        if args.action == 'integration':
            result = docker('exec', '-T', 'jobmanager', 'java', '-cp', '/opt/flink/usrlib/risk-engine.jar', 'com.portfolio.paymentrisk.tools.RuleUpdateScenario', stdout=subprocess.PIPE, text=True)
            print(result.stdout, end='')
            (ROOT / 'artifacts/rule-update.jsonl').write_text(result.stdout)

if __name__ == '__main__':
    main()
