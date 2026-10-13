#!/usr/bin/env python3
"""Isolated storage benchmark. Input samples must come from a real Flink decision topic."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
import re
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ['docker', 'compose', '--env-file', str(ROOT / '.env'), '-f',
           str(ROOT / 'docker-compose.clickhouse-benchmark.yml')]


def environment():
    values = {}
    for line in (ROOT / '.env').read_text().splitlines():
        if line.strip() and not line.lstrip().startswith('#') and '=' in line:
            key, value = line.split('=', 1)
            values[key] = value.strip().strip('\"\'')
    return values


def request(url, body=None, method=None, headers=None):
    req = urllib.request.Request(url, data=body, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=120) as response:
            return response.read().decode()
    except urllib.error.HTTPError as error:
        raise RuntimeError(error.read().decode()[:4000]) from error


def sql(query, query_id=None):
    url = 'http://localhost:38123/'
    if query_id:
        url += '?query_id=' + query_id
    return request(url, query.encode(), headers={
        'X-ClickHouse-User': 'risk', 'X-ClickHouse-Key': environment()['CLICKHOUSE_PASSWORD']})


def api(path, data=None, method=None):
    result = request('http://localhost:38083/' + path,
                     None if data is None else json.dumps(data).encode(), method,
                     {'Content-Type': 'application/json'})
    return json.loads(result) if result else None


def compose(*args, **kwargs):
    return subprocess.run(COMPOSE + list(args), cwd=ROOT, text=True, check=True, **kwargs)


def resources(services=('connect', 'keeper', 'benchmark-clickhouse', 'benchmark-postgres')):
    result = {}
    for service in services:
        container = compose('ps', '-q', service, capture_output=True).stdout.strip()
        inspection = json.loads(subprocess.check_output(['docker', 'inspect', container], text=True))[0]
        result[service] = {'image': inspection['Config']['Image'],
                           'cpus': inspection['HostConfig']['NanoCpus']/1e9,
                           'memory_bytes': inspection['HostConfig']['Memory']}
    return result


def produce(source, topic, namespace, count, rate=0, variant='initial'):
    log = ROOT / 'artifacts' / f'{namespace}-{variant}-producer.log'
    with log.open('w') as stream:
        compose('run', '--rm', '--no-deps', 'producer', source, topic, namespace,
                str(count), str(rate), variant, stdout=stream, stderr=subprocess.STDOUT)
    report = None
    with log.open() as stream:
        for line in stream:
            if line.startswith('{') and 'achieved_records_per_second' in line:
                report = json.loads(line)
    if report is None:
        raise RuntimeError(f'Producer returned no report; see {log}')
    return report


def postgres(query):
    return compose('exec', '-T', 'benchmark-postgres', 'psql', '-U', 'risk', '-d', 'risk',
                   '-At', '-v', 'ON_ERROR_STOP=1', '-c', query, capture_output=True).stdout.strip()


def postgres_case(topic, expected):
    env = dict(os.environ, BENCHMARK_TOPIC=topic, BENCHMARK_GROUP=topic + '-pg')
    started = time.monotonic()
    container = compose('run', '-d', '--no-deps', 'postgres-materializer',
                        env=env, capture_output=True).stdout.strip()
    progress = []
    try:
        while time.monotonic() - started < 900:
            current = int(postgres(f"SELECT coalesce((SELECT assessments FROM risk.topic_totals WHERE source_topic='{topic}'),0)"))
            progress.append([round(time.monotonic()-started, 3), current])
            if current >= expected['records']:
                break
            time.sleep(2)
        else:
            raise TimeoutError(f'Postgres: {progress[-1]}')
        elapsed = time.monotonic()-started
        result = json.loads(postgres(f"""SELECT json_build_object('rows',count(*),
            'amount_minor',sum((payload->>'amount_minor')::bigint),
            'risk_score_sum',sum((payload->>'risk_score')::bigint),
            'insert_span_ms',1000*extract(epoch FROM max(stored_at)-min(stored_at)))
            FROM risk.assessments WHERE source_topic='{topic}'"""))
        for key in ['amount_minor', 'risk_score_sum']:
            assert int(result[key]) == expected[key], result
        assert result['rows'] == expected['records'], result
        assert int(postgres(f"SELECT count(*) FROM risk.incidents WHERE source_topic='{topic}'")) == 0
        return dict(result, case='postgres', seconds_including_start=elapsed,
                    records_per_second_including_start=expected['records']/elapsed, progress=progress)
    finally:
        subprocess.run(['docker','stop',container], check=True, capture_output=True)


def query_comparison(name):
    results = []
    for source, amount, payments in [(name, 'amount_minor', 'count()'),
                                     (name+'_totals', 'amount_minor', 'sum(payments)')]:
        query_id = source + '_read'
        sql(f'SELECT decision, {payments} payments, sum({amount}) amount FROM benchmark.{source} GROUP BY decision', query_id)
        sql('SYSTEM FLUSH LOGS')
        measured = json.loads(sql(f"SELECT read_rows, read_bytes, memory_usage, query_duration_ms FROM system.query_log WHERE query_id='{query_id}' AND type='QueryFinish' ORDER BY event_time_microseconds DESC LIMIT 1 FORMAT JSONEachRow"))
        results.append(dict(measured, table=source))
    return results


def duplicate_case(source, run, expected):
    """New-offset retries are distinct from replay of already committed Kafka offsets."""
    topic = 'bench.' + run
    name = run + '_mv3'
    connector(name, topic, 3)
    env = dict(os.environ, BENCHMARK_TOPIC=topic, BENCHMARK_GROUP=topic + '-pg')
    container = compose('run', '-d', '--no-deps', 'postgres-materializer',
                        env=env, capture_output=True).stdout.strip()
    reports = []
    try:
        for variant, records in [('retry', 1000), ('new-event', 1000), ('conflict', 10)]:
            reports.append(produce(source, topic, run, records, variant=variant))
        totals = {key: expected[key] + sum(r[key] for r in reports)
                  for key in ['records','amount_minor','risk_score_sum']}
        totals['transactions'] = expected['records']
        wait_for(name, totals['records'])
        deadline = time.monotonic()+120
        while int(postgres(f"SELECT deliveries FROM risk.topic_totals WHERE source_topic='{topic}'")) < totals['records']:
            if time.monotonic()>deadline:
                raise TimeoutError('Postgres duplicate verification timed out')
            time.sleep(1)
        result = verify(name, totals)
        pg = json.loads(postgres(f"""SELECT json_build_object(
            'assessments',(SELECT assessments FROM risk.topic_totals WHERE source_topic='{topic}'),
            'deliveries',(SELECT deliveries FROM risk.topic_totals WHERE source_topic='{topic}'),
            'incidents',(SELECT count(*) FROM risk.incidents WHERE source_topic='{topic}'))"""))
        assert pg['assessments'] == expected['records'], pg
        assert pg['incidents'] == 10, pg
        assert int(result['transactions']) == expected['records'], result
        return dict(case='business_duplicates', clickhouse=result, postgres=pg, workload=reports)
    finally:
        subprocess.run(['docker','stop',container], check=True, capture_output=True)
        api('connectors/' + name, method='DELETE')


def columns(schema_name='risk-decision'):
    schema = json.loads((ROOT / ('schemas/' + schema_name + '.avsc')).read_text())
    def datatype(value):
        if isinstance(value, str):
            return {'string': 'String', 'long': 'Int64', 'int': 'Int32', 'boolean': 'Bool'}[value]
        if value['type'] == 'array':
            return 'Array(' + datatype(value['items']) + ')'
        return 'Tuple(' + ','.join(f["name"] + ' ' + datatype(f['type']) for f in value['fields']) + ')'
    return ',\n'.join(field['name'] + ' ' + datatype(field['type']) for field in schema['fields'])


def tables(name, aggregate=True):
    sql(f"""CREATE TABLE benchmark.{name} (
        {columns()}, source_topic String, source_partition Int32, source_offset Int64,
        stored_at DateTime64(3) DEFAULT now64(3))
        ENGINE=ReplicatedMergeTree('/benchmark/tables/{name}', 'one')
        ORDER BY (transaction_id, source_partition, source_offset)
        SETTINGS replicated_deduplication_window=10000,
                 replicated_deduplication_window_seconds=3600""")
    if aggregate:
        sql(f"""CREATE TABLE benchmark.{name}_totals (
            bucket DateTime, decision LowCardinality(String), currency LowCardinality(String),
            payments UInt64, amount_minor Int64, risk_score_sum Int64)
            ENGINE=SummingMergeTree ORDER BY (bucket, decision, currency)""")
        sql(f"""CREATE MATERIALIZED VIEW benchmark.{name}_mv TO benchmark.{name}_totals AS
            SELECT toStartOfInterval(fromUnixTimestamp64Milli(event_time), INTERVAL 10 SECOND) bucket,
            decision, currency, count() payments, sum(amount_minor) amount_minor,
            sum(toInt64(risk_score)) risk_score_sum FROM benchmark.{name}
            GROUP BY bucket, decision, currency""")


def connector(name, topic, tasks=1, coordinate_prefix='source_'):
    config = {
        'connector.class': 'com.clickhouse.kafka.connect.ClickHouseSinkConnector',
        'tasks.max': str(tasks), 'topics': topic, 'hostname': 'benchmark-clickhouse',
        'port': '8123', 'ssl': 'false', 'database': 'benchmark', 'username': 'risk',
        'password': environment()['CLICKHOUSE_PASSWORD'], 'exactlyOnce': 'true',
        'zkDatabase': 'state_' + name, 'zkPath': '/benchmark/state/' + name,
        # Kafka commits can lag multiple successful inserts at a crash. Keeper's
        # AFTER_PROCESSING state identifies these older batches as already stored.
        # Never reuse this state namespace with a cleared/restored target table.
        'topic2TableMap': topic + '=' + name, 'tolerateStateMismatch': 'true',
        'clickhouseSettings': 'insert_deduplicate=1,deduplicate_blocks_in_dependent_materialized_views=1',
        'consumer.override.auto.offset.reset': 'earliest',
        'consumer.override.isolation.level': 'read_committed',
        'consumer.override.max.poll.records': '1000',
        'errors.tolerance': 'none', 'errors.retry.timeout': '60000',
        'transforms': 'source', 'transforms.source.type': 'org.apache.kafka.connect.transforms.InsertField$Value',
        'transforms.source.topic.field': coordinate_prefix + 'topic',
        'transforms.source.partition.field': coordinate_prefix + 'partition',
        'transforms.source.offset.field': coordinate_prefix + 'offset'}
    api('connectors', {'name': name, 'config': config}, 'POST')


def count(name):
    # ClickHouse can answer unfiltered count from active part metadata.
    return int(sql(f'SELECT count() FROM benchmark.{name}'))


def wait_for(name, expected, timeout=600):
    started = time.monotonic()
    progress = []
    while time.monotonic() - started < timeout:
        current = count(name)
        progress.append([round(time.monotonic() - started, 3), current])
        try:
            status = api('connectors/' + name + '/status')
        except RuntimeError as error:
            if '"error_code":404' not in str(error):
                raise
            status = {}
        for task in status.get('tasks', []):
            if task['state'] == 'FAILED':
                raise RuntimeError(task.get('trace', str(task)))
        if current >= expected:
            return progress
        time.sleep(1)
    raise TimeoutError(f'{name}: {progress[-1]} expected {expected}')


def verify(name, expected, aggregate=True):
    result = json.loads(sql(f"""SELECT count() rows,
        uniqExact((source_topic,source_partition,source_offset)) coordinates,
        uniqExact(transaction_id) transactions, sum(amount_minor) amount_minor,
        sum(toInt64(risk_score)) risk_score_sum,
        dateDiff('millisecond', min(stored_at), max(stored_at)) insert_span_ms,
        quantileExact(0.95)(toUnixTimestamp64Milli(stored_at)-processed_at) publish_to_insert_p95_ms
        FROM benchmark.{name} FORMAT JSONEachRow"""))
    for key in ['amount_minor', 'risk_score_sum']:
        assert int(result[key]) == expected[key], (key, result, expected)
    assert int(result['rows']) == int(result['coordinates']) == expected['records'], result
    assert int(result['transactions']) == expected.get('transactions', expected['records']), result
    partitions = [json.loads(line) for line in sql(f"""SELECT source_partition partition,
        count() rows, min(source_offset) first_offset, max(source_offset) last_offset
        FROM benchmark.{name} GROUP BY source_partition ORDER BY source_partition FORMAT JSONEachRow""").splitlines()]
    for part in partitions:
        assert int(part['first_offset']) == 0, part
        assert int(part['last_offset']) + 1 == int(part['rows']), part
        if 'records_by_partition' in expected:
            assert int(part['rows']) == expected['records_by_partition'][part['partition']], part
    result['partitions'] = partitions
    if aggregate:
        totals = json.loads(sql(f"""SELECT sum(payments) payments, sum(amount_minor) amount_minor,
            sum(risk_score_sum) risk_score_sum FROM benchmark.{name}_totals FORMAT JSONEachRow"""))
        assert int(totals['payments']) == expected['records'], totals
        assert int(totals['amount_minor']) == expected['amount_minor'], totals
        assert int(totals['risk_score_sum']) == expected['risk_score_sum'], totals
        result['incremental_totals'] = totals
    return result


def live_case(source, run, rate, seconds):
    name = run + '_live' + str(rate)
    live_topic = 'bench.' + name
    # Bootstrap creates topics and schemas. The single row has its own identity.
    seed = produce(source, live_topic, name+'_seed', 1)
    tables(name)
    connector(name, live_topic, 3)
    wait_for(name, 1)
    with ThreadPoolExecutor(max_workers=1) as pool:
        producer = pool.submit(produce, source, live_topic, name,
                               rate*seconds, rate)
        started = time.monotonic()
        progress = []
        while not producer.done():
            progress.append([int(time.time()*1000), count(name)])
            time.sleep(1)
        workload = producer.result()
    live_expected = {key: workload[key]+seed[key] for key in ['records','amount_minor','risk_score_sum']}
    drain = wait_for(name, live_expected['records'])
    result = verify(name, live_expected)
    # Exclude the bootstrap row from latency; it predates the offered-rate interval.
    result['publish_to_insert_p95_ms'] = int(sql(f"SELECT quantileExact(0.95)(toUnixTimestamp64Milli(stored_at)-processed_at) FROM benchmark.{name} WHERE NOT startsWith(transaction_id,'{name}_seed')"))
    api('connectors/' + name, method='DELETE')
    acknowledgements = []
    with (ROOT / 'artifacts' / f'{name}-initial-producer.log').open() as stream:
        for line in stream:
            if line.startswith('{') and 'acknowledged' in line:
                acknowledgements.append(json.loads(line))
    return dict(result, case='live'+str(rate), workload=workload, progress=progress,
                acknowledgements=acknowledgements, post_producer_drain_seconds=drain[-1][0],
                backlog=backlog_summary(progress, acknowledgements))


def backlog_summary(progress, acknowledgements):
    """Conservative backlog: an earlier acknowledged count minus a later stored count.

    Polling misses sub-second peaks. The seed row is excluded from stored counts.
    No scheduled/offered records are treated as acknowledged records.
    """
    bounds = []
    index = 0
    for observed_at, stored in progress:
        while index < len(acknowledgements) and acknowledgements[index]['at_ms'] <= observed_at:
            index += 1
        if index:
            bounds.append(max(0, acknowledgements[index-1]['acknowledged'] - (stored-1)))
    return {'maximum_observed_lower_bound': max(bounds, default=0),
            'last_observed_lower_bound': bounds[-1] if bounds else 0}


def recovery_case(run, topic, expected):
    name = run + '_recovery'
    tables(name)
    connector(name, topic, 3)
    deadline = time.monotonic()+120
    while count(name) < min(10000, expected['records']//4):
        if time.monotonic()>deadline:
            raise TimeoutError('Recovery test never reached fault threshold')
        time.sleep(.25)
    before = count(name)
    if before >= expected['records']:
        api('connectors/' + name, method='DELETE')
        return dict(case='recovery', passed=False,
                    failure='Workload drained before fault injection; repeat with more records')
    compose('kill', '-s', 'SIGKILL', 'connect', capture_output=True)
    compose('up', '-d', 'connect', capture_output=True)
    deadline = time.monotonic()+120
    while True:
        try:
            api('connectors')
            break
        except (OSError, RuntimeError):
            if time.monotonic()>deadline:
                raise
            time.sleep(1)
    try:
        progress = wait_for(name, expected['records'])
        result = dict(verify(name, expected), case='recovery', passed=True,
                      rows_before_kill=before, progress=progress)
    except (RuntimeError, TimeoutError) as error:
        result = dict(case='recovery', passed=False, rows_before_kill=before,
                      rows_after_restart=count(name), expected_records=expected['records'],
                      failure=str(error), status=api('connectors/' + name + '/status'))
    api('connectors/' + name, method='DELETE')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-topic', help='Defaults to the latest completed showcase decision topic')
    parser.add_argument('--records', type=int, default=100000)
    parser.add_argument('--run', default='sink_' + str(int(time.time())))
    parser.add_argument('--smoke', action='store_true')
    parser.add_argument('--postgres', action='store_true', help='Also run the existing acceptance materializer')
    parser.add_argument('--rates', default='', help='Comma-separated offered rates for 60-second live tests')
    parser.add_argument('--seconds', type=int, default=60)
    parser.add_argument('--recovery', action='store_true', help='Kill Connect during a separate backlog drain')
    parser.add_argument('--duplicates', action='store_true', help='Compare new-offset retries and conflicting assessments; requires --postgres')
    args = parser.parse_args()
    if not args.source_topic:
        manifest = ROOT / 'artifacts/showcase/latest.json'
        if not manifest.exists():
            parser.error('Run the showcase first or provide --source-topic')
        showcase = json.loads(manifest.read_text())
        if showcase.get('status') != 'COMPLETE':
            parser.error('The latest showcase is incomplete; provide an existing --source-topic explicitly')
        args.source_topic = showcase['decision_topic']
    if args.duplicates and (not args.postgres or args.smoke or args.records < 1000):
        parser.error('--duplicates requires --postgres, at least 1000 records, and the full matrix')
    if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,39}', args.run):
        parser.error('run must be at most 40 alphanumeric/underscore characters')
    if not 10 <= args.seconds <= 300 or not 1 <= args.records <= 2_000_000:
        parser.error('seconds must be 10..300 and records 1..2000000')
    if args.recovery and args.records < 10000:
        parser.error('--recovery requires at least 10000 records')
    rates = [int(value) for value in args.rates.split(',') if value]
    if any(not 1 <= rate <= 100000 or rate*args.seconds > 2_000_000 for rate in rates):
        parser.error('each offered rate must generate 1..2000000 records')
    output = ROOT / 'artifacts' / args.run
    output.mkdir(exist_ok=False)
    topic = 'bench.' + args.run
    expected = produce(args.source_topic, topic, args.run, args.records)
    results = {'workload': expected, 'resources': resources(), 'cases': []}
    (output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    def save(result):
        results['cases'].append(result)
        (output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
        print(json.dumps({k: v for k, v in result.items() if k != 'progress'}), flush=True)
    if args.postgres:
        save(postgres_case(topic, expected))
    cases = [('mv1', 1, True)] if args.smoke else [('raw1', 1, False), ('mv1', 1, True), ('mv3', 3, True)]
    for suffix, tasks, aggregate in cases:
        name = args.run + '_' + suffix
        tables(name, aggregate)
        started = time.monotonic()
        connector(name, topic, tasks)
        progress = wait_for(name, args.records)
        elapsed = time.monotonic() - started
        result = verify(name, expected, aggregate)
        result.update(case=suffix, seconds_including_start=elapsed,
                      records_per_second_including_start=args.records/elapsed, progress=progress)
        if aggregate:
            result['read_queries'] = query_comparison(name)
        save(result)
        api('connectors/' + name, method='DELETE')

    if args.recovery:
        save(recovery_case(args.run, topic, expected))
    for rate in rates:
        save(live_case(args.source_topic, args.run, rate, args.seconds))
    if args.duplicates:
        save(duplicate_case(args.source_topic, args.run, expected))
    if any(case.get('passed') is False for case in results['cases']):
        raise SystemExit('Recovery qualification failed; see the saved results. No offsets were advanced manually.')


if __name__ == '__main__':
    main()
