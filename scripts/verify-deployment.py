"""Validate rendered Flink deployment against the pinned Apache operator CRD."""
import subprocess
import json
import tarfile
from pathlib import Path
import yaml
import jsonschema

root = Path(__file__).resolve().parents[1]
artifacts = root / 'artifacts'
artifacts.mkdir(exist_ok=True)
subprocess.run(['helm', 'pull', 'flink-kubernetes-operator', '--repo',
    'https://downloads.apache.org/flink/flink-kubernetes-operator-1.15.0/', '--version', '1.15.0', '--destination', str(artifacts)], check=True)
with tarfile.open(artifacts / 'flink-kubernetes-operator-1.15.0-helm.tgz') as chart:
    crd = next(yaml.safe_load(chart.extractfile(m)) for m in chart.getmembers() if '/crds/' in m.name and 'flinkdeployment' in m.name)
rendered = subprocess.check_output(['helm', 'template', 'payment-risk', str(root / 'infrastructure/helm/payment-risk'),
    '--set', 'image=ghcr.io/example/payment-risk@sha256:' + 'a' * 64,
    '--set', 'bootstrapServers=kafka.example:9093', '--set', 'schemaRegistryUrl=https://registry.example/apis/ccompat/v7',
    '--set', 'checkpointBucket=example-state', '--set', 'transactionalPrefix=payment-risk-prod',
    '--set', 'allowedEgressCidrs[0]=10.0.0.0/8'], text=True)
schema = next(v['schema']['openAPIV3Schema'] for v in crd['spec']['versions'] if v['name'] == 'v1beta1')
deployment = next(x for x in yaml.safe_load_all(rendered) if x and x['kind'] == 'FlinkDeployment')
jsonschema.Draft7Validator(schema).validate(deployment)
(artifacts / 'rendered.yaml').write_text(rendered)
(artifacts / 'deployment-validation.json').write_text(json.dumps({'operator': '1.15.0', 'result': 'PASS'}, indent=2) + '\n')
print('PASS: application chart validates against Flink Operator 1.15.0 CRD')
