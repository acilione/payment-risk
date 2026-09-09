"""Check provisioned dashboards after integration/load has established a payment watermark."""
import base64
import json
import urllib.parse
import urllib.request
from pathlib import Path

root = Path(__file__).resolve().parents[1]
env = dict(line.split('=', 1) for line in (root / '.env').read_text().splitlines() if line and not line.startswith('#'))
auth = base64.b64encode(('admin:' + env['GRAFANA_ADMIN_PASSWORD']).encode()).decode()
report = []
for path in sorted((root / 'dashboards').glob('*.json')):
    dashboard = json.loads(path.read_text())
    req = urllib.request.Request('http://localhost:23000/api/dashboards/uid/' + dashboard['uid'], headers={'Authorization': 'Basic ' + auth})
    with urllib.request.urlopen(req, timeout=15) as response:
        assert json.load(response)['dashboard']['uid'] == dashboard['uid']
    for panel in dashboard['panels']:
        for target in panel['targets']:
            url = 'http://localhost:29090/api/v1/query?' + urllib.parse.urlencode({'query': target['expr']})
            with urllib.request.urlopen(url, timeout=15) as response:
                result = json.load(response)
            assert result['status'] == 'success' and result['data']['result'], (panel['title'], result)
            report.append({'dashboard': dashboard['uid'], 'panel': panel['title'], 'series': len(result['data']['result'])})
(root / 'artifacts').mkdir(exist_ok=True)
(root / 'artifacts/observability.json').write_text(json.dumps(report, indent=2) + '\n')
print('PASS:', len(report), 'dashboard queries returned live series; both dashboards provisioned')
