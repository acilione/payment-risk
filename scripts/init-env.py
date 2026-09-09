"""Create local secrets without printing them or replacing existing values."""
from pathlib import Path
import secrets

path = Path('.env')
values = dict(line.split('=', 1) for line in path.read_text().splitlines()
              if line and not line.startswith('#')) if path.exists() else {}
values.setdefault('MINIO_ROOT_USER', 'payment_risk')
values.setdefault('CLICKHOUSE_USER', 'risk')
for key in ('MINIO_ROOT_PASSWORD', 'CLICKHOUSE_PASSWORD', 'GRAFANA_ADMIN_PASSWORD', 'REGISTRY_DB_PASSWORD'):
    if not values.get(key):
        values[key] = secrets.token_urlsafe(32)
path.write_text(''.join(f'{key}={value}\n' for key, value in values.items()))
path.chmod(0o600)
print('Local .env ready; credentials were not printed.')
