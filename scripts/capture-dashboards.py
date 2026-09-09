"""Capture the real provisioned UI; requires Playwright and its Chromium browser."""
import os
from pathlib import Path
from playwright.sync_api import sync_playwright

root = Path(__file__).resolve().parents[1]
env = dict(line.split('=', 1) for line in (root / '.env').read_text().splitlines() if line and not line.startswith('#'))
out = root / 'docs/screenshots'
out.mkdir(exist_ok=True)
with sync_playwright() as p:
    options = {'headless': True}
    if os.environ.get('CHROMIUM_PATH'):
        options['executable_path'] = os.environ['CHROMIUM_PATH']
    browser = p.chromium.launch(**options)
    page = browser.new_page(viewport={'width': 1600, 'height': 1000}, device_scale_factor=1)
    page.goto('http://localhost:23000/login')
    page.locator('input[name="user"]').fill('admin')
    page.locator('input[name="password"]').fill(env['GRAFANA_ADMIN_PASSWORD'])
    page.locator('button[type="submit"]').click()
    page.wait_for_url(lambda url: '/login' not in url)
    for uid in ('risk-operations', 'risk-business'):
        page.goto('http://localhost:23000/d/' + uid + '?orgId=1&from=now-30m&to=now&kiosk')
        page.wait_for_function('''() => {
            const canvases = [...document.querySelectorAll('canvas')];
            return canvases.length > 0 && canvases.every(e => e.width && e.height &&
                e.getContext('2d').getImageData(0, 0, e.width, e.height).data.some(v => v > 0));
        }''', timeout=60000)
        page.screenshot(path=str(out / (uid + '.png')))
    page.goto('http://localhost:28082/#/job/running')
    page.wait_for_timeout(4000)
    page.screenshot(path=str(out / 'flink-running.png'))
    browser.close()
print('Captured Grafana dashboards and Flink UI in docs/screenshots')
