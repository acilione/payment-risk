#!/usr/bin/env python3
"""Browser acceptance for the packaged live website or public static demo."""
import argparse
import json
from pathlib import Path
from playwright.sync_api import sync_playwright, expect

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--url', default='http://127.0.0.1:23001/')
parser.add_argument('--static', action='store_true')
args = parser.parse_args()
out = Path('artifacts/website')
out.mkdir(parents=True, exist_ok=True)
evidence = {'url': args.url, 'mode': 'static' if args.static else 'live', 'checks': []}

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={'width': 1512, 'height': 1100}, device_scale_factor=1, reduced_motion='reduce')
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.goto(args.url, wait_until='networkidle')
    if not args.static:
        response = page.request.get(args.url.rstrip('/') + '/api/overview?window=24h')
        assert response.ok, f'Live API returned {response.status}'
        payload = response.json()
        expect(page.locator('.stat-value').first).to_have_text(f"{payload['totals']['transactions']:,}")
        assert sum(payload['totals'][k] for k in ('approved', 'review', 'rejected')) == payload['totals']['transactions']
        evidence['live_transactions'] = payload['totals']['transactions']
        evidence['checks'].append('live totals and decision reconciliation')
        page.get_by_role('button', name='Explore demo').click()
    expect(page.get_by_text('Illustrative demo', exact=True)).to_be_visible()
    expect(page.locator('tbody tr')).to_have_count(6)
    page.screenshot(path=str(out / 'overview-desktop.png'), full_page=True)
    page.get_by_role('button', name='Transactions', exact=True).click()
    expect(page.locator('tbody tr')).to_have_count(48)
    page.get_by_label('Decision filter').select_option('REJECT')
    assert page.locator('tbody tr').count() > 0
    assert all(x == 'Rejected' for x in page.locator('tbody .badge').all_text_contents())
    page.get_by_label('Search transactions').fill('no-such-payment')
    expect(page.get_by_text('No matching decisions')).to_be_visible()
    page.get_by_role('button', name='Clear search').click()
    page.locator('.transaction-link').first.click()
    expect(page.get_by_role('dialog', name='A decision, explained.')).to_be_visible()
    expect(page.locator('.inspector .matched-rule')).to_have_count(3)
    page.screenshot(path=str(out / 'decision-inspector.png'))
    page.keyboard.press('Escape')
    expect(page.get_by_role('dialog', name='A decision, explained.')).not_to_be_visible()
    page.get_by_label('Decision filter').select_option('ALL')
    page.get_by_label('Time window').select_option('15m')
    expect(page.get_by_label('Time window')).to_have_value('15m')
    page.get_by_role('button', name='Risk rules', exact=False).first.click()
    expect(page.locator('.rule-detail')).to_have_count(5)
    page.get_by_role('button', name='Architecture', exact=True).click()
    expect(page.get_by_text('From event to evidence', exact=True)).to_be_visible()
    expect(page.locator('.architecture-node')).to_have_count(4)
    page.screenshot(path=str(out / 'architecture-desktop.png'), full_page=True)
    page.get_by_role('button', name='About this project').click()
    expect(page.get_by_role('dialog', name='About Pulse')).to_be_visible()
    page.keyboard.press('Escape')
    expect(page.get_by_role('dialog', name='About Pulse')).not_to_be_visible()
    evidence['checks'].append('navigation, search, filtering, policy inspection, modal keyboard behavior')
    page.get_by_role('button', name='Overview', exact=True).click()
    page.set_viewport_size({'width': 390, 'height': 844})
    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth'), 'Mobile horizontal overflow'
    page.screenshot(path=str(out / 'overview-mobile.png'), full_page=True)
    page.get_by_role('button', name='Toggle navigation').click()
    page.get_by_role('button', name='Architecture', exact=True).click()
    expect(page.get_by_text('From event to evidence', exact=True)).to_be_visible()
    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth'), 'Architecture mobile overflow'
    page.get_by_role('button', name='Toggle navigation').click()
    page.get_by_role('button', name='Risk rules', exact=False).first.click()
    expect(page.get_by_text('Baseline policy catalog', exact=True)).to_be_visible()
    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth'), 'Rule catalog mobile overflow'
    evidence['checks'].append('390px mobile layout and navigation')
    if not args.static:
        page.set_viewport_size({'width': 1512, 'height': 1100})
        page.get_by_role('button', name='Overview', exact=True).click()
        page.route('**/api/overview?*', lambda route: route.fulfill(status=503, content_type='application/json', body='{}'))
        page.get_by_role('button', name='Connect live', exact=True).click()
        expect(page.get_by_role('alert')).to_contain_text('Live connection interrupted')
        expect(page.locator('.demo-banner')).to_have_count(0)
        expect(page.locator('.stat-value').first).to_have_text('—')
        evidence['checks'].append('upstream outage never fabricates demo data')
    assert not errors, errors
    evidence['checks'].append('no browser JavaScript errors')
    browser.close()

(out / 'acceptance.json').write_text(json.dumps(evidence, indent=2) + '\n')
print(json.dumps(evidence, indent=2))
