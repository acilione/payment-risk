"""Browser regressions for request cancellation, mode changes and serialized polling."""
import copy
import time
from urllib.parse import parse_qs, urlparse
from playwright.sync_api import Error, expect


def wait_for_requests(page, pending, count):
    deadline = time.monotonic() + 5
    while len(pending) < count and time.monotonic() < deadline:
        page.wait_for_timeout(20)
    assert len(pending) == count, f"Expected {count} requests, received {len(pending)}"


def fulfill(route, payload):
    # A correctly canceled browser request can already be gone when its server reply arrives.
    try:
        route.fulfill(json=payload)
    except Error as error:
        if "closed" not in str(error).lower() and "invalid interception" not in str(error).lower():
            raise


def verify_request_lifecycle(browser, url, snapshot):
    overview = {
        "generatedAt": "2026-10-10T12:00:00Z", "window": "24h",
        "totals": {"transactions": 999, "amountMinor": 99900, "approved": 999,
                   "review": 0, "rejected": 0, "finalizationP95": None},
        "series": [], "rules": [], "decisions": [], "services": [],
        "checkpoint": None, "job": None,
    }
    context = browser.new_context(viewport={"width": 1512, "height": 1100})
    try:
        page = context.new_page()
        pending = []
        page.route("**/api/overview?*", lambda route: pending.append(route))
        page.goto(url, wait_until="domcontentloaded")
        wait_for_requests(page, pending, 1)
        page.get_by_label("Time window").select_option("15m")
        wait_for_requests(page, pending, 2)
        latest = copy.deepcopy(overview)
        latest["window"] = "15m"
        latest["totals"].update(transactions=111, approved=111, amountMinor=11100)
        fulfill(pending[1], latest)
        expect(page.locator(".stat-value").first).to_have_text("111")
        fulfill(pending[0], overview)
        page.wait_for_timeout(50)
        expect(page.locator(".stat-value").first).to_have_text("111")

        # A response from live mode cannot overwrite the saved run after switching modes.
        page.get_by_label("Time window").select_option("24h")
        wait_for_requests(page, pending, 3)
        page.get_by_role("button", name="View demo", exact=True).click()
        expect(page.locator(".stat-value").first).to_have_text(str(snapshot["provenance"]["exported_payments"]))
        fulfill(pending[2], overview)
        page.wait_for_timeout(50)
        expect(page.locator(".stat-value").first).to_have_text(str(snapshot["provenance"]["exported_payments"]))
    finally:
        context.close()

    context = browser.new_context(viewport={"width": 1512, "height": 1100})
    try:
        page = context.new_page()
        page.clock.install()
        page.route("**/api/overview?*", lambda route: route.fulfill(json=overview))
        pending = []
        page.route("**/api/showcase", lambda route: pending.append(route))
        page.goto(url, wait_until="domcontentloaded")
        page.get_by_role("button", name="Showcase", exact=True).click()
        wait_for_requests(page, pending, 1)
        page.clock.fast_forward(6000)
        assert len(pending) == 1, "Polling must not overlap a pending request"
        page.get_by_role("button", name="Overview", exact=True).click()
        page.get_by_role("button", name="Showcase", exact=True).click()
        wait_for_requests(page, pending, 2)
        current = {**snapshot["provenance"], "run_id": "current-test-run"}
        fulfill(pending[1], current)
        expect(page.locator(".showcase-run code")).to_have_text("current-test-run")
        fulfill(pending[0], {**current, "run_id": "stale-test-run"})
        page.wait_for_timeout(50)
        expect(page.locator(".showcase-run code")).to_have_text("current-test-run")
        page.clock.fast_forward(5000)
        wait_for_requests(page, pending, 3)
        page.clock.fast_forward(6000)
        assert len(pending) == 3, "A slow poll must finish before another starts"
        fulfill(pending[2], current)
    finally:
        context.close()

    context = browser.new_context(viewport={"width": 1512, "height": 1100})
    try:
        page = context.new_page()
        page.route("**/api/overview?*", lambda route: route.fulfill(json=overview))
        queries = []
        original = snapshot["cases"][0]
        decisions = [entry["decision"] for entry in reversed(original["timeline"])]
        summary = {"customer_id": "first-customer", "payments": len(decisions), "alerts": 0,
                   "maxScore": 0, "lastEvent": decisions[0]["event_time"]}

        def customers(route):
            params = parse_qs(urlparse(route.request.url).query)
            queries.append(params)
            cursor = params.get("cursor", [""])[0]
            query = params.get("q", [""])[0]
            customer = "changed-customer" if query else "second-customer" if cursor else "first-customer"
            route.fulfill(json={"generatedAt": overview["generatedAt"],
                                "items": [{**summary, "customer_id": customer}],
                                "nextCursor": None if query or cursor else "page-2"})

        def timeline(route):
            params = parse_qs(urlparse(route.request.url).query)
            older = bool(params.get("cursor"))
            items = decisions[2:4] if older else decisions[:2]
            items = [{**decision, "customer_id": params["customer"][0],
                      "policy_snapshot": snapshot["policies"][decision["policy_ref"]]} for decision in items]
            route.fulfill(json={"generatedAt": overview["generatedAt"], "customer": summary,
                                "items": items, "nextCursor": None if older else "older-page"})

        page.route("**/api/investigations/customers?*", customers)
        page.route("**/api/investigations/timeline?*", timeline)
        page.goto(url, wait_until="domcontentloaded")
        page.get_by_role("button", name="Investigations", exact=True).click()
        expect(page.locator(".customer-result strong")).to_have_text("first-customer")
        page.get_by_role("button", name="Next customers", exact=True).click()
        expect(page.locator(".customer-result strong")).to_have_text("second-customer")
        page.get_by_label("Customer ID prefix").fill("changed")
        expect(page.locator(".customer-result strong")).to_have_text("changed-customer")
        assert "cursor" not in queries[-1]
        page.get_by_label("Customer ID prefix").fill("")
        expect(page.locator(".customer-result strong")).to_have_text("first-customer")
        assert "cursor" not in queries[-1], "Returning to a search must start at its first page"
        page.locator(".customer-result").click()
        expect(page.locator(".evidence-heading p")).to_have_text(decisions[0]["transaction_id"])
        page.locator(".payment-timeline button").last.click()
        expect(page.locator(".evidence-heading p")).to_have_text(decisions[1]["transaction_id"])
        page.get_by_role("button", name="Older payments", exact=True).click()
        expect(page.locator(".evidence-heading p")).to_have_text(decisions[2]["transaction_id"])
        page.get_by_role("button", name="Newest payments", exact=True).click()
        expect(page.locator(".evidence-heading p")).to_have_text(decisions[0]["transaction_id"])
    finally:
        context.close()
