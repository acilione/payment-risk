"""Observe an actual database outage and recovery in the showcase dashboard."""
import json
import signal
import subprocess
import time
import urllib.request
from pathlib import Path
from playwright.sync_api import sync_playwright, expect

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "artifacts/website"
OUT.mkdir(parents=True, exist_ok=True)

with (OUT / "recovery-run.log").open("w") as log:
    process = subprocess.Popen(["python3", "scripts/showcase.py", "run", "--no-build", "--database-outage"],
                               cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    try:
        deadline = time.monotonic() + 240
        while True:
            if process.poll() is not None:
                raise RuntimeError("Showcase exited before the browser connected; inspect recovery-run.log")
            try:
                with urllib.request.urlopen("http://127.0.0.1:23001/api/health", timeout=2) as response:
                    if response.status == 200:
                        break
            except OSError:
                pass
            if time.monotonic() > deadline:
                raise TimeoutError("Website did not start")
            time.sleep(1)
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page(viewport={"width": 1512, "height": 1100}, reduced_motion="reduce")
            page.goto("http://127.0.0.1:23001/", wait_until="networkidle")
            page.get_by_role("button", name="Showcase", exact=True).click()
            panel = page.get_by_role("region", name="Processing completeness")
            expect(panel).to_contain_text("Consumer offsets stayed unchanged", timeout=240000)
            expect(panel).to_contain_text("Database unavailable")
            page.screenshot(path=str(OUT / "database-outage.png"), full_page=True)
            page.set_viewport_size({"width": 390, "height": 844})
            assert page.evaluate("document.documentElement.scrollWidth <= innerWidth"), "Outage view overflows on mobile"
            page.screenshot(path=str(OUT / "database-outage-mobile.png"), full_page=True)
            expect(panel).to_contain_text("Replaying decisions preserved one logical result per payment", timeout=240000)
            expect(page.locator(".showcase-status")).to_have_text("COMPLETE", timeout=30000)
            expect(panel.locator("dd")).to_have_text(["0", "0"])
            expect(panel).to_contain_text("IDs and hashes checked")
            page.set_viewport_size({"width": 1512, "height": 1100})
            page.screenshot(path=str(OUT / "database-recovered.png"), full_page=True)
            browser.close()
        assert process.wait(timeout=30) == 0, "Showcase verification failed"
        report = json.loads((ROOT / "artifacts/showcase/latest.json").read_text())
        assert report["recovery"]["offsets_unchanged"]
        assert report["reconciliation"]["verified"]
        assert report["stored_decisions"] == report["expected_payments"]
        print(json.dumps({"result": "PASS", "run_id": report["run_id"],
                          "checks": ["visible database outage", "no consumer offset advance", "mobile layout",
                                     "complete input archive", "one decision per payment after replay"]}))
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=45)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
