"""Analyze offered load and durable assessment backlog without hiding producer stalls."""
import math


def percentile(db, expression, start, end, fraction=.95):
    if expression not in ("stored_at-scheduled_at", "producer_at-scheduled_at"):
        raise ValueError("Unsupported latency expression")
    count = db.execute("SELECT count(*) FROM benchmark_arrivals WHERE scheduled_at>=? AND scheduled_at<?", (start, end)).fetchone()[0]
    if not count:
        return None
    return db.execute(f"SELECT {expression} FROM benchmark_arrivals WHERE scheduled_at>=? AND scheduled_at<? ORDER BY {expression} LIMIT 1 OFFSET ?",
                      (start, end, math.ceil(count * fraction) - 1)).fetchone()[0]


def slope(points):
    origin = points[0][0]
    points = [(x - origin, y) for x, y in points]
    mean_x = sum(x for x, _ in points) / len(points)
    mean_y = sum(y for _, y in points) / len(points)
    denominator = sum((x - mean_x) ** 2 for x, _ in points)
    return sum((x - mean_x) * (y - mean_y) for x, y in points) / denominator if denominator else 0


def sustained_summary(db, report, warmup_seconds=30, window_seconds=20):
    if warmup_seconds < 0 or window_seconds < 1:
        raise ValueError("Invalid observation windows")
    count = db.execute("SELECT count(*) FROM benchmark_arrivals").fetchone()[0]
    result = {"status": "insufficient_evidence", "scope": "Offered payments to durable assessments; excludes warmup and post-publication drain",
              "warmup_seconds": warmup_seconds, "window_seconds": window_seconds, "windows": []}
    if not report.get("load_mode") or count != report["stored_decisions"] or not count:
        result["reason"] = "Complete offered-schedule coverage requires the live-load producer"
        return result
    start = report["publishing_started_at"]
    rate = report["config"]["publish_rate"]
    end = start + math.ceil(count * 1000 / rate)
    boundary = start + warmup_seconds * 1000
    step = window_seconds * 1000
    while boundary + step <= end:
        stop = boundary + step
        due = db.execute("SELECT count(*) FROM benchmark_arrivals WHERE scheduled_at<?", (stop,)).fetchone()[0]
        stored = db.execute("SELECT count(*) FROM benchmark_arrivals WHERE stored_at<?", (stop,)).fetchone()[0]
        completions = db.execute("SELECT count(*) FROM benchmark_arrivals WHERE stored_at>=? AND stored_at<?", (boundary, stop)).fetchone()[0]
        result["windows"].append({"from_seconds": (boundary-start)/1000, "to_seconds": (stop-start)/1000,
                                  "offered_backlog": due-stored, "stored_per_second": completions/window_seconds,
                                  "scheduled_to_storage_p95_ms": percentile(db, "stored_at-scheduled_at", boundary, stop),
                                  "generator_delay_p95_ms": percentile(db, "producer_at-scheduled_at", boundary, stop)})
        boundary = stop
    windows = result["windows"]
    result["criteria"] = {"minimum_windows": 4, "minimum_achieved_rate_fraction": .95,
                          "maximum_generator_p95_ms": 1000, "maximum_backlog_growth_per_second": max(1, rate*.02),
                          "maximum_latency_growth_ms": max(1000, windows[0]["scheduled_to_storage_p95_ms"]*.1) if windows else None}
    if len(windows) < 4:
        result["reason"] = "At least four complete observation windows are required"
        return result
    growth = slope([(w["to_seconds"], w["offered_backlog"]) for w in windows])
    latency_growth = windows[-1]["scheduled_to_storage_p95_ms"] - windows[0]["scheduled_to_storage_p95_ms"]
    result.update(backlog_growth_per_second=growth, latency_growth_ms=latency_growth)
    if report["achieved_payments_per_second"] < rate*.95 or max(w["generator_delay_p95_ms"] for w in windows) > 1000:
        result.update(status="generator_limited", reason="The driver did not maintain its offered schedule; this run cannot qualify the requested processing rate")
    elif growth > result["criteria"]["maximum_backlog_growth_per_second"] or latency_growth > result["criteria"]["maximum_latency_growth_ms"]:
        result.update(status="backlog_or_latency_growing", reason="The observed load did not meet the bounded-growth criteria")
    else:
        result.update(status="observed_stable", reason="Bounded backlog and latency over this observation interval; not a maximum-capacity or long-duration guarantee")
    return result
