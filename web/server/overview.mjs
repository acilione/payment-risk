export const windows = Object.freeze({ "15m": 900, "1h": 3600, "24h": 86400 });

export function createReader(
  { clickhouse, user, password, flink, prometheus },
  fetcher = fetch,
) {
  async function json(url, options = {}) {
    const response = await fetcher(url, {
      ...options,
      signal: AbortSignal.timeout(5000),
    });
    if (!response.ok) throw new Error("Upstream unavailable");
    return response.json();
  }
  async function sql(query) {
    const result = await json(clickhouse, {
      method: "POST",
      headers: {
        "X-ClickHouse-User": user,
        "X-ClickHouse-Key": password,
        "Content-Type": "text/plain",
      },
      body: query + " SETTINGS readonly=1, max_execution_time=4 FORMAT JSON",
    });
    return result.data;
  }
  return async function overview(window) {
    if (!Object.hasOwn(windows, window)) throw new Error("Invalid time window");
    const seconds = windows[window];
    const bucket = seconds / 30;
    const where = `processed_at >= toUnixTimestamp64Milli(now64()) - ${seconds * 1000}`;
    const [totals, series, rules, decisions, cluster, metrics] =
      await Promise.allSettled([
        sql(
          `SELECT count() AS transactions, sum(amount_minor) AS amountMinor, countIf(decision='APPROVE') AS approved, countIf(decision='REVIEW') AS review, countIf(decision='REJECT') AS rejected, if(count()=0,0,quantileExact(0.95)(greatest(0,processed_at-event_time))) AS finalizationP95 FROM risk.decisions_current WHERE ${where}`,
        ),
        sql(
          `SELECT intDiv(processed_at,${bucket * 1000})*${bucket * 1000} AS time, countIf(decision='APPROVE') AS approved, countIf(decision='REVIEW') AS review, countIf(decision='REJECT') AS rejected FROM risk.decisions_current WHERE ${where} GROUP BY time ORDER BY time`,
        ),
        sql(
          `SELECT arrayJoin(matched_rules) AS id, count() AS matches FROM risk.decisions_current WHERE ${where} GROUP BY id ORDER BY id`,
        ),
        sql(
          `SELECT transaction_id,event_id,customer_id,amount_minor,risk_score,decision,matched_rules,reason_codes,rules_fingerprint,event_time,processed_at,source_partition,source_offset FROM risk.decisions_current WHERE ${where} ORDER BY processed_at DESC,transaction_id LIMIT 100`,
        ),
        json(flink + "/jobs/overview"),
        json(
          prometheus +
            "/api/v1/query?query=" +
            encodeURIComponent(
              "max(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max)",
            ),
        ),
      ]);
    if ([totals, series, rules, decisions].some((x) => x.status === "rejected"))
      throw new Error("Analytics unavailable");
    const numeric = (row, fields) =>
      Object.fromEntries(
        Object.entries(row).map(([k, v]) => [
          k,
          fields.includes(k) ? Number(v) : v,
        ]),
      );
    const job =
      cluster.status === "fulfilled"
        ? cluster.value.jobs.find(
            (j) => j.name === "payment-risk-v1" && j.state === "RUNNING",
          )
        : null;
    let checkpoint = null;
    if (job) {
      try {
        const checkpoints = await json(
          flink + "/jobs/" + encodeURIComponent(job.jid) + "/checkpoints",
        );
        const c = checkpoints.latest?.completed;
        if (c)
          checkpoint = {
            id: c.id,
            duration: c.end_to_end_duration,
            bytes: c.state_size,
            completedAt: c.latest_ack_timestamp,
          };
      } catch {
        /* A failed health probe must not replace valid analytical data. */
      }
    }
    const lagValue =
      metrics.status === "fulfilled" && metrics.value.status === "success"
        ? metrics.value.data?.result?.[0]?.value?.[1]
        : undefined;
    const lag =
      lagValue !== undefined &&
      Number.isFinite(Number(lagValue)) &&
      Number(lagValue) >= 0
        ? Number(lagValue)
        : undefined;
    return {
      generatedAt: new Date().toISOString(),
      window,
      totals: numeric(totals.value[0], [
        "transactions",
        "amountMinor",
        "approved",
        "review",
        "rejected",
        "finalizationP95",
      ]),
      series: series.value.map((r) =>
        numeric(r, ["time", "approved", "review", "rejected"]),
      ),
      rules: rules.value.map((r) => numeric(r, ["matches"])),
      decisions: decisions.value.map((r) =>
        numeric(r, [
          "amount_minor",
          "risk_score",
          "event_time",
          "processed_at",
          "source_partition",
          "source_offset",
        ]),
      ),
      checkpoint,
      job: job ? { id: job.jid, state: job.state } : null,
      services: [
        {
          name: "Kafka",
          status: lag === undefined ? "unavailable" : "healthy",
          detail:
            lag === undefined
              ? "Kafka lag unavailable"
              : `${lag} records behind, reported by Flink`,
        },
        {
          name: "Apache Flink",
          status: job ? "healthy" : "unavailable",
          detail: job
            ? "Payment processing job running"
            : "No running payment-risk job detected",
        },
        {
          name: "ClickHouse",
          status: "healthy",
          detail: "Stored payment decisions",
        },
        {
          name: "Checkpoints",
          status:
            checkpoint && Date.now() - checkpoint.completedAt < 180000
              ? "healthy"
              : "unavailable",
          detail: checkpoint
            ? `#${checkpoint.id} · ${checkpoint.duration} ms to complete`
            : "Checkpoint data unavailable",
        },
      ],
    };
  };
}
