import { ArrowUpRight, Check, ShieldCheck, Workflow } from "lucide-react";
import type { Overview, WindowKey } from "../../types";
import { formatNumber } from "../../lib/format";
import { TrendChart } from "./TrendChart";

export function OverviewCharts({
  data,
  demo,
  window,
  onArchitecture,
}: {
  data: Overview | null;
  demo: boolean;
  window: WindowKey;
  onArchitecture: () => void;
}) {
  const totals = data?.totals;
  const total = totals?.transactions ?? 0;
  const approval =
    total && totals ? ((totals.approved / total) * 100).toFixed(1) : "0.0";
  const healthy = data?.services.every(
    (service) => service.status === "healthy",
  );
  return (
    <>
      <div className="charts-grid">
        <section className="card trend-card">
          <div className="card-heading">
            <div>
              <h2>Risk assessments over time</h2>
              <p>
                {demo
                  ? "Counts by payment event time"
                  : "Counts by decision processing time"}
              </p>
            </div>
            <span className="chip">
              {window === "1h"
                ? "2 min"
                : window === "15m"
                  ? "30 sec"
                  : "48 min"}{" "}
              buckets
            </span>
          </div>
          <div className="chart-summary">
            <strong>{formatNumber(total)}</strong>
            <span>decisions in this window</span>
            <div className="legend">
              <span>
                <i className="green" />
                Low risk
              </span>
              <span>
                <i className="amber" />
                Needs review
              </span>
              <span>
                <i className="coral" />
                High risk
              </span>
            </div>
          </div>
          <TrendChart series={data?.series || []} />
        </section>
        <section className="card distribution">
          <div className="card-heading">
            <div>
              <h2>Risk breakdown</h2>
              <p>Share of low-risk, review, and high-risk assessments</p>
            </div>
            <ShieldCheck size={18} className="muted" />
          </div>
          <div className="donut-wrap">
            <svg
              viewBox="0 0 180 180"
              role="img"
              aria-label={`${approval}% low risk`}
            >
              <circle
                cx="90"
                cy="90"
                r="65"
                fill="none"
                stroke="#f0efeb"
                strokeWidth="19"
              />
              {totals &&
                total > 0 &&
                [totals.approved, totals.review, totals.rejected].map(
                  (count, i, all) => (
                    <circle
                      key={i}
                      cx="90"
                      cy="90"
                      r="65"
                      fill="none"
                      stroke={["#46866c", "#ddae62", "#e68063"][i]}
                      strokeWidth="19"
                      strokeDasharray={`${(count / total) * 408.4} 408.4`}
                      strokeDashoffset={
                        (-all.slice(0, i).reduce((a, b) => a + b, 0) / total) *
                        408.4
                      }
                      transform="rotate(-90 90 90)"
                    />
                  ),
                )}
              <text x="90" y="88" textAnchor="middle" className="donut-value">
                {approval}%
              </text>
              <text x="90" y="109" textAnchor="middle" className="donut-label">
                LOW RISK
              </text>
            </svg>
          </div>
          <div className="breakdown-rows">
            {[
              {
                name: "Low risk",
                count: totals?.approved ?? 0,
                color: "green",
              },
              {
                name: "Needs review",
                count: totals?.review ?? 0,
                color: "amber",
              },
              {
                name: "High risk",
                count: totals?.rejected ?? 0,
                color: "coral",
              },
            ].map(({ name, count, color }) => (
              <div key={name}>
                <span>
                  <i className={`dot ${color}`} />
                  {name}
                </span>
                <strong>{formatNumber(count)}</strong>
                <small>
                  {total ? ((count / total) * 100).toFixed(1) : "0.0"}%
                </small>
              </div>
            ))}
          </div>
        </section>
      </div>
      <section className="pipeline-strip">
        <div className="pipeline-intro">
          <span className="pipeline-icon">
            <Workflow size={20} />
          </span>
          <div>
            <strong>Service status</strong>
            <small>
              {demo
                ? "Sample service status"
                : healthy
                  ? "All monitored services available"
                  : "Check service availability"}
            </small>
          </div>
        </div>
        <div className="pipeline-services">
          {data?.services.map((s, i) => (
            <div className="service-wrap" key={s.name}>
              <div className="service" title={s.detail}>
                <i className={`status-dot ${s.status}`} />
                <span>{s.name}</span>
                {s.status === "healthy" && <Check size={12} />}
              </div>
              {i < 3 && <span className="service-link">···</span>}
            </div>
          )) || <span className="muted">Waiting for service status</span>}
        </div>
        <button className="text-button" onClick={() => onArchitecture()}>
          View architecture <ArrowUpRight size={14} />
        </button>
      </section>
    </>
  );
}
