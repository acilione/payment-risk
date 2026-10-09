import { useEffect, useState } from "react";
import {
  ArrowRight,
  Search,
  RefreshCw,
  ShieldCheck,
  Clock3,
} from "lucide-react";
import { demoCustomers, demoTimeline, stories } from "./investigation-demo";
import type {
  CustomerPage,
  TimelinePage,
  InvestigationDecision,
  RuleEvidence,
} from "./investigation-types";
import "./investigations.css";
const money = (n: number) =>
  new Intl.NumberFormat("en-IE", { style: "currency", currency: "EUR" }).format(
    n / 100,
  );
const date = (n: number) =>
  new Date(n).toLocaleString("en-GB", {
    timeZone: "UTC",
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }) + " UTC";
const names: Record<string, string> = {
  R001: "Payment frequency",
  R002: "Amount in window",
  R003: "Distinct devices",
  R004: "New device, high amount",
  R005: "Approval after declines",
};
const evidenceValue = (r: RuleEvidence, n: number) =>
  ["R002", "R004"].includes(r.rule_id) ? money(n) : String(n);
async function read<T>(path: string, signal: AbortSignal): Promise<T> {
  const response = await fetch(
    `${import.meta.env.BASE_URL}api/investigations/${path}`,
    {
      signal: AbortSignal.any([signal, AbortSignal.timeout(12000)]),
      cache: "no-store",
    },
  );
  if (
    !response.ok ||
    !response.headers.get("content-type")?.includes("application/json")
  )
    throw new Error("Unavailable");
  return response.json();
}
export function Investigations({ demo }: { demo: boolean }) {
  const [query, setQuery] = useState("");
  const [q, setQ] = useState("");
  const [filter, setFilter] = useState("ALL");
  const [customers, setCustomers] = useState<CustomerPage | null>(null);
  const [customer, setCustomer] = useState("");
  const [timeline, setTimeline] = useState<TimelinePage | null>(null);
  const [selected, setSelected] = useState<InvestigationDecision | null>(null);
  const [customerCursor, setCustomerCursor] = useState("");
  const [timelineCursor, setTimelineCursor] = useState("");
  const [refresh, setRefresh] = useState(0);
  const [customerError, setCustomerError] = useState(false);
  const [timelineError, setTimelineError] = useState(false);
  useEffect(() => {
    const timer = setTimeout(() => {
      setQ(query.trim());
      setCustomerCursor("");
    }, 300);
    return () => clearTimeout(timer);
  }, [query]);
  useEffect(() => {
    const controller = new AbortController();
    setCustomers(null);
    setCustomerError(false);
    const request = demo
      ? Promise.resolve(demoCustomers(q, filter))
      : read<CustomerPage>(
          `customers?${new URLSearchParams({ q, decision: filter, cursor: customerCursor })}`,
          controller.signal,
        );
    void request
      .then((value) => {
        if (!controller.signal.aborted) setCustomers(value);
      })
      .catch(() => {
        if (!controller.signal.aborted) setCustomerError(true);
      });
    return () => controller.abort();
  }, [demo, q, filter, customerCursor, refresh]);
  useEffect(() => {
    const controller = new AbortController();
    setTimeline(null);
    setSelected(null);
    setTimelineError(false);
    if (!customer) return;
    const request = demo
      ? Promise.resolve(demoTimeline(customer))
      : read<TimelinePage>(
          `timeline?${new URLSearchParams({ customer, cursor: timelineCursor })}`,
          controller.signal,
        );
    void request
      .then((value) => {
        if (!controller.signal.aborted) {
          setTimeline(value);
          setSelected(value.items[0] || null);
        }
      })
      .catch(() => {
        if (!controller.signal.aborted) setTimelineError(true);
      });
    return () => controller.abort();
  }, [demo, customer, timelineCursor, refresh]);
  function open(id: string) {
    setTimelineCursor("");
    setCustomer(id);
  }
  const story = demo
    ? stories.find((s) => s.target_payment.customer_id === customer)
    : undefined;
  return (
    <div className="investigations">
      <div className="investigation-intro">
        <div>
          <span className="eyebrow">CUSTOMER INVESTIGATION</span>
          <h2>Payment history and rule evidence.</h2>
          <p>
            Search recorded decisions and reconstruct what the rules observed.
            An alert is a reason to review a payment.
          </p>
        </div>
        <button
          className="button secondary"
          onClick={() => setRefresh((x) => x + 1)}
        >
          <RefreshCw size={14} /> Refresh
        </button>
      </div>
      {demo && (
        <section className="scenario-grid" aria-label="Guided investigations">
          {[
            {
              id: "account-takeover",
              title: "Rapid attempts across devices",
              tag: "Detected pattern",
              text: "Four declines, new devices, then approvals.",
            },
            {
              id: "new-phone",
              title: "A legitimate replacement phone",
              tag: "False alarm",
              text: "Why a normal purchase received a review.",
            },
            {
              id: "stolen-trusted-device",
              title: "A familiar device, a missed signal",
              tag: "Missed detection",
              text: "Why a suspicious payment scored zero.",
            },
          ].map((s) => (
            <button
              className={`scenario-card ${customer === "demo-" + s.id ? "chosen" : ""}`}
              key={s.id}
              onClick={() => open("demo-" + s.id)}
            >
              <span>{s.tag}</span>
              <strong>{s.title}</strong>
              <p>{s.text}</p>
              <ArrowRight size={17} />
            </button>
          ))}
        </section>
      )}
      <div className="investigation-workspace">
        <section className="card customer-panel" aria-label="Customer search">
          <div className="card-heading">
            <h2>Customers</h2>
            <Search size={17} />
          </div>
          <div className="customer-controls">
            <label>
              Customer ID prefix
              <input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="e.g. demo-new-phone"
                maxLength={256}
              />
            </label>
            <label>
              Has a decision
              <select
                value={filter}
                onChange={(e) => {
                  setFilter(e.target.value);
                  setCustomerCursor("");
                }}
              >
                <option value="ALL">Any decision</option>
                <option value="REVIEW">Review</option>
                <option value="REJECT">Reject</option>
                <option value="APPROVE">Approve</option>
              </select>
            </label>
          </div>
          {customerError ? (
            <p role="alert" className="investigation-message">
              Customer data unavailable. Refresh to retry.
            </p>
          ) : !customers ? (
            <p role="status" className="investigation-message">
              Loading customers...
            </p>
          ) : (
            <>
              <div className="customer-list">
                {customers.items.map((c) => (
                  <button
                    key={c.customer_id}
                    className={`customer-result ${customer === c.customer_id ? "chosen" : ""}`}
                    onClick={() => open(c.customer_id)}
                    aria-pressed={customer === c.customer_id}
                  >
                    <strong>{c.customer_id}</strong>
                    <span>
                      {c.payments} payments <b>{c.alerts} alerts</b>
                    </span>
                    <small>Last event {date(c.lastEvent)}</small>
                  </button>
                ))}
              </div>
              {!customers.items.length && (
                <p className="investigation-message">
                  No matching customers. Try another prefix or decision filter.
                </p>
              )}
              <div className="investigation-pager">
                {customerCursor && (
                  <button
                    className="button secondary"
                    onClick={() => setCustomerCursor("")}
                  >
                    First page
                  </button>
                )}
                {customers.nextCursor && (
                  <button
                    className="button secondary"
                    onClick={() => setCustomerCursor(customers.nextCursor!)}
                  >
                    Next customers
                  </button>
                )}
              </div>
            </>
          )}
        </section>
        <div className="customer-investigation">
          {!customer ? (
            <section className="card investigation-empty">
              <ShieldCheck size={32} />
              <h2>Choose a customer to begin</h2>
              <p>
                Open a guided example or search by customer ID. Timelines show
                evaluated payments, including approvals.
              </p>
            </section>
          ) : timelineError ? (
            <section className="card investigation-empty" role="alert">
              <h2>Timeline unavailable</h2>
              <p>
                The source is unavailable or stored decisions require an
                integrity review. No partial history is shown.
              </p>
              <button
                className="button secondary"
                onClick={() => setRefresh((x) => x + 1)}
              >
                Retry timeline
              </button>
            </section>
          ) : !timeline ? (
            <section className="card investigation-empty" role="status">
              Loading customer history...
            </section>
          ) : (
            <>
              <section className="card customer-summary">
                <div>
                  <span className="eyebrow">CUSTOMER</span>
                  <h2>{customer}</h2>
                  <p>
                    {timeline.customer
                      ? `${timeline.customer.payments} evaluated payments / ${timeline.customer.alerts} alerts / highest score ${timeline.customer.maxScore}`
                      : "No evaluated payments found for this customer."}
                  </p>
                </div>
                <small>
                  Read {new Date(timeline.generatedAt).toLocaleString("en-GB")}
                  <br />
                  Source:{" "}
                  {demo ? "synthetic engine fixture" : "ClickHouse decisions"}
                </small>
              </section>
              {story && (
                <aside
                  className={`scenario-context ${story.outcome.toLowerCase()}`}
                >
                  <strong>
                    Authored scenario:{" "}
                    {story.label === "LEGITIMATE"
                      ? "legitimate activity"
                      : "suspicious activity"}
                  </strong>
                  <p>{story.story}</p>
                  <small>
                    This label describes the final payment in the synthetic
                    story. It is not an input to the engine or a confirmed fraud
                    finding.
                  </small>
                </aside>
              )}
              <div className="timeline-detail-grid">
                <section
                  className="card timeline-panel"
                  aria-label="Customer timeline"
                >
                  <div className="card-heading">
                    <h2>Payment timeline</h2>
                    <Clock3 size={16} />
                  </div>
                  <p className="timeline-caption">
                    Newest event first. Select a payment.
                  </p>
                  <ol className="payment-timeline">
                    {timeline.items.map((d) => (
                      <li key={d.evaluation_id}>
                        <button
                          aria-pressed={
                            selected?.evaluation_id === d.evaluation_id
                          }
                          className={
                            selected?.evaluation_id === d.evaluation_id
                              ? "chosen"
                              : ""
                          }
                          onClick={() => setSelected(d)}
                        >
                          <small>{date(d.event_time)}</small>
                          <div>
                            <strong>{money(d.amount_minor)}</strong>
                            <span
                              className={`badge ${d.decision.toLowerCase()}`}
                            >
                              {d.decision}
                            </span>
                          </div>
                          <span>Score {d.risk_score} / 100</span>
                          <small className="timeline-id">
                            {d.transaction_id}
                          </small>
                        </button>
                      </li>
                    ))}
                  </ol>
                  <div className="investigation-pager">
                    {timelineCursor && (
                      <button
                        className="button secondary"
                        onClick={() => setTimelineCursor("")}
                      >
                        Newest payments
                      </button>
                    )}
                    {timeline.nextCursor && (
                      <button
                        className="button secondary"
                        onClick={() => setTimelineCursor(timeline.nextCursor!)}
                      >
                        Older payments
                      </button>
                    )}
                  </div>
                  <p className="timeline-caption">
                    Only evaluated payments are listed. Pending, invalid and
                    late inputs remain in the separate input archive.
                  </p>
                </section>
                {selected && <Evidence decision={selected} demo={demo} />}
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
function Evidence({
  decision: d,
  demo,
}: {
  decision: InvestigationDecision;
  demo: boolean;
}) {
  let thresholds: { review_threshold?: number; reject_threshold?: number } = {};
  try {
    thresholds = JSON.parse(d.policy_snapshot || "{}");
  } catch {
    /* Historical snapshots may be absent. */
  }
  return (
    <section
      className="card evidence-panel"
      aria-label="Recorded rule evidence"
    >
      <div className="evidence-heading">
        <div>
          <span className="eyebrow">RECORDED DECISION</span>
          <h2>{money(d.amount_minor)}</h2>
          <p>{d.transaction_id}</p>
        </div>
        <div className={`evidence-score ${d.decision.toLowerCase()}`}>
          <strong>{d.risk_score}</strong>
          <span>{d.decision}</span>
        </div>
      </div>
      <p className="threshold-note">
        {thresholds.review_threshold !== undefined
          ? `Review from ${thresholds.review_threshold}; reject from ${thresholds.reject_threshold}. Combined score capped at 100.`
          : "Historical classification thresholds unavailable."}
      </p>
      <div className="evidence-rules">
        {d.rule_evidence.map((r) => (
          <article
            className={`evidence-rule ${r.matched ? "matched" : ""}`}
            key={r.rule_id}
          >
            <div>
              <span>
                {r.rule_id} / v{r.rule_version}
              </span>
              <strong>
                {r.matched
                  ? `+${r.score_contribution} points`
                  : !r.enabled
                    ? "Disabled"
                    : !r.eligible
                      ? "Not eligible"
                      : "Not matched"}
              </strong>
            </div>
            <h3>{names[r.rule_id] || r.type}</h3>
            <p>
              Observed <b>{evidenceValue(r, r.observed)}</b>
              <span>
                {" "}
                / threshold{" "}
                {r.rule_id === "R001" || r.rule_id === "R002" ? ">" : ">="}{" "}
                {evidenceValue(r, r.threshold)}
              </span>
            </p>
            <small>
              Lookback{" "}
              {r.window_seconds >= 86400
                ? `${r.window_seconds / 86400} days`
                : `${r.window_seconds / 60} min`}
              {r.rule_id === "R004"
                ? "; requires an unseen device"
                : r.rule_id === "R005"
                  ? "; requires an approved input"
                  : ""}
            </small>
          </article>
        ))}
      </div>
      {!d.rule_evidence.length && (
        <p className="investigation-message">
          Rule evidence was not recorded for this historical decision.
        </p>
      )}
      <details className="decision-provenance">
        <summary>Decision provenance and policy</summary>
        <dl>
          <dt>Event time</dt>
          <dd>{date(d.event_time)}</dd>
          <dt>Evaluated at</dt>
          <dd>{date(d.processed_at)}</dd>
          <dt>Engine</dt>
          <dd>{d.engine_version || "Unavailable"}</dd>
          <dt>Policy</dt>
          <dd>
            {d.policy_id} / {d.policy_version}
          </dd>
          <dt>Evaluation</dt>
          <dd>{d.evaluation_id}</dd>
          <dt>Fingerprint</dt>
          <dd>{d.rules_fingerprint}</dd>
          <dt>Decision source</dt>
          <dd>
            {demo
              ? "Synthetic fixture; no Kafka delivery"
              : `${d.source_topic} / partition ${d.source_partition} / offset ${d.source_offset}`}
          </dd>
        </dl>
        <pre>{d.policy_snapshot || "Snapshot unavailable"}</pre>
      </details>
    </section>
  );
}
