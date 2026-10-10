import { useEffect, useState } from "react";
import {
  ArrowRight,
  Check,
  Copy,
  Database,
  GitBranch,
  Play,
  RefreshCw,
} from "lucide-react";
import snapshot from "./showcase-data";
import "./showcase.css";

type Report = {
  run_id: string;
  status: string;
  updated_at: string;
  source_commit: string;
  source_dirty: boolean;
  generator_version?: string;
  expected_payments?: number;
  acknowledged_deliveries?: number;
  transport_retries?: number;
  control_records?: number;
  archived_records?: number;
  stored_decisions?: number;
  decision_counts?: Record<string, number>;
  verification?: string;
  error?: string;
  config: {
    seed: number;
    customers: number;
    min_payments: number;
    max_payments: number;
    history_minutes: number;
    publish_rate: number;
    retry_percent: number;
    profiles: Record<string, number>;
  };
  customers?: { id: string; profile: string; payments: number }[];
};
const names: Record<string, string> = {
  normal: "Regular purchases",
  new_device: "Replacement phone",
  takeover: "Account takeover",
  card_testing: "Card testing",
  low_burst: "Small payment burst",
  trusted_device: "Familiar device misuse",
};
const commands =
  "make showcase\n# Open http://localhost:23001\nmake showcase-stop\n# Edit config/showcase.json, then run again";
export function Showcase({
  demo,
  investigate,
}: {
  demo: boolean;
  investigate: (id: string) => void;
}) {
  const [report, setReport] = useState<Report | null>(
    demo ? snapshot.provenance : null,
  );
  const [error, setError] = useState("");
  const [refresh, setRefresh] = useState(0);
  const [copied, setCopied] = useState(false);
  useEffect(() => {
    if (demo) {
      setReport(snapshot.provenance);
      setError("");
      return;
    }
    const controller = new AbortController();
    const read = async () => {
      try {
        const response = await fetch(
          `${import.meta.env.BASE_URL}api/showcase`,
          {
            signal: AbortSignal.any([
              controller.signal,
              AbortSignal.timeout(10000),
            ]),
            cache: "no-store",
          },
        );
        if (!response.ok)
          throw new Error(
            "Start a run with make showcase to see its results here.",
          );
        const value: Report = await response.json();
        if (!controller.signal.aborted) {
          setReport(value);
          setError("");
        }
      } catch (e) {
        if (!controller.signal.aborted)
          setError(e instanceof Error ? e.message : "Run report unavailable.");
      }
    };
    void read();
    const timer = setInterval(() => void read(), 5000);
    return () => {
      controller.abort();
      clearInterval(timer);
    };
  }, [demo, refresh]);
  const examples = demo
    ? snapshot.cases
        .filter(
          (c, i, cases) =>
            cases.findIndex((s) => s.profile === c.profile) === i,
        )
        .map((c) => ({
          id: c.target_payment.customer_id,
          profile: c.profile,
          payments: c.timeline.length,
        }))
    : (report?.customers || []).filter(
        (c, i, cases) => cases.findIndex((s) => s.profile === c.profile) === i,
      );
  const stages = [
    {
      name: "Generated payments",
      value: report?.expected_payments,
      text: "Unique payment IDs",
      icon: Play,
    },
    {
      name: "Kafka acknowledgements",
      value: report?.acknowledged_deliveries,
      text: `Includes ${report?.transport_retries ?? "-"} retry deliveries`,
      icon: GitBranch,
    },
    {
      name: "Archived input records",
      value: report?.archived_records,
      text: `Includes ${report?.control_records ?? "-"} watermark controls`,
      icon: Database,
    },
    {
      name: "Stored decisions",
      value: report?.stored_decisions,
      text: "One logical result per payment",
      icon: Check,
    },
  ];
  return (
    <div className="showcase">
      <section className="showcase-intro">
        <div>
          <span className="eyebrow">REPRODUCIBLE PAYMENT SIMULATION</span>
          <h2>Generated payments, recorded decisions.</h2>
          <p>
            Configurable customer histories pass through Kafka, Flink and
            ClickHouse. Explore the decisions and the evidence behind them.
          </p>
        </div>
        <button
          className="button secondary"
          onClick={() => setRefresh((x) => x + 1)}
        >
          <RefreshCw size={14} />
          Refresh
        </button>
      </section>
      {error && (
        <p className="error-banner" role="alert">
          {error}
        </p>
      )}
      {report && (
        <>
          <div className="showcase-run">
            <span className={`showcase-status ${report.status.toLowerCase()}`}>
              {report.status}
            </span>
            <code>{report.run_id}</code>
            <span>
              {demo ? "Saved pipeline run" : "Local pipeline run"} /{" "}
              {new Date(report.updated_at).toLocaleString("en-GB", {
                timeZone: "UTC",
              })}{" "}
              UTC
            </span>
          </div>
          <section className="showcase-flow" aria-label="Run reconciliation">
            {stages.map(({ name, value, text, icon: Icon }, i) => (
              <article className="card" key={name}>
                <span className="showcase-stage">
                  <Icon size={17} />
                  {String(i + 1).padStart(2, "0")}
                </span>
                <h3>{name}</h3>
                <strong>
                  {value === undefined ? "-" : value.toLocaleString("en-GB")}
                </strong>
                <p>{text}</p>
                {i < 3 && <ArrowRight className="flow-arrow" size={18} />}
              </article>
            ))}
          </section>
          <p className="showcase-verification">
            {report.verification ||
              "Waiting for generation and storage checks to finish."}
          </p>
          {report.error && (
            <p role="alert" className="error-banner">
              {report.error}
            </p>
          )}
          <div className="showcase-columns">
            <section className="card showcase-settings">
              <div className="card-heading">
                <h2>Generation settings</h2>
                <span>Seed {report.config.seed}</span>
              </div>
              <dl>
                <div>
                  <dt>Customers</dt>
                  <dd>{report.config.customers}</dd>
                </div>
                <div>
                  <dt>Payments per customer</dt>
                  <dd>
                    {report.config.min_payments} to {report.config.max_payments}
                  </dd>
                </div>
                <div>
                  <dt>Simulated history</dt>
                  <dd>{report.config.history_minutes} minutes</dd>
                </div>
                <div>
                  <dt>Publish rate</dt>
                  <dd>{report.config.publish_rate}/second</dd>
                </div>
                <div>
                  <dt>Retry probability</dt>
                  <dd>{report.config.retry_percent}%</dd>
                </div>
              </dl>
              <h3>Customer behavior weights</h3>
              {Object.entries(report.config.profiles).map(
                ([profile, weight]) => (
                  <div className="profile-weight" key={profile}>
                    <span>{names[profile] || profile}</span>
                    <div>
                      <i style={{ width: `${weight}%` }} />
                    </div>
                    <b>{weight}%</b>
                  </div>
                ),
              )}
              <p className="showcase-note">
                Weights are sampling probabilities. The seed reproduces amounts,
                behavior and relative timing; each run receives new IDs and a
                current time anchor.
              </p>
            </section>
            <section className="card showcase-results">
              <div className="card-heading">
                <h2>Pipeline results</h2>
                <span>Flink decisions</span>
              </div>
              <div className="showcase-bars">
                {["APPROVE", "REVIEW", "REJECT"].map((decision) => {
                  const count = report.decision_counts?.[decision] || 0;
                  return (
                    <div key={decision}>
                      <div>
                        <span className={`badge ${decision.toLowerCase()}`}>
                          {decision}
                        </span>
                        <strong>{count}</strong>
                      </div>
                      <div className="decision-track">
                        <i
                          className={decision.toLowerCase()}
                          style={{
                            width: `${(count / Math.max(1, report.stored_decisions || 0)) * 100}%`,
                          }}
                        />
                      </div>
                    </div>
                  );
                })}
              </div>
              <p className="showcase-note">
                Intent labels belong to the generator and never enter the
                payment stream. They let you examine detections and rule
                limitations; they do not measure real-world fraud accuracy.
              </p>
              <p className="showcase-note">
                {demo
                  ? `${snapshot.provenance.exported_payments} decisions from ${snapshot.provenance.exported_customers} complete customer histories are included in this snapshot.`
                  : "Open an investigation to read the stored customer history from ClickHouse."}
              </p>
            </section>
          </div>
          <section>
            <div className="showcase-section-heading">
              <h2>Investigate a generated customer</h2>
              <p>Each example comes from this run.</p>
            </div>
            <div className="showcase-examples">
              {examples.map((c) => (
                <button
                  className="scenario-card"
                  key={c.id}
                  onClick={() => investigate(c.id)}
                >
                  <span>{c.payments} payments</span>
                  <strong>{names[c.profile] || c.profile}</strong>
                  <p>{c.id}</p>
                  <ArrowRight size={17} />
                </button>
              ))}
            </div>
          </section>
        </>
      )}
      <section className="card showcase-replay">
        <div>
          <h2>Run it locally</h2>
          <p>
            Edit <code>config/showcase.json</code> to change the seed, volume,
            amounts and behavior mix. Start a fresh run, then follow its
            payments in the dashboard.
          </p>
          <p className="showcase-note">
            History is published in chronological order on one Kafka partition.
            This presentation workflow is not a distributed throughput
            benchmark. Stopping retains data volumes and a Flink savepoint.
          </p>
        </div>
        <div>
          <button
            className="button secondary"
            onClick={() => {
              void navigator.clipboard
                .writeText(commands)
                .then(() => setCopied(true))
                .catch(() => setCopied(false));
            }}
          >
            <Copy size={14} />
            {copied ? "Copied" : "Copy commands"}
          </button>
          <pre>{commands}</pre>
        </div>
      </section>
      {report && (
        <p className="showcase-note">
          Generator: {report.generator_version || "starting"} / Source commit:{" "}
          {report.source_commit.slice(0, 12)}
          {report.source_dirty ? " + working tree changes" : ""}. Full inputs
          and verification report:{" "}
          <code>artifacts/showcase/{report.run_id}/</code>
        </p>
      )}
    </div>
  );
}
