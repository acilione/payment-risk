import { decisionLabels } from "../../lib/format";
import { useState } from "react";
import {
  ArrowRight,
  Check,
  Database,
  GitBranch,
  Play,
  RefreshCw,
} from "lucide-react";
import { useJsonResource } from "../../hooks/useJsonResource";
import { profiles } from "./profiles";
import { RunConfiguration } from "./RunConfiguration";
import { RunInstructions } from "./RunInstructions";
import { Reconciliation } from "./Reconciliation";
import type { ShowcaseReport } from "./types";
import snapshot from "../../showcase-data";
import "../../showcase.css";
export function Showcase({
  demo,
  investigate,
}: {
  demo: boolean;
  investigate: (id: string) => void;
}) {
  const [refresh, setRefresh] = useState(0);
  const live = useJsonResource<ShowcaseReport>(
    demo ? null : "showcase",
    refresh,
    5000,
  );
  const report: ShowcaseReport | null = demo ? snapshot.provenance : live.data;
  const error = live.error
    ? "Start a run with make showcase to see its results here."
    : "";
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
      name: "Stored assessments",
      value: report?.stored_decisions,
      text: "Evaluated payments; excludes invalid and late inputs",
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
          <Reconciliation report={report} />
          <div className="showcase-columns">
            <RunConfiguration config={report.config} />
            <section className="card showcase-results">
              <div className="card-heading">
                <h2>Pipeline results</h2>
                <span>Flink decisions</span>
              </div>
              <div className="showcase-bars">
                {(["APPROVE", "REVIEW", "REJECT"] as const).map((decision) => {
                  const count = report.decision_counts?.[decision] || 0;
                  return (
                    <div key={decisionLabels[decision]}>
                      <div>
                        <span className={`badge ${decision.toLowerCase()}`}>
                          {decisionLabels[decision]}
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
              <p>
                Each example comes from this run. Devices identify checkouts,
                not bank login sessions.
              </p>
            </div>
            <div className="showcase-examples">
              {examples.map((c) => (
                <button
                  className="scenario-card"
                  key={c.id}
                  onClick={() => investigate(c.id)}
                >
                  <span>{c.payments} payments</span>
                  <strong>{profiles[c.profile]?.name || c.profile}</strong>
                  <p>{profiles[c.profile]?.description}</p>
                  <p>{c.id}</p>
                  <ArrowRight size={17} />
                </button>
              ))}
            </div>
          </section>
        </>
      )}
      <RunInstructions />
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
