import { decisionLabels } from "../../lib/format";
import type {
  InvestigationDecision,
  RuleEvidence,
} from "../../investigation-types";
import { formatMoney, formatUtcDate } from "../../lib/format";
const names: Record<string, string> = {
  R001: "Payment frequency",
  R002: "Amount in window",
  R003: "Distinct devices",
  R004: "New device, high amount",
  R005: "Approval after declines",
};
const evidenceValue = (rule: RuleEvidence, n: number) =>
  ["R002", "R004"].includes(rule.rule_id) ? formatMoney(n) : String(n);
export function DecisionEvidence({
  decision,
  demo,
}: {
  decision: InvestigationDecision;
  demo: boolean;
}) {
  let thresholds: { review_threshold?: number; reject_threshold?: number } = {};
  try {
    thresholds = JSON.parse(decision.policy_snapshot || "{}");
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
          <h2>{formatMoney(decision.amount_minor)}</h2>
          <p>{decision.transaction_id}</p>
        </div>
        <div className={`evidence-score ${decision.decision.toLowerCase()}`}>
          <strong>{decision.risk_score}</strong>
          <span>{decisionLabels[decision.decision]}</span>
        </div>
      </div>
      <p className="threshold-note">
        {thresholds.review_threshold !== undefined
          ? `Needs review from ${thresholds.review_threshold}; high risk from ${thresholds.reject_threshold}. Combined score capped at 100.`
          : "Historical classification thresholds unavailable."}
      </p>
      <div className="evidence-rules">
        {decision.rule_evidence.map((rule) => (
          <article
            className={`evidence-rule ${rule.matched ? "matched" : ""}`}
            key={rule.rule_id}
          >
            <div>
              <span>
                {rule.rule_id} / v{rule.rule_version}
              </span>
              <strong>
                {rule.matched
                  ? `+${rule.score_contribution} points`
                  : !rule.enabled
                    ? "Disabled"
                    : !rule.eligible
                      ? "Not eligible"
                      : "Not matched"}
              </strong>
            </div>
            <h3>{names[rule.rule_id] || rule.type}</h3>
            <p>
              Observed <b>{evidenceValue(rule, rule.observed)}</b>
              <span>
                {" "}
                / threshold{" "}
                {rule.rule_id === "R001" || rule.rule_id === "R002"
                  ? ">"
                  : ">="}{" "}
                {evidenceValue(rule, rule.threshold)}
              </span>
            </p>
            <small>
              Lookback{" "}
              {rule.window_seconds >= 86400
                ? `${rule.window_seconds / 86400} days`
                : `${rule.window_seconds / 60} min`}
              {rule.rule_id === "R004"
                ? "; requires an unseen device"
                : rule.rule_id === "R005"
                  ? "; requires an approved input"
                  : ""}
            </small>
          </article>
        ))}
      </div>
      {!decision.rule_evidence.length && (
        <p className="investigation-message">
          Rule evidence was not recorded for this historical decision.
        </p>
      )}
      <details className="decision-provenance">
        <summary>Decision provenance and policy</summary>
        <dl>
          <dt>Event time</dt>
          <dd>{formatUtcDate(decision.event_time)}</dd>
          <dt>Evaluated at</dt>
          <dd>{formatUtcDate(decision.processed_at)}</dd>
          <dt>Engine</dt>
          <dd>{decision.engine_version || "Unavailable"}</dd>
          <dt>Policy</dt>
          <dd>
            {decision.policy_id} / {decision.policy_version}
          </dd>
          <dt>Evaluation</dt>
          <dd>{decision.evaluation_id}</dd>
          <dt>Fingerprint</dt>
          <dd>{decision.rules_fingerprint}</dd>
          <dt>Decision source</dt>
          <dd>
            {demo
              ? "Synthetic fixture; no Kafka delivery"
              : `${decision.source_topic} / partition ${decision.source_partition} / offset ${decision.source_offset}`}
          </dd>
        </dl>
        <pre>{decision.policy_snapshot || "Snapshot unavailable"}</pre>
      </details>
    </section>
  );
}
