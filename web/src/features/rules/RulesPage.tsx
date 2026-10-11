import { Check, ShieldCheck } from "lucide-react";
import type { Overview } from "../../types";
import { formatNumber } from "../../lib/format";
import { ruleCatalog } from "../../data/rules";

export function RulesPage({ data }: { data: Overview | null }) {
  return (
    <>
      <div className="policy-note">
        <ShieldCheck size={22} />
        <div>
          <strong>Default risk rules</strong>
          <p>
            These are the default rule settings. Live updates arrive through
            Kafka. Each decision includes a fingerprint of the settings used to
            calculate its score.
          </p>
        </div>
      </div>
      <div className="rule-catalog">
        {ruleCatalog.map((rule) => (
          <section className="card rule-detail" key={rule.id}>
            <span className="rule-number">{rule.id}</span>
            <span className="rule-weight">+{rule.score} points</span>
            <h2>{rule.name}</h2>
            <p>{rule.description}</p>
            <div className="rule-detail-footer">
              <span>
                {formatNumber(
                  data?.rules.find((match) => match.id === rule.id)?.matches ||
                    0,
                )}{" "}
                matches in selected window
              </span>
              <Check size={15} />
            </div>
          </section>
        ))}
      </div>
      <section className="card score-guide">
        <h2>Decision thresholds</h2>
        <p>
          The score is the sum of matched rule weights, capped at 100. These are
          the default thresholds; they can be changed in the deployment
          settings.
        </p>
        <div>
          <span className="approve">
            <strong>0–29</strong>Approve
          </span>
          <span className="review">
            <strong>30–69</strong>Review
          </span>
          <span className="reject">
            <strong>70–100</strong>Reject
          </span>
        </div>
      </section>
    </>
  );
}
