import { ShieldCheck } from "lucide-react";
import type { Overview } from "../../types";
import { formatNumber } from "../../lib/format";
import { ruleCatalog } from "../../data/rules";

export function RuleMatches({
  data,
  onViewRules,
}: {
  data: Overview | null;
  onViewRules: () => void;
}) {
  const max = Math.max(1, ...(data?.rules.map((match) => match.matches) ?? []));
  return (
    <section className="card rules-card">
      <div className="card-heading">
        <div>
          <h2>Rule matches</h2>
          <p>Matched rules in this window</p>
        </div>
        <span className="tiny-icon">
          <ShieldCheck size={16} />
        </span>
      </div>
      <div className="rule-list">
        {ruleCatalog.map((rule) => {
          const count =
            data?.rules.find((match) => match.id === rule.id)?.matches || 0;
          return (
            <button key={rule.id} className="rule-row" onClick={onViewRules}>
              <span className="rule-number">{rule.id}</span>
              <div>
                <span>
                  {rule.name}
                  <strong>{formatNumber(count)}</strong>
                </span>
                <div className="rule-track">
                  <i style={{ width: `${(count / max) * 100}%` }} />
                </div>
              </div>
            </button>
          );
        })}
      </div>
      <div className="rules-note">
        <ShieldCheck size={16} />
        <span>Select a transaction to see the rules used in its score.</span>
      </div>
    </section>
  );
}
