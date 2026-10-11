import { Check, CheckCheck, Database, ShieldCheck, X } from "lucide-react";
import type { Decision } from "../../types";
import { formatMoney, decisionLabels } from "../../lib/format";
import { ruleCatalog } from "../../data/rules";
import { Modal } from "../../components/Modal";

interface DecisionInspectorProps {
  selected: Decision | null;
  demo: boolean;
  onClose: () => void;
}
export function DecisionInspector({
  selected,
  demo,
  onClose,
}: DecisionInspectorProps) {
  return (
    <Modal
      open={selected !== null}
      onClose={onClose}
      className="inspector"
      labelledBy="inspector-title"
    >
      {selected && (
        <div className="inspector-body">
          <div className="inspector-top">
            <span className="eyebrow">PAYMENT DETAILS</span>
            <button
              className="icon-button"
              onClick={() => onClose()}
              aria-label="Close inspector"
            >
              <X size={20} />
            </button>
          </div>
          <h2 id="inspector-title">Decision details</h2>
          <p className="inspector-id">{selected.transaction_id}</p>
          <div className="inspector-score">
            <div>
              <strong>
                {selected.risk_score}
                <small>/ 100</small>
              </strong>
              <span className={`badge ${selected.decision.toLowerCase()}`}>
                {decisionLabels[selected.decision]}
              </span>
            </div>
            <ShieldCheck size={42} />
          </div>
          <dl>
            <dt>Customer</dt>
            <dd>{selected.customer_id}</dd>
            <dt>Payment amount</dt>
            <dd>{formatMoney(selected.amount_minor)}</dd>
            <dt>Event time</dt>
            <dd>{new Date(selected.event_time).toLocaleString()}</dd>
            <dt>Finalized at</dt>
            <dd>{new Date(selected.processed_at).toLocaleString()}</dd>
          </dl>
          <h3>Matched rules</h3>
          {selected.matched_rules.length ? (
            selected.matched_rules.map((id) => (
              <div className="matched-rule" key={id}>
                <span className="rule-number">{id}</span>
                <span>
                  <strong>
                    {ruleCatalog.find((r) => r.id === id)?.name || id}
                  </strong>
                  <small>
                    {selected.reason_codes.find((c) => c.includes(id)) ||
                      "Rule condition matched"}
                  </small>
                </span>
                <Check size={16} />
              </div>
            ))
          ) : (
            <div className="no-match">
              <CheckCheck size={18} />
              No risk rule matched this payment.
            </div>
          )}
          {selected.reason_codes.length > 0 && (
            <>
              <h3>Recorded reason codes</h3>
              <code>{selected.reason_codes.join(" · ")}</code>
            </>
          )}
          <h3>Rules fingerprint</h3>
          <code className="fingerprint">{selected.rules_fingerprint}</code>
          <p className="inspector-note">
            This fingerprint identifies the rule settings used for this payment.
            They may differ from the current settings.
          </p>
          <div className="source-position">
            <Database size={15} />
            {demo
              ? "Synthetic engine fixture; no Kafka delivery"
              : `Kafka partition ${selected.source_partition} / offset ${selected.source_offset}`}
          </div>
        </div>
      )}
    </Modal>
  );
}
