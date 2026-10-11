import { Clock3 } from "lucide-react";
import type {
  InvestigationDecision,
  TimelinePage,
} from "../../investigation-types";
import { formatMoney, formatUtcDate } from "../../lib/format";
interface PaymentTimelineProps {
  timeline: TimelinePage;
  selected: InvestigationDecision | null;
  cursor: string;
  onSelect: (id: string) => void;
  onPageChange: (cursor: string) => void;
}
export function PaymentTimeline({
  timeline,
  selected,
  cursor,
  onSelect,
  onPageChange,
}: PaymentTimelineProps) {
  return (
    <section className="card timeline-panel" aria-label="Customer timeline">
      <div className="card-heading">
        <h2>Payment timeline</h2>
        <Clock3 size={16} />
      </div>
      <p className="timeline-caption">Newest event first. Select a payment.</p>
      <ol className="payment-timeline">
        {timeline.items.map((decision) => (
          <li key={decision.evaluation_id}>
            <button
              aria-pressed={selected?.evaluation_id === decision.evaluation_id}
              className={
                selected?.evaluation_id === decision.evaluation_id
                  ? "chosen"
                  : ""
              }
              onClick={() => onSelect(decision.evaluation_id)}
            >
              <small>{formatUtcDate(decision.event_time)}</small>
              <div>
                <strong>{formatMoney(decision.amount_minor)}</strong>
                <span className={`badge ${decision.decision.toLowerCase()}`}>
                  {decision.decision}
                </span>
              </div>
              <span>Score {decision.risk_score} / 100</span>
              <small className="timeline-id">{decision.transaction_id}</small>
            </button>
          </li>
        ))}
      </ol>
      <div className="investigation-pager">
        {cursor && (
          <button className="button secondary" onClick={() => onPageChange("")}>
            Newest payments
          </button>
        )}
        {timeline.nextCursor && (
          <button
            className="button secondary"
            onClick={() => onPageChange(timeline.nextCursor!)}
          >
            Older payments
          </button>
        )}
      </div>
      <p className="timeline-caption">
        Only evaluated payments are listed. Pending, invalid and late inputs
        remain in the separate input archive.
      </p>
    </section>
  );
}
