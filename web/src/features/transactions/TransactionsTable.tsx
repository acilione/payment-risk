import {
  ArrowUpRight,
  CheckCheck,
  ChevronDown,
  ChevronRight,
  Search,
  SlidersHorizontal,
  X,
} from "lucide-react";
import type { Decision, DecisionFilter } from "../../types";
import {
  formatMoney,
  formatTime,
  shortenId,
  decisionLabels,
} from "../../lib/format";

interface TransactionsTableProps {
  decisions: Decision[];
  hasData: boolean;
  compact: boolean;
  demo: boolean;
  search: string;
  filter: DecisionFilter;
  onSearchChange: (search: string) => void;
  onFilterChange: (filter: DecisionFilter) => void;
  onSelect: (decision: Decision) => void;
  onViewAll: () => void;
}
export function TransactionsTable({
  decisions,
  hasData,
  compact,
  demo,
  search,
  filter,
  onSearchChange,
  onFilterChange,
  onSelect,
  onViewAll,
}: TransactionsTableProps) {
  return (
    <section className="card transactions-card">
      <div className="card-heading">
        <div>
          <h2>
            Recent decisions{" "}
            <span className="heading-count">{decisions.length}</span>
          </h2>
          <p>Select a transaction to view its score and matched rules</p>
        </div>
        {compact && (
          <button className="text-button" onClick={() => onViewAll()}>
            View all <ArrowUpRight size={14} />
          </button>
        )}
      </div>
      <div className="table-controls">
        <label className="search-box">
          <Search size={15} />
          <input
            aria-label="Search transactions"
            placeholder="Search transaction or customer…"
            value={search}
            onChange={(e) => onSearchChange(e.target.value)}
          />
          {search && (
            <button
              className="icon-button"
              onClick={() => onSearchChange("")}
              aria-label="Clear search"
            >
              <X size={13} />
            </button>
          )}
        </label>
        <label className="filter-select">
          <SlidersHorizontal size={14} />
          <select
            aria-label="Decision filter"
            value={filter}
            onChange={(e) => onFilterChange(e.target.value as DecisionFilter)}
          >
            <option value="ALL">All decisions</option>
            <option value="APPROVE">{decisionLabels.APPROVE}</option>
            <option value="REVIEW">{decisionLabels.REVIEW}</option>
            <option value="REJECT">{decisionLabels.REJECT}</option>
          </select>
          <ChevronDown size={13} />
        </label>
      </div>
      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              <th>TRANSACTION</th>
              <th>AMOUNT</th>
              <th>RISK SCORE</th>
              <th>DECISION</th>
              <th>TIME</th>
              <th>
                <span className="sr-only">Inspect</span>
              </th>
            </tr>
          </thead>
          <tbody>
            {decisions.slice(0, compact ? 6 : 100).map((d) => (
              <tr key={d.transaction_id}>
                <td>
                  <button
                    className="transaction-link"
                    onClick={() => onSelect(d)}
                  >
                    <span
                      className={`transaction-icon ${d.decision.toLowerCase()}`}
                    >
                      <ArrowUpRight size={15} />
                    </span>
                    <span>
                      <strong>
                        {shortenId(d.transaction_id.replace("txn_", ""))}
                      </strong>
                      <small>{shortenId(d.customer_id)}</small>
                    </span>
                  </button>
                </td>
                <td className="amount">{formatMoney(d.amount_minor)}</td>
                <td>
                  <div className="score-cell">
                    <span>{d.risk_score}</span>
                    <div>
                      <i
                        className={d.decision.toLowerCase()}
                        style={{
                          width: `${Math.max(5, d.risk_score)}%`,
                        }}
                      />
                    </div>
                  </div>
                </td>
                <td>
                  <span className={`badge ${d.decision.toLowerCase()}`}>
                    <i />
                    {decisionLabels[d.decision]}
                  </span>
                </td>
                <td className="table-time">{formatTime(d.processed_at)}</td>
                <td>
                  <button
                    className="icon-button"
                    onClick={() => onSelect(d)}
                    aria-label={`Inspect ${d.transaction_id}`}
                  >
                    <ChevronRight size={15} />
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {!decisions.length && (
          <div className="empty-state">
            <Search size={23} />
            <strong>
              {hasData ? "No matching decisions" : "Waiting for payment data"}
            </strong>
            <p>
              {hasData
                ? "Try a wider time window or another search."
                : "Start the local services or select View demo to load sample data."}
            </p>
          </div>
        )}
      </div>
      <div className="table-footer">
        <span>
          <CheckCheck size={13} />{" "}
          {demo
            ? "Generated payment decisions"
            : "One decision per transaction"}
        </span>
        <span>
          Latest {Math.min(decisions.length, compact ? 6 : 100)} of{" "}
          {decisions.length} loaded
        </span>
      </div>
    </section>
  );
}
