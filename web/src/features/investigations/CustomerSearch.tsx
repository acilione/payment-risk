import { Search } from "lucide-react";
import type { CustomerPage } from "../../investigation-types";
import type { DecisionFilter } from "../../types";
import { formatUtcDate } from "../../lib/format";
interface CustomerSearchProps {
  query: string;
  filter: DecisionFilter;
  customer: string;
  customers: CustomerPage | null;
  error: Error | null;
  cursor: string;
  onQueryChange: (query: string) => void;
  onFilterChange: (filter: DecisionFilter) => void;
  onPageChange: (cursor: string) => void;
  onSelect: (id: string) => void;
}
export function CustomerSearch({
  query,
  filter,
  customer,
  customers,
  error,
  cursor,
  onQueryChange,
  onFilterChange,
  onPageChange,
  onSelect,
}: CustomerSearchProps) {
  return (
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
            onChange={(e) => onQueryChange(e.target.value)}
            placeholder="e.g. show-"
            maxLength={256}
          />
        </label>
        <label>
          Has a decision
          <select
            value={filter}
            onChange={(e) => {
              onFilterChange(e.target.value as DecisionFilter);
              onPageChange("");
            }}
          >
            <option value="ALL">Any decision</option>
            <option value="REVIEW">Review</option>
            <option value="REJECT">Reject</option>
            <option value="APPROVE">Approve</option>
          </select>
        </label>
      </div>
      {error ? (
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
            {customers.items.map((customerSummary) => (
              <button
                key={customerSummary.customer_id}
                className={`customer-result ${customer === customerSummary.customer_id ? "chosen" : ""}`}
                onClick={() => onSelect(customerSummary.customer_id)}
                aria-pressed={customer === customerSummary.customer_id}
              >
                <strong>{customerSummary.customer_id}</strong>
                <span>
                  {customerSummary.payments} payments{" "}
                  <b>
                    {customerSummary.alerts}{" "}
                    {customerSummary.alerts === 1 ? "alert" : "alerts"}
                  </b>
                </span>
                <small>
                  Last event {formatUtcDate(customerSummary.lastEvent)}
                </small>
              </button>
            ))}
          </div>
          {!customers.items.length && (
            <p className="investigation-message">
              No matching customers. Try another prefix or decision filter.
            </p>
          )}
          <div className="investigation-pager">
            {cursor && (
              <button
                className="button secondary"
                onClick={() => onPageChange("")}
              >
                First page
              </button>
            )}
            {customers.nextCursor && (
              <button
                className="button secondary"
                onClick={() => onPageChange(customers.nextCursor!)}
              >
                Next customers
              </button>
            )}
          </div>
        </>
      )}
    </section>
  );
}
