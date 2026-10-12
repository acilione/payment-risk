import { useState } from "react";
import { ArrowRight, RefreshCw, ShieldCheck } from "lucide-react";
import { demoCustomers, demoTimeline, stories } from "../../investigation-demo";
import type { CustomerPage, TimelinePage } from "../../investigation-types";
import type { DecisionFilter } from "../../types";
import { formatUtcDate } from "../../lib/format";
import { useDebouncedValue } from "../../hooks/useDebouncedValue";
import { useJsonResource } from "../../hooks/useJsonResource";
import { DecisionEvidence } from "./DecisionEvidence";
import { PaymentTimeline } from "./PaymentTimeline";
import { CustomerSearch } from "./CustomerSearch";
import "../../investigations.css";
export function Investigations({
  demo,
  initialCustomer = "",
}: {
  demo: boolean;
  initialCustomer?: string;
}) {
  const [query, setQuery] = useState("");
  const [filter, setFilter] = useState<DecisionFilter>("ALL");
  const [customer, setCustomer] = useState(initialCustomer);
  const [customerPage, setCustomerPage] = useState({
    query: "",
    filter: "ALL",
    cursor: "",
  });
  const [timelineCursor, setTimelineCursor] = useState("");
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [refresh, setRefresh] = useState(0);
  const debouncedQuery = useDebouncedValue(query.trim(), 300);
  const customerCursor =
    customerPage.query === debouncedQuery && customerPage.filter === filter
      ? customerPage.cursor
      : "";
  function setCustomerCursor(cursor: string) {
    setCustomerPage({ query: debouncedQuery, filter, cursor });
  }
  function changeQuery(value: string) {
    setQuery(value);
    setCustomerCursor("");
  }
  function changeTimelinePage(cursor: string) {
    setSelectedId(null);
    setTimelineCursor(cursor);
  }
  const customerRequest = useJsonResource<CustomerPage>(
    demo
      ? null
      : `investigations/customers?${new URLSearchParams({ q: debouncedQuery, decision: filter, cursor: customerCursor })}`,
    refresh,
  );
  const timelineRequest = useJsonResource<TimelinePage>(
    demo || !customer
      ? null
      : `investigations/timeline?${new URLSearchParams({ customer, cursor: timelineCursor })}`,
    refresh,
  );
  const customers = demo
    ? demoCustomers(debouncedQuery, filter)
    : customerRequest.data;
  const timeline =
    demo && customer ? demoTimeline(customer) : timelineRequest.data;
  const customerError = customerRequest.error;
  const timelineError = timelineRequest.error;
  const selected =
    timeline?.items.find((decision) => decision.evaluation_id === selectedId) ??
    timeline?.items[0] ??
    null;
  function open(id: string) {
    setTimelineCursor("");
    setSelectedId(null);
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
          {stories
            .filter(
              (s, index, cases) =>
                ["takeover", "new_device", "trusted_device"].includes(
                  s.profile,
                ) && cases.findIndex((c) => c.profile === s.profile) === index,
            )
            .map((s) => (
              <button
                className={`scenario-card ${customer === s.target_payment.customer_id ? "chosen" : ""}`}
                key={s.scenario}
                onClick={() => open(s.target_payment.customer_id)}
              >
                <span>{s.outcome.replaceAll("_", " ").toLowerCase()}</span>
                <strong>{s.title}</strong>
                <p>{s.story}</p>
                <ArrowRight size={17} />
              </button>
            ))}
        </section>
      )}
      <div className="investigation-workspace">
        <CustomerSearch
          query={query}
          filter={filter}
          customer={customer}
          customers={customers}
          error={customerError}
          cursor={customerCursor}
          onQueryChange={changeQuery}
          onFilterChange={setFilter}
          onPageChange={setCustomerCursor}
          onSelect={open}
        />
        <div className="customer-investigation">
          {!customer ? (
            <section className="card investigation-empty">
              <ShieldCheck size={32} />
              <h2>Choose a customer to begin</h2>
              <p>
                Open a guided example or search by customer ID. Timelines show
                evaluated payments, including low-risk assessments.
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
                  {demo ? "Fixture" : "Data as of"}{" "}
                  {formatUtcDate(Date.parse(timeline.generatedAt))}
                  <br />
                  Source:{" "}
                  {demo ? "synthetic engine fixture" : "PostgreSQL decisions"}
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
                <PaymentTimeline
                  timeline={timeline}
                  selected={selected}
                  cursor={timelineCursor}
                  onSelect={setSelectedId}
                  onPageChange={changeTimelinePage}
                />
                {selected && (
                  <DecisionEvidence decision={selected} demo={demo} />
                )}
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
