import { useEffect, useState } from "react";
import { ArrowUpRight, Radio } from "lucide-react";
import type { Decision, DecisionFilter, WindowKey } from "../types";
import type { Page } from "./navigation";
import { github, staticDemo } from "./config";
import { Sidebar } from "../components/Sidebar";
import { TopBar } from "../components/TopBar";
import { PageHeading } from "../components/PageHeading";
import { AboutDialog } from "../components/AboutDialog";
import { DecisionInspector } from "../features/transactions/DecisionInspector";
import { TransactionsTable } from "../features/transactions/TransactionsTable";
import { OverviewSummary } from "../features/overview/OverviewSummary";
import { OverviewCharts } from "../features/overview/OverviewCharts";
import { useOverview } from "../features/overview/useOverview";
import { RuleMatches } from "../features/rules/RuleMatches";
import { RulesPage } from "../features/rules/RulesPage";
import { Architecture } from "../features/architecture/Architecture";
import { Investigations } from "../features/investigations/Investigations";
import { Showcase } from "../features/showcase/Showcase";

export function App() {
  const [page, setPage] = useState<Page>("Overview");
  const [demo, setDemo] = useState(staticDemo);
  const [timeWindow, setTimeWindow] = useState<WindowKey>("24h");
  const [refreshVersion, setRefreshVersion] = useState(0);
  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState<DecisionFilter>("ALL");
  const [selectedDecision, setSelectedDecision] = useState<Decision | null>(
    null,
  );
  const [investigationCustomer, setInvestigationCustomer] = useState("");
  const [navigationOpen, setNavigationOpen] = useState(false);
  const [aboutOpen, setAboutOpen] = useState(false);
  const { data, loading, error } = useOverview(
    demo,
    timeWindow,
    page !== "Investigations" && page !== "Showcase",
    refreshVersion,
  );

  useEffect(() => {
    const closeNavigation = (event: KeyboardEvent) => {
      if (event.key === "Escape") setNavigationOpen(false);
    };
    globalThis.addEventListener("keydown", closeNavigation);
    return () => globalThis.removeEventListener("keydown", closeNavigation);
  }, []);

  function navigate(nextPage: Page) {
    setPage(nextPage);
    setNavigationOpen(false);
    globalThis.scrollTo(0, 0);
  }
  function changeMode() {
    setSelectedDecision(null);
    setDemo((previous) => !previous);
  }
  function investigate(customerId: string) {
    setInvestigationCustomer(customerId);
    navigate("Investigations");
  }
  function refresh() {
    setRefreshVersion((previous) => previous + 1);
  }
  const normalizedSearch = search.toLowerCase();
  const decisions = (data?.decisions ?? []).filter(
    (decision) =>
      (filter === "ALL" || decision.decision === filter) &&
      `${decision.transaction_id} ${decision.customer_id} ${decision.matched_rules.join(" ")}`
        .toLowerCase()
        .includes(normalizedSearch),
  );
  const showsPayments = page === "Overview" || page === "Transactions";

  return (
    <div className="app-shell">
      <Sidebar
        activePage={page}
        isOpen={navigationOpen}
        onClose={() => setNavigationOpen(false)}
        onNavigate={navigate}
        onAbout={() => setAboutOpen(true)}
      />
      <div className="main-shell">
        <TopBar
          activePage={page}
          navigationOpen={navigationOpen}
          onToggleNavigation={() => setNavigationOpen((previous) => !previous)}
          onAbout={() => setAboutOpen(true)}
          demo={demo}
          error={error}
          loading={loading}
          hasData={data !== null}
        />
        <main>
          <PageHeading
            activePage={page}
            demo={demo}
            onChangeMode={changeMode}
          />
          {error && !demo && (
            <div role="alert" className="error-banner">
              <Radio size={18} />
              <div>
                <strong>Live data unavailable</strong>
                <p>
                  {error}
                  {data ? " The last received data is shown below." : ""}
                </p>
              </div>
              <button className="button secondary" onClick={refresh}>
                Retry
              </button>
            </div>
          )}
          {page === "Showcase" && (
            <Showcase
              key={String(demo)}
              demo={demo}
              investigate={investigate}
            />
          )}
          {page === "Investigations" && (
            <Investigations
              key={`${demo}:${investigationCustomer}`}
              demo={demo}
              initialCustomer={investigationCustomer}
            />
          )}
          {showsPayments && (
            <OverviewSummary
              data={data}
              tab={page}
              window={timeWindow}
              loading={loading}
              onRefresh={refresh}
              onWindowChange={setTimeWindow}
            />
          )}
          {page === "Overview" && (
            <OverviewCharts
              data={data}
              demo={demo}
              window={timeWindow}
              onArchitecture={() => navigate("Architecture")}
            />
          )}
          {showsPayments && (
            <div className={page === "Overview" ? "bottom-grid" : ""}>
              <TransactionsTable
                decisions={decisions}
                hasData={data !== null}
                compact={page === "Overview"}
                demo={demo}
                search={search}
                filter={filter}
                onSearchChange={setSearch}
                onFilterChange={setFilter}
                onSelect={setSelectedDecision}
                onViewAll={() => navigate("Transactions")}
              />
              {page === "Overview" && (
                <RuleMatches
                  data={data}
                  onViewRules={() => navigate("Risk rules")}
                />
              )}
            </div>
          )}
          {page === "Risk rules" && <RulesPage data={data} />}
          {page === "Architecture" && <Architecture data={data} demo={demo} />}
          <footer className="page-footer">
            <span>Payment risk</span>
            <span>
              <a href={github} target="_blank" rel="noreferrer">
                Source code <ArrowUpRight size={12} />
              </a>
            </span>
          </footer>
        </main>
      </div>
      <DecisionInspector
        selected={selectedDecision}
        demo={demo}
        onClose={() => setSelectedDecision(null)}
      />
      <AboutDialog open={aboutOpen} onClose={() => setAboutOpen(false)} />
    </div>
  );
}
