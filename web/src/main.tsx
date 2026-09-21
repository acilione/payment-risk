import { StrictMode, useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  Activity,
  ArrowDownLeft,
  ArrowRight,
  ArrowUpRight,
  Check,
  CheckCheck,
  ChevronDown,
  ChevronRight,
  CircleHelp,
  Clock3,
  Code2,
  Database,
  ExternalLink,
  Filter,
  Gauge,
  GitBranch,
  GitFork,
  Layers3,
  LayoutDashboard,
  Menu,
  Radio,
  RefreshCw,
  Search,
  ShieldCheck,
  SlidersHorizontal,
  Terminal,
  Wallet,
  Workflow,
  X,
} from "lucide-react";
import type { Decision, Overview, WindowKey } from "./types";
import { demoOverview, ruleCatalog } from "./demo";
import "./style.css";

const staticDemo = import.meta.env.VITE_DEMO_MODE === "true";
const github = "https://github.com/acilione/payment-risk";
const technicalGuide = `${github}/blob/main/docs/technical-guide.md`;
const number = (n: number) => new Intl.NumberFormat("en-GB").format(n);
const money = (n: number) =>
  new Intl.NumberFormat("en-IE", {
    style: "currency",
    currency: "EUR",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(n / 100);
const time = (n: number) =>
  new Date(n).toLocaleTimeString("en-GB", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
const short = (s: string) =>
  s.length > 24 ? `${s.slice(0, 12)}…${s.slice(-7)}` : s;
const label = { APPROVE: "Approved", REVIEW: "Review", REJECT: "Rejected" };

function App() {
  const [tab, setTab] = useState("Overview");
  const [demo, setDemo] = useState(import.meta.env.VITE_DEMO_MODE === "true");
  const [window, setWindow] = useState<WindowKey>("24h");
  const [data, setData] = useState<Overview | null>(() =>
    import.meta.env.VITE_DEMO_MODE === "true" ? demoOverview("24h") : null,
  );
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [refresh, setRefresh] = useState(0);
  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState("ALL");
  const [selected, setSelected] = useState<Decision | null>(null);
  const [menu, setMenu] = useState(false);
  const [mobile, setMobile] = useState(
    () => matchMedia("(max-width: 760px)").matches,
  );
  useEffect(() => {
    const media = matchMedia("(max-width: 760px)");
    const resize = () => setMobile(media.matches);
    const escape = (event: KeyboardEvent) => {
      if (event.key === "Escape") setMenu(false);
    };
    media.addEventListener("change", resize);
    globalThis.addEventListener("keydown", escape);
    return () => {
      media.removeEventListener("change", resize);
      globalThis.removeEventListener("keydown", escape);
    };
  }, []);
  const [help, setHelp] = useState(false);
  const [hover, setHover] = useState<number | null>(null);
  const dialog = useRef<HTMLDialogElement>(null);
  const helpDialog = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    if (help) helpDialog.current?.showModal();
    else helpDialog.current?.close();
  }, [help]);
  useEffect(() => {
    if (demo) {
      setData(demoOverview(window));
      setError("");
      setLoading(false);
      return;
    }
    let active = true;
    let controller: AbortController;
    const read = async () => {
      controller = new AbortController();
      setLoading(true);
      try {
        const response = await fetch(
          `${import.meta.env.BASE_URL}api/overview?window=${window}`,
          {
            signal: AbortSignal.any([
              controller.signal,
              AbortSignal.timeout(12000),
            ]),
            cache: "no-store",
          },
        );
        if (
          !response.ok ||
          !response.headers.get("content-type")?.includes("application/json")
        )
          throw new Error("Unavailable");
        const result: Overview = await response.json();
        if (active) {
          setData(result);
          setError("");
        }
      } catch (e) {
        if (active && !(e instanceof DOMException && e.name === "AbortError"))
          setError(
            "Live data is unavailable. Start the local services or switch to demo data.",
          );
      } finally {
        if (active) setLoading(false);
      }
    };
    void read();
    const interval = globalThis.setInterval(() => void read(), 15000);
    return () => {
      active = false;
      controller?.abort();
      clearInterval(interval);
    };
  }, [demo, window, refresh]);
  useEffect(() => {
    if (selected) dialog.current?.showModal();
    else dialog.current?.close();
  }, [selected]);
  const changeMode = () => {
    setData(null);
    setSelected(null);
    setDemo(!demo);
  };
  const totals = data?.totals;
  const total = totals?.transactions || 0;
  const approval = total
    ? ((totals!.approved / total) * 100).toFixed(1)
    : "0.0";
  const flagged = (totals?.review || 0) + (totals?.rejected || 0);
  const decisions = (data?.decisions || []).filter(
    (d) =>
      (filter === "ALL" || d.decision === filter) &&
      `${d.transaction_id} ${d.customer_id} ${d.matched_rules.join(" ")}`
        .toLowerCase()
        .includes(search.toLowerCase()),
  );
  const healthy = data?.services.every((s) => s.status === "healthy");
  const nav = (name: string) => {
    setTab(name);
    setMenu(false);
  };
  return (
    <div className="app-shell">
      <aside
        id="sidebar"
        className={`sidebar ${menu ? "open" : ""}`}
        inert={mobile && !menu}
      >
        {mobile && menu && (
          <button
            className="sidebar-close icon-button"
            onClick={() => setMenu(false)}
            aria-label="Close navigation"
          >
            <X size={18} />
          </button>
        )}
        <a
          className="brand"
          href="#"
          onClick={(e) => {
            e.preventDefault();
            nav("Overview");
          }}
          aria-label="Payment risk home"
        >
          <span className="brand-mark">
            <Activity size={25} />
          </span>
          <span>Payment risk</span>
        </a>
        <div className="nav-label">DASHBOARD</div>
        <nav aria-label="Main navigation">
          {[
            [LayoutDashboard, "Overview"],
            [ArrowDownLeft, "Transactions"],
            [ShieldCheck, "Risk rules"],
            [Workflow, "Architecture"],
          ].map(([Icon, name]) => {
            const I = Icon as typeof Activity;
            const n = name as string;
            return (
              <button
                key={n}
                className={`nav-item ${tab === n ? "active" : ""}`}
                onClick={() => nav(n)}
              >
                <I size={18} />
                {n}
                {n === "Risk rules" && <span className="nav-count">05</span>}
              </button>
            );
          })}
        </nav>
        <div className="nav-label nav-resources">RESOURCES</div>
        <a
          className="nav-item"
          href={technicalGuide}
          target="_blank"
          rel="noreferrer"
        >
          <Code2 size={18} />
          Documentation
          <ArrowUpRight size={14} className="nav-out" />
        </a>
        <a className="nav-item" href={github} target="_blank" rel="noreferrer">
          <GitFork size={18} />
          Source code
          <ArrowUpRight size={14} className="nav-out" />
        </a>
        <div className="sidebar-bottom">
          <button className="profile" onClick={() => setHelp(true)}>
            <span className="avatar">AC</span>
            <span>
              Antonino Cilione<small>Project maintainer</small>
            </span>
            <CircleHelp size={16} />
          </button>
        </div>
      </aside>
      <div className="main-shell">
        <header className="topbar">
          <div className="breadcrumbs">
            <button
              className="mobile-menu icon-button"
              onClick={() => setMenu(!menu)}
              aria-label="Toggle navigation"
              aria-expanded={menu}
              aria-controls="sidebar"
            >
              <Menu size={20} />
            </button>
            <span>Dashboard</span>
            <ChevronRight size={13} />
            <strong>{tab}</strong>
          </div>
          <div className="topbar-right">
            <span className={`connection ${error ? "offline" : ""}`}>
              <i />
              {demo
                ? "Demo data"
                : error
                  ? "Connection unavailable"
                  : loading && !data
                    ? "Connecting"
                    : "Live data"}
            </span>
            <span className="topbar-divider" />
            <button
              className="icon-button"
              onClick={() => setHelp(true)}
              aria-label="About this project"
            >
              <CircleHelp size={18} />
            </button>
            <span className="avatar avatar-small">AC</span>
          </div>
        </header>
        <main>
          <div className="page-heading">
            <div>
              <h1>{tab}</h1>
              <p>
                {tab === "Overview"
                  ? "Payment totals, risk decisions, and service status."
                  : tab === "Transactions"
                    ? "Search payment decisions and review their scores and matched rules."
                    : tab === "Risk rules"
                      ? "Default rules, score weights, and decision thresholds."
                      : "How payments are processed, stored, and recovered after a restart."}
              </p>
            </div>
            <div className="heading-actions">
              <button
                className="button secondary"
                onClick={
                  staticDemo
                    ? () =>
                        globalThis.open(
                          "http://localhost:23001",
                          "_blank",
                          "noopener,noreferrer",
                        )
                    : changeMode
                }
              >
                <Database size={15} />
                {staticDemo
                  ? "Open local dashboard"
                  : demo
                    ? "Use live data"
                    : "View demo"}
              </button>
              <a
                className="button dark"
                href={github}
                target="_blank"
                rel="noreferrer"
              >
                <GitFork size={15} />
                View source
                <ArrowUpRight size={14} />
              </a>
            </div>
          </div>
          {demo && (
            <div className="demo-banner">
              <span>
                <Database size={15} />
                <strong>Demo data</strong>
                <span>
                  Transactions, charts, and service status use sample data.
                </span>
              </span>
              <button
                onClick={
                  staticDemo
                    ? () =>
                        globalThis.open(
                          "http://localhost:23001",
                          "_blank",
                          "noopener,noreferrer",
                        )
                    : changeMode
                }
              >
                {staticDemo ? "Open local" : "Use live data"}{" "}
                <ArrowRight size={14} />
              </button>
            </div>
          )}
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
              <button
                className="button secondary"
                onClick={() => setRefresh(refresh + 1)}
              >
                Retry
              </button>
            </div>
          )}
          {(tab === "Overview" || tab === "Transactions") && (
            <>
              <div className="section-toolbar">
                <div className="section-title">
                  <span className="live-dot" />{" "}
                  {tab === "Overview" ? "Risk overview" : "Payment decisions"}
                  <span className="subtle">/ EUR payments</span>
                </div>
                <div className="time-controls">
                  <span className="updated">
                    {data
                      ? `Updated ${time(Date.parse(data.generatedAt))}`
                      : "Waiting for data"}
                  </span>
                  <button
                    className={`icon-button ${loading ? "spinning" : ""}`}
                    disabled={loading}
                    onClick={() =>
                      demo
                        ? setData(demoOverview(window))
                        : setRefresh(refresh + 1)
                    }
                    aria-label="Refresh data"
                  >
                    <RefreshCw size={15} />
                  </button>
                  <label className="window-select">
                    <Clock3 size={14} />
                    <select
                      aria-label="Time window"
                      value={window}
                      onChange={(e) => (
                        setData(null),
                        setWindow(e.target.value as WindowKey)
                      )}
                    >
                      <option value="15m">Last 15 minutes</option>
                      <option value="1h">Last hour</option>
                      <option value="24h">Last 24 hours</option>
                    </select>
                    <ChevronDown size={13} />
                  </label>
                </div>
              </div>
              <section className="stats-grid" aria-label="Risk summary">
                <Stat
                  title="Transactions evaluated"
                  value={totals ? number(total) : "—"}
                  detail="Unique transactions"
                  icon={<Activity size={18} />}
                  chart="coral"
                />
                <Stat
                  title="Payment volume"
                  value={totals ? money(totals.amountMinor) : "—"}
                  detail="Evaluated amount · EUR"
                  icon={<Wallet size={18} />}
                  chart="neutral"
                />
                <Stat
                  title="Approval rate"
                  value={totals ? `${approval}%` : "—"}
                  detail={`${number(totals?.approved || 0)} payments approved`}
                  icon={<ShieldCheck size={18} />}
                  chart="green"
                />
                <Stat
                  title="Review or rejected"
                  value={totals ? number(flagged) : "—"}
                  detail={`${number(totals?.review || 0)} review · ${number(totals?.rejected || 0)} rejected`}
                  icon={<Filter size={18} />}
                  chart="amber"
                />
              </section>
            </>
          )}
          {tab === "Overview" && (
            <>
              <div className="charts-grid">
                <section className="card trend-card">
                  <div className="card-heading">
                    <div>
                      <h2>Decisions over time</h2>
                      <p>Payment counts by decision and time interval</p>
                    </div>
                    <span className="chip">
                      {window === "1h"
                        ? "2 min"
                        : window === "15m"
                          ? "30 sec"
                          : "48 min"}{" "}
                      buckets
                    </span>
                  </div>
                  <div className="chart-summary">
                    <strong>{number(total)}</strong>
                    <span>decisions in this window</span>
                    <div className="legend">
                      <span>
                        <i className="green" />
                        Approved
                      </span>
                      <span>
                        <i className="amber" />
                        Review
                      </span>
                      <span>
                        <i className="coral" />
                        Rejected
                      </span>
                    </div>
                  </div>
                  <Trend
                    series={data?.series || []}
                    hover={hover}
                    setHover={setHover}
                  />
                </section>
                <section className="card distribution">
                  <div className="card-heading">
                    <div>
                      <h2>Decision breakdown</h2>
                      <p>Share of approved, review, and rejected payments</p>
                    </div>
                    <ShieldCheck size={18} className="muted" />
                  </div>
                  <div className="donut-wrap">
                    <svg
                      viewBox="0 0 180 180"
                      role="img"
                      aria-label={`${approval}% approved`}
                    >
                      <circle
                        cx="90"
                        cy="90"
                        r="65"
                        fill="none"
                        stroke="#f0efeb"
                        strokeWidth="19"
                      />
                      {total > 0 &&
                        [
                          totals!.approved,
                          totals!.review,
                          totals!.rejected,
                        ].map((count, i, all) => (
                          <circle
                            key={i}
                            cx="90"
                            cy="90"
                            r="65"
                            fill="none"
                            stroke={["#46866c", "#ddae62", "#e68063"][i]}
                            strokeWidth="19"
                            strokeDasharray={`${(count / total) * 408.4} 408.4`}
                            strokeDashoffset={
                              (-all.slice(0, i).reduce((a, b) => a + b, 0) /
                                total) *
                              408.4
                            }
                            transform="rotate(-90 90 90)"
                          />
                        ))}
                      <text
                        x="90"
                        y="88"
                        textAnchor="middle"
                        className="donut-value"
                      >
                        {approval}%
                      </text>
                      <text
                        x="90"
                        y="109"
                        textAnchor="middle"
                        className="donut-label"
                      >
                        APPROVED
                      </text>
                    </svg>
                  </div>
                  <div className="breakdown-rows">
                    {[
                      ["Approved", totals?.approved || 0, "green"],
                      ["Review", totals?.review || 0, "amber"],
                      ["Rejected", totals?.rejected || 0, "coral"],
                    ].map(([name, n, color]) => (
                      <div key={name as string}>
                        <span>
                          <i className={`dot ${color}`} />
                          {name}
                        </span>
                        <strong>{number(n as number)}</strong>
                        <small>
                          {total
                            ? (((n as number) / total) * 100).toFixed(1)
                            : "0.0"}
                          %
                        </small>
                      </div>
                    ))}
                  </div>
                </section>
              </div>
              <section className="pipeline-strip">
                <div className="pipeline-intro">
                  <span className="pipeline-icon">
                    <Workflow size={20} />
                  </span>
                  <div>
                    <strong>Service status</strong>
                    <small>
                      {demo
                        ? "Sample service status"
                        : healthy
                          ? "All monitored services available"
                          : "Check service availability"}
                    </small>
                  </div>
                </div>
                <div className="pipeline-services">
                  {data?.services.map((s, i) => (
                    <div className="service-wrap" key={s.name}>
                      <div className="service" title={s.detail}>
                        <i className={`status-dot ${s.status}`} />
                        <span>{s.name}</span>
                        {s.status === "healthy" && <Check size={12} />}
                      </div>
                      {i < 3 && <span className="service-link">···</span>}
                    </div>
                  )) || (
                    <span className="muted">Waiting for service status</span>
                  )}
                </div>
                <button
                  className="text-button"
                  onClick={() => nav("Architecture")}
                >
                  View architecture <ArrowUpRight size={14} />
                </button>
              </section>
            </>
          )}
          {(tab === "Overview" || tab === "Transactions") && (
            <div className={tab === "Overview" ? "bottom-grid" : ""}>
              <section className="card transactions-card">
                <div className="card-heading">
                  <div>
                    <h2>
                      Recent decisions{" "}
                      <span className="heading-count">{decisions.length}</span>
                    </h2>
                    <p>
                      Select a transaction to view its score and matched rules
                    </p>
                  </div>
                  {tab === "Overview" && (
                    <button
                      className="text-button"
                      onClick={() => nav("Transactions")}
                    >
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
                      onChange={(e) => setSearch(e.target.value)}
                    />
                    {search && (
                      <button
                        className="icon-button"
                        onClick={() => setSearch("")}
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
                      onChange={(e) => setFilter(e.target.value)}
                    >
                      <option value="ALL">All decisions</option>
                      <option value="APPROVE">Approved</option>
                      <option value="REVIEW">Review</option>
                      <option value="REJECT">Rejected</option>
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
                      {decisions
                        .slice(0, tab === "Overview" ? 6 : 100)
                        .map((d) => (
                          <tr key={d.transaction_id}>
                            <td>
                              <button
                                className="transaction-link"
                                onClick={() => setSelected(d)}
                              >
                                <span
                                  className={`transaction-icon ${d.decision.toLowerCase()}`}
                                >
                                  <ArrowUpRight size={15} />
                                </span>
                                <span>
                                  <strong>
                                    {short(
                                      d.transaction_id.replace("txn_", ""),
                                    )}
                                  </strong>
                                  <small>{short(d.customer_id)}</small>
                                </span>
                              </button>
                            </td>
                            <td className="amount">{money(d.amount_minor)}</td>
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
                              <span
                                className={`badge ${d.decision.toLowerCase()}`}
                              >
                                <i />
                                {label[d.decision]}
                              </span>
                            </td>
                            <td className="table-time">
                              {time(d.processed_at)}
                            </td>
                            <td>
                              <button
                                className="icon-button"
                                onClick={() => setSelected(d)}
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
                        {data
                          ? "No matching decisions"
                          : "Waiting for payment data"}
                      </strong>
                      <p>
                        {data
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
                      ? "Sample transactions"
                      : "One decision per transaction"}
                  </span>
                  <span>
                    Latest{" "}
                    {Math.min(decisions.length, tab === "Overview" ? 6 : 100)}{" "}
                    of {decisions.length} loaded
                  </span>
                </div>
              </section>
              {tab === "Overview" && (
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
                    {ruleCatalog.map((r) => {
                      const count =
                        data?.rules.find((v) => v.id === r.id)?.matches || 0;
                      const max = Math.max(
                        1,
                        ...(data?.rules.map((v) => v.matches) || []),
                      );
                      return (
                        <button
                          key={r.id}
                          className="rule-row"
                          onClick={() => nav("Risk rules")}
                        >
                          <span className="rule-number">{r.id}</span>
                          <div>
                            <span>
                              {r.name}
                              <strong>{number(count)}</strong>
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
                    <span>
                      Select a transaction to see the rules used in its score.
                    </span>
                  </div>
                </section>
              )}
            </div>
          )}
          {tab === "Risk rules" && (
            <>
              <div className="policy-note">
                <ShieldCheck size={22} />
                <div>
                  <strong>Default risk rules</strong>
                  <p>
                    These are the default rule settings. Live updates arrive
                    through Kafka. Each decision includes a fingerprint of the
                    settings used to calculate its score.
                  </p>
                </div>
              </div>
              <div className="rule-catalog">
                {ruleCatalog.map((r) => (
                  <section className="card rule-detail" key={r.id}>
                    <span className="rule-number">{r.id}</span>
                    <span className="rule-weight">+{r.score} points</span>
                    <h2>{r.name}</h2>
                    <p>{r.description}</p>
                    <div className="rule-detail-footer">
                      <span>
                        {number(
                          data?.rules.find((v) => v.id === r.id)?.matches || 0,
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
                  The score is the sum of matched rule weights, capped at 100.
                  These are the default thresholds; they can be changed in the
                  deployment settings.
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
          )}
          {tab === "Architecture" && <Architecture data={data} demo={demo} />}
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
      <dialog
        ref={dialog}
        className="inspector"
        onCancel={() => setSelected(null)}
        onClick={(e) => {
          if (e.target === e.currentTarget) setSelected(null);
        }}
        aria-labelledby="inspector-title"
      >
        {selected && (
          <div className="inspector-body">
            <div className="inspector-top">
              <span className="eyebrow">PAYMENT DETAILS</span>
              <button
                className="icon-button"
                onClick={() => setSelected(null)}
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
                  {label[selected.decision]}
                </span>
              </div>
              <ShieldCheck size={42} />
            </div>
            <dl>
              <dt>Customer</dt>
              <dd>{selected.customer_id}</dd>
              <dt>Payment amount</dt>
              <dd>{money(selected.amount_minor)}</dd>
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
              This fingerprint identifies the rule settings used for this
              payment. They may differ from the current settings.
            </p>
            <div className="source-position">
              <Database size={15} />
              Kafka partition {selected.source_partition} · offset{" "}
              {selected.source_offset}
            </div>
          </div>
        )}
      </dialog>
      <dialog
        ref={helpDialog}
        className="help-dialog"
        aria-label="About Payment risk"
        onCancel={() => setHelp(false)}
        onClick={(e) => {
          if (e.target === e.currentTarget) setHelp(false);
        }}
      >
        <section className="help-modal card">
          <button
            className="icon-button help-close"
            onClick={() => setHelp(false)}
            aria-label="Close about"
          >
            <X size={20} />
          </button>
          <span className="brand-mark">
            <Activity size={28} />
          </span>
          <h2>About Payment risk</h2>
          <p>
            This dashboard shows payment decisions from an Apache Flink job. Use
            it to review transaction scores, matched rules, and service status.
          </p>
          <p>
            <strong>Live mode</strong> reads data from your local services.{" "}
            <strong>Demo mode</strong> uses sample data and does not require
            those services.
          </p>
          <a
            className="button dark"
            href={technicalGuide}
            target="_blank"
            rel="noreferrer"
          >
            <Code2 size={16} />
            Read documentation
            <ExternalLink size={14} />
          </a>
        </section>
      </dialog>
    </div>
  );
}

function Stat({
  title,
  value,
  detail,
  icon,
  chart,
}: {
  title: string;
  value: string;
  detail: string;
  icon: React.ReactNode;
  chart: string;
}) {
  return (
    <article className={`card stat-card ${chart}`}>
      <div className="stat-top">
        <span>{title}</span>
        <span className="stat-icon">{icon}</span>
      </div>
      <strong className="stat-value">{value}</strong>
      <div className="stat-bottom">
        <span>{detail}</span>
        <span className={`mini-chart ${chart}`} aria-hidden="true">
          {[35, 55, 42, 73, 62, 88, 100].map((height, i) => (
            <i key={i} style={{ height: `${height}%` }} />
          ))}
        </span>
      </div>
    </article>
  );
}
function Trend({
  series,
  hover,
  setHover,
}: {
  series: Overview["series"];
  hover: number | null;
  setHover: (n: number | null) => void;
}) {
  const max = Math.max(
    1,
    ...series.map((p) => p.approved + p.review + p.rejected),
  );
  const x = (i: number) => 45 + (i / Math.max(1, series.length - 1)) * 675;
  const y = (v: number) => 194 - (v / max) * 160;
  const path = (field: "approved" | "review" | "rejected") =>
    series
      .map((p, i) => `${i === 0 ? "M" : "L"}${x(i)},${y(p[field])}`)
      .join(" ");
  return (
    <div className="trend-chart">
      <svg
        viewBox="0 0 750 240"
        role="img"
        aria-label="Decisions over time"
        onMouseLeave={() => setHover(null)}
      >
        <defs>
          <linearGradient id="area" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#46866c" stopOpacity=".17" />
            <stop offset="100%" stopColor="#46866c" stopOpacity=".01" />
          </linearGradient>
        </defs>
        {[0, 1, 2, 3, 4].map((i) => (
          <g key={i}>
            <line
              x1="45"
              x2="720"
              y1={y((max / 4) * i)}
              y2={y((max / 4) * i)}
              stroke="#ecece6"
              strokeDasharray="3 4"
            />
            <text x="30" y={y((max / 4) * i) + 4} textAnchor="end">
              {Math.round((max / 4) * i)}
            </text>
          </g>
        ))}
        {series.length > 0 && (
          <>
            <path
              d={`${path("approved")} L720,194 L45,194Z`}
              fill="url(#area)"
            />
            {(["approved", "review", "rejected"] as const).map((f, i) => (
              <path
                key={f}
                d={path(f)}
                stroke={["#46866c", "#ddae62", "#e68063"][i]}
                fill="none"
                strokeWidth={i === 0 ? 2.5 : 1.8}
                strokeLinejoin="round"
                strokeLinecap="round"
              />
            ))}
            {series.map((p, i) => (
              <rect
                key={i}
                x={x(i) - 675 / series.length / 2}
                y="20"
                width={675 / series.length}
                height="180"
                fill="transparent"
                onMouseEnter={() => setHover(i)}
              />
            ))}
            {[
              ...new Set([
                0,
                Math.floor((series.length - 1) / 3),
                Math.floor(((series.length - 1) * 2) / 3),
                series.length - 1,
              ]),
            ].map((i) => (
              <text key={i} x={x(i)} y="225" textAnchor="middle">
                {time(series[i].time).slice(0, 5)}
              </text>
            ))}
          </>
        )}
        {hover !== null && series[hover] && (
          <g>
            <line
              x1={x(hover)}
              x2={x(hover)}
              y1="20"
              y2="195"
              stroke="#a9b7ac"
              strokeDasharray="4 4"
            />
            <circle
              cx={x(hover)}
              cy={y(series[hover].approved)}
              r="4"
              fill="#46866c"
              stroke="white"
              strokeWidth="2"
            />
          </g>
        )}
      </svg>
      {hover !== null && series[hover] && (
        <div className="chart-tooltip">
          <strong>{time(series[hover].time)}</strong>
          <span>
            {number(series[hover].approved)} approved · {series[hover].review}{" "}
            review · {series[hover].rejected} rejected
          </span>
        </div>
      )}
      {!series.length && (
        <div className="chart-empty">No decisions in this time window</div>
      )}
    </div>
  );
}
function Architecture({
  data,
  demo,
}: {
  data: Overview | null;
  demo: boolean;
}) {
  return (
    <>
      <section className="architecture-hero card">
        <div className="card-heading">
          <div>
            <h2>Payment processing</h2>
            <p>
              Flink reads payments from Kafka and writes risk decisions back to
              Kafka. ClickHouse stores those decisions for dashboard queries.
            </p>
          </div>
          <a
            className="text-button"
            href={`${technicalGuide}#data-model`}
            target="_blank"
            rel="noreferrer"
          >
            Data model <ArrowUpRight size={14} />
          </a>
        </div>
        <div className="architecture-flow">
          {[
            [Radio, "01", "Kafka", "Incoming payments"],
            [
              Layers3,
              "02",
              "Apache Flink",
              "Payment ordering and risk scoring",
            ],
            [ShieldCheck, "03", "Decision log", "Transactional Kafka output"],
            [Database, "04", "ClickHouse", "Dashboard queries"],
          ].map(([Icon, n, name, detail], i) => {
            const I = Icon as typeof Radio;
            return (
              <div className="architecture-node" key={name as string}>
                <span className="node-step">{n as string}</span>
                <I size={28} />
                <h3>{name as string}</h3>
                <p>{detail as string}</p>
                {i < 3 && <ArrowRight size={20} className="node-arrow" />}
              </div>
            );
          })}
        </div>
        <div className="architecture-state">
          <GitBranch size={19} />
          <span>
            <strong>State storage.</strong> Flink stores working state in
            RocksDB and checkpoints it to S3 or MinIO. Savepoints and stable
            operator IDs support upgrades that preserve compatible state.
          </span>
        </div>
      </section>
      <div className="architecture-grid">
        {[
          [
            Clock3,
            "Event ordering",
            "Payments are ordered by event time with a 10-second allowance for out-of-order arrivals. Late payments are sent to a separate output. Pending payments remain buffered when the stream is idle.",
          ],
          [
            CheckCheck,
            "Delivery and duplicates",
            "Flink checkpoints coordinate state with Kafka transactions. Consumers read committed decisions. Repeated event IDs are filtered for 24 hours.",
          ],
          [
            SlidersHorizontal,
            "Rule updates",
            "Kafka distributes rule updates to all risk workers. Older rule versions are rejected. Each payment keeps the rule settings recorded when it entered processing.",
          ],
          [
            Gauge,
            "Processing latency",
            "Time to finalize a decision includes waiting for event ordering. Time to publish it also includes waiting for a checkpoint. Rule evaluation time is measured separately.",
          ],
        ].map(([Icon, title, text]) => {
          const I = Icon as typeof Clock3;
          return (
            <section
              className="card architecture-principle"
              key={title as string}
            >
              <I size={23} />
              <h2>{title as string}</h2>
              <p>{text as string}</p>
            </section>
          );
        })}
      </div>
      <section className="card recovery-card">
        <div>
          <span className="eyebrow">
            {demo ? "SAMPLE CHECKPOINT" : "LATEST CHECKPOINT"}
          </span>
          <h2>
            {data?.checkpoint
              ? `Checkpoint #${data.checkpoint.id}`
              : "Waiting for checkpoint data"}
          </h2>
          <p>
            {data?.checkpoint
              ? `${data.checkpoint.duration} ms duration · ${number(data.checkpoint.bytes)} state bytes`
              : "Start a local Flink job to see checkpoint details."}
          </p>
        </div>
        <a
          className="button dark"
          href={`${technicalGuide}#operations`}
          target="_blank"
          rel="noreferrer"
        >
          <Terminal size={16} />
          Operations guide
          <ArrowUpRight size={15} />
        </a>
      </section>
    </>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
