import {
  Activity,
  ChevronDown,
  Clock3,
  Filter,
  RefreshCw,
  ShieldCheck,
  Wallet,
} from "lucide-react";
import type { Overview, WindowKey } from "../../types";
import { formatNumber, formatMoney, formatTime } from "../../lib/format";
import { Stat } from "./Stat";

interface OverviewSummaryProps {
  data: Overview | null;
  tab: "Overview" | "Transactions";
  window: WindowKey;
  loading: boolean;
  onRefresh: () => void;
  onWindowChange: (window: WindowKey) => void;
}
export function OverviewSummary({
  data,
  tab,
  window,
  loading,
  onRefresh,
  onWindowChange,
}: OverviewSummaryProps) {
  const totals = data?.totals;
  const total = totals?.transactions ?? 0;
  const approval =
    total && totals ? ((totals.approved / total) * 100).toFixed(1) : "0.0";
  const flagged = (totals?.review ?? 0) + (totals?.rejected ?? 0);
  return (
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
              ? `Updated ${formatTime(Date.parse(data.generatedAt))}`
              : "Waiting for data"}
          </span>
          <button
            className={`icon-button ${loading ? "spinning" : ""}`}
            disabled={loading}
            onClick={onRefresh}
            aria-label="Refresh data"
          >
            <RefreshCw size={15} />
          </button>
          <label className="window-select">
            <Clock3 size={14} />
            <select
              aria-label="Time window"
              value={window}
              onChange={(event) =>
                onWindowChange(event.target.value as WindowKey)
              }
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
          value={totals ? formatNumber(total) : "—"}
          detail="Unique transactions"
          icon={<Activity size={18} />}
          chart="coral"
        />
        <Stat
          title="Payment volume"
          value={totals ? formatMoney(totals.amountMinor) : "—"}
          detail="Evaluated amount · EUR"
          icon={<Wallet size={18} />}
          chart="neutral"
        />
        <Stat
          title="Approval rate"
          value={totals ? `${approval}%` : "—"}
          detail={`${formatNumber(totals?.approved || 0)} payments approved`}
          icon={<ShieldCheck size={18} />}
          chart="green"
        />
        <Stat
          title="Review or rejected"
          value={totals ? formatNumber(flagged) : "—"}
          detail={`${formatNumber(totals?.review || 0)} review · ${formatNumber(totals?.rejected || 0)} rejected`}
          icon={<Filter size={18} />}
          chart="amber"
        />
      </section>
    </>
  );
}
