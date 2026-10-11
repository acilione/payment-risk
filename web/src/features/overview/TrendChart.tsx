import type { Overview } from "../../types";
import { formatNumber, formatTime } from "../../lib/format";
import { useState } from "react";

export function TrendChart({ series }: { series: Overview["series"] }) {
  const [hover, setHover] = useState<number | null>(null);
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
            {series.map((_point, i) => (
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
                {formatTime(series[i].time).slice(0, 5)}
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
          <strong>{formatTime(series[hover].time)}</strong>
          <span>
            {formatNumber(series[hover].approved)} approved ·{" "}
            {series[hover].review} review · {series[hover].rejected} rejected
          </span>
        </div>
      )}
      {!series.length && (
        <div className="chart-empty">No decisions in this time window</div>
      )}
    </div>
  );
}
