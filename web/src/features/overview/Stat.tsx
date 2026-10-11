import type { ReactNode } from "react";
export function Stat({
  title,
  value,
  detail,
  icon,
  chart,
}: {
  title: string;
  value: string;
  detail: string;
  icon: ReactNode;
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
      </div>
    </article>
  );
}
