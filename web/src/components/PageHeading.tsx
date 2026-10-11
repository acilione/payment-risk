import { ArrowRight, ArrowUpRight, Database, GitFork } from "lucide-react";
import { github, staticDemo, openLocalDashboard } from "../app/config";
import { pages, type Page } from "../app/navigation";

export function PageHeading({
  activePage,
  demo,
  onChangeMode,
}: {
  activePage: Page;
  demo: boolean;
  onChangeMode: () => void;
}) {
  return (
    <>
      <div className="page-heading">
        <div>
          <h1>{activePage}</h1>
          <p>{pages.find((page) => page.name === activePage)?.description}</p>
        </div>
        <div className="heading-actions">
          <button
            className="button secondary"
            onClick={staticDemo ? openLocalDashboard : onChangeMode}
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
            <strong>Saved pipeline run</strong>
            <span>
              Generated payments from a verified Kafka, Flink and ClickHouse
              run.
            </span>
          </span>
          <button onClick={staticDemo ? openLocalDashboard : onChangeMode}>
            {staticDemo ? "Open local" : "Use live data"}{" "}
            <ArrowRight size={14} />
          </button>
        </div>
      )}
    </>
  );
}
