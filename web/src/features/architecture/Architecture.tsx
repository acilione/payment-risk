import {
  ArrowRight,
  ArrowUpRight,
  CheckCheck,
  Clock3,
  Database,
  Gauge,
  GitBranch,
  Layers3,
  Radio,
  ShieldCheck,
  SlidersHorizontal,
  Terminal,
} from "lucide-react";
import type { Overview } from "../../types";
import { formatNumber } from "../../lib/format";
import { documentation } from "../../app/config";

export function Architecture({
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
            href={`${documentation}#data-model`}
            target="_blank"
            rel="noreferrer"
          >
            Data model <ArrowUpRight size={14} />
          </a>
        </div>
        <div className="architecture-flow">
          {[
            {
              icon: Radio,
              step: "01",
              name: "Kafka",
              detail: "Incoming payments",
            },
            {
              icon: Layers3,
              step: "02",
              name: "Apache Flink",
              detail: "Payment ordering and risk scoring",
            },
            {
              icon: ShieldCheck,
              step: "03",
              name: "Decision log",
              detail: "Transactional Kafka output",
            },
            {
              icon: Database,
              step: "04",
              name: "ClickHouse",
              detail: "Dashboard queries",
            },
          ].map(({ icon: Icon, step, name, detail }, index) => {
            return (
              <div className="architecture-node" key={name}>
                <span className="node-step">{step}</span>
                <Icon size={28} />
                <h3>{name}</h3>
                <p>{detail}</p>
                {index < 3 && <ArrowRight size={20} className="node-arrow" />}
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
          {
            icon: Clock3,
            title: "Event ordering",
            text: "Payments are ordered by event time with a 10-second allowance for out-of-order arrivals. Late payments are sent to a separate output. Pending payments remain buffered when the stream is idle.",
          },
          {
            icon: CheckCheck,
            title: "Delivery and duplicates",
            text: "Flink checkpoints coordinate state with Kafka transactions. Consumers read committed decisions. Repeated event IDs are filtered for 24 hours.",
          },
          {
            icon: SlidersHorizontal,
            title: "Rule updates",
            text: "Kafka distributes rule updates to all risk workers. Older rule versions are rejected. Each payment keeps the rule settings recorded when it entered processing.",
          },
          {
            icon: Gauge,
            title: "Processing latency",
            text: "Time to finalize a decision includes waiting for event ordering. Time to publish it also includes waiting for a checkpoint. Rule evaluation time is measured separately.",
          },
        ].map(({ icon: Icon, title, text }) => {
          return (
            <section className="card architecture-principle" key={title}>
              <Icon size={23} />
              <h2>{title}</h2>
              <p>{text}</p>
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
              ? `${data.checkpoint.duration} ms duration · ${formatNumber(data.checkpoint.bytes)} state bytes`
              : "Start a local Flink job to see checkpoint details."}
          </p>
        </div>
        <a
          className="button dark"
          href={`${documentation}#operations`}
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
