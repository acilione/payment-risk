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
              Flink assesses reported payment attempts from Kafka. Risk
              assessments and invalid or late records reach ClickHouse through
              committed Kafka outputs. A separate consumer archives every
              original input. This pipeline monitors payments; it does not
              authorize them.
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
              name: "Processing outputs",
              detail: "Assessments, invalid and late records",
            },
            {
              icon: Database,
              step: "04",
              name: "ClickHouse",
              detail: "Input archive and dashboard queries",
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
            text: "Payments are ordered by event time, with a default 10-second allowance for out-of-order arrivals. Late payments are stored separately. An idle stream does not force pending payments to finalize.",
          },
          {
            icon: CheckCheck,
            title: "Delivery and duplicates",
            text: "Flink checks event and transaction identities, with a default 24-hour retention. Checkpoints coordinate state with Kafka transactions. Database retries can create physical copies; logical views avoid double counting and block ambiguous results. Permanent input uniqueness is not provided.",
          },
          {
            icon: SlidersHorizontal,
            title: "Rule policies",
            text: "The showcase and non-local deployments use a frozen policy catalog. Local development also supports rule updates through Kafka. Each payment retains the rule settings used for its assessment.",
          },
          {
            icon: Database,
            title: "Input archive and outcomes",
            text: "An independent consumer stores original payment bytes and source coordinates even while Flink is unavailable. Database consumers commit offsets only after persistence. Reconciliation accounts for assessments, invalid inputs and late arrivals, and reports unresolved inputs.",
          },
          {
            icon: Layers3,
            title: "Customer state",
            text: "Pending payments are stored individually. Dense histories use exact rolling counts and sums; smaller histories use the reference calculation. Expiry indexes remove old history and device entries. State limits stop processing rather than discard required history.",
          },
          {
            icon: Gauge,
            title: "Processing latency",
            text: "Visible results include time spent waiting for event ordering, a completed checkpoint and database writes. Rule evaluation time is measured separately. The load reports distinguish these measurements.",
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
