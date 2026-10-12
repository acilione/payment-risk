import { Activity, Code2, ExternalLink, X } from "lucide-react";
import { documentation } from "../app/config";
import { Modal } from "./Modal";

export function AboutDialog({
  open,
  onClose,
}: {
  open: boolean;
  onClose: () => void;
}) {
  return (
    <Modal
      open={open}
      onClose={onClose}
      className="help-dialog"
      label="About Payment risk"
    >
      <section className="help-modal card">
        <button
          className="icon-button help-close"
          onClick={() => onClose()}
          aria-label="Close about"
        >
          <X size={20} />
        </button>
        <span className="brand-mark">
          <Activity size={28} />
        </span>
        <h2>About Payment risk</h2>
        <p>
          This dashboard shows risk assessments from an Apache Flink job. Use it
          to review transaction scores, matched rules, and service status.
        </p>
        <p>
          <strong>Live mode</strong> reads data from your local services.{" "}
          <strong>Demo mode</strong> includes investigations exported from a
          verified generator run through the pipeline and does not require those
          services.
        </p>
        <a
          className="button dark"
          href={documentation}
          target="_blank"
          rel="noreferrer"
        >
          <Code2 size={16} />
          Read documentation
          <ExternalLink size={14} />
        </a>
      </section>
    </Modal>
  );
}
