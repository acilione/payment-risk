import { useState } from "react";
import { Copy } from "lucide-react";
const commands =
  "make showcase\n# Open http://localhost:23001\nmake showcase-stop\n# Edit config/showcase.json, then run again";
export function RunInstructions() {
  const [copied, setCopied] = useState(false);
  return (
    <section className="card showcase-replay">
      <div>
        <h2>Run it locally</h2>
        <p>
          Edit <code>config/showcase.json</code> to change the seed, volume,
          amounts and behavior mix. Start a fresh run, then follow its payments
          in the dashboard.
        </p>
        <p className="showcase-note">
          History is published in chronological order on one Kafka partition.
          This presentation workflow is not a distributed throughput benchmark.
          Stopping retains data volumes and a Flink savepoint.
        </p>
      </div>
      <div>
        <button
          className="button secondary"
          onClick={() => {
            void navigator.clipboard
              .writeText(commands)
              .then(() => setCopied(true))
              .catch(() => setCopied(false));
          }}
        >
          <Copy size={14} />
          {copied ? "Copied" : "Copy commands"}
        </button>
        <pre>{commands}</pre>
      </div>
    </section>
  );
}
