import type { ShowcaseReport } from "./types";

export function Reconciliation({ report }: { report: ShowcaseReport }) {
  const check = report.reconciliation;
  const recovery = report.recovery;
  const unavailable =
    report.storage_unavailable ||
    recovery?.stage === "DATABASE_UNAVAILABLE" ||
    recovery?.stage === "BACKLOG_OBSERVED";
  return (
    <section
      className="card showcase-reconciliation"
      aria-label="Processing completeness"
    >
      <div className="card-heading">
        <h2>Processing completeness</h2>
        <span>
          {unavailable
            ? "Database unavailable"
            : check?.verified
              ? "IDs and hashes checked"
              : "Live counts"}
        </span>
      </div>
      {unavailable ? (
        <p role="status">
          Kafka is accepting payments while the database is unavailable. Storage
          counts cannot be checked until it recovers.
        </p>
      ) : check ? (
        <>
          <dl className="reconciliation-counts">
            <div>
              <dt>Inputs not yet archived</dt>
              <dd>{check.missing_archive.toLocaleString("en-GB")}</dd>
            </div>
            <div>
              <dt>Payments without a stored decision</dt>
              <dd>{check.without_decision.toLocaleString("en-GB")}</dd>
            </div>
          </dl>
          <p>
            {check.verified
              ? "Input coordinates and payload hashes were compared with the generator ledger. Decision identities and input hashes were checked separately."
              : "Counts are provisional. Completion requires comparing every acknowledged input and every generated payment with the ledger."}
          </p>
          {!!check.missing_payment_examples?.length && (
            <details>
              <summary>Payments requiring follow-up</summary>
              <ul>
                {check.missing_payment_examples.map((id) => (
                  <li key={id}>
                    <code>{id}</code>
                  </li>
                ))}
              </ul>
            </details>
          )}
          {!!check.missing_input_examples?.length && (
            <details>
              <summary>Input records not found in the archive</summary>
              <ul>
                {check.missing_input_examples.map((row) => (
                  <li key={`${row.partition}:${row.offset}`}>
                    <code>
                      Partition {row.partition}, offset {row.offset}
                    </code>
                  </li>
                ))}
              </ul>
            </details>
          )}
        </>
      ) : (
        <p>Waiting for the first storage check.</p>
      )}
      {recovery && (
        <div className="recovery-progress" role="status">
          <h3>Database recovery demonstration</h3>
          <p>
            {recovery.verification ||
              {
                DATABASE_UNAVAILABLE:
                  "The database has been stopped. Payment generation continues through Kafka and Flink.",
                BACKLOG_OBSERVED: `${recovery.acknowledged_during_outage ?? 0} deliveries acknowledged. Consumer offsets stayed unchanged. Restarting the database in ${recovery.resume_in_seconds ?? 0} seconds.`,
                RECOVERING:
                  "The database is restarting. Consumers are retrying the retained records.",
                REPLAYING:
                  "Storage has caught up. Replaying committed decisions to verify that retries do not change logical payment counts.",
                STORAGE_RECONCILED:
                  "Storage was reconciled with a later check. The recovery demonstration's replay step has not completed.",
              }[recovery.stage] ||
              "Checking recovery."}
          </p>
          {recovery.replay && (
            <p>
              {recovery.replay.records.toLocaleString("en-GB")} decision
              deliveries replayed.
            </p>
          )}
        </div>
      )}
      <p className="showcase-note">
        This check covers this generator run. A missing decision needs
        investigation; it is not automatically classified as pending, invalid or
        late.
      </p>
    </section>
  );
}
