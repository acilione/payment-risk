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
              <dt>
                {check.unresolved_inputs === undefined
                  ? "Payments without a stored assessment"
                  : "Inputs without a known outcome"}
              </dt>
              <dd>
                {(
                  check.unresolved_inputs ?? check.without_decision
                ).toLocaleString("en-GB")}
              </dd>
            </div>
          </dl>
          {check.unresolved_inputs !== undefined && (
            <dl className="reconciliation-counts outcome-counts">
              <div>
                <dt>Evaluated payments</dt>
                <dd>{check.evaluated.toLocaleString("en-GB")}</dd>
              </div>
              <div>
                <dt>Invalid input records</dt>
                <dd>{(check.invalid_inputs ?? 0).toLocaleString("en-GB")}</dd>
              </div>
              <div>
                <dt>Late payments</dt>
                <dd>{(check.late_payments ?? 0).toLocaleString("en-GB")}</dd>
              </div>
            </dl>
          )}
          {!!(
            (check.invalid_input_examples?.length ?? 0) +
            (check.late_payment_examples?.length ?? 0)
          ) && (
            <details>
              <summary>Invalid and late input evidence</summary>
              <ul>
                {check.invalid_input_examples?.map((row) => (
                  <li key={`invalid:${row.partition}:${row.offset}`}>
                    Invalid input at partition {row.partition}, offset{" "}
                    {row.offset}: <code>{row.reason}</code>.
                  </li>
                ))}
                {check.late_payment_examples?.map((row) => (
                  <li key={row.event_id}>
                    Late payment <code>{row.event_id}</code>:{" "}
                    {row.lateness_ms.toLocaleString("en-GB")} ms behind the
                    cutoff (<code>{row.reason}</code>).
                  </li>
                ))}
              </ul>
            </details>
          )}
          {!!check.unresolved_input_examples?.length && (
            <details>
              <summary>Inputs requiring follow-up</summary>
              <ul>
                {check.unresolved_input_examples.map((row) => (
                  <li key={`${row.partition}:${row.offset}`}>
                    <code>
                      Partition {row.partition}, offset {row.offset}
                    </code>
                  </li>
                ))}
              </ul>
            </details>
          )}
          <p>
            {check.verified
              ? "Input coordinates and payload hashes were compared with the generator ledger. Assessment identities and input hashes were checked separately. Invalid and late counts, when present, require matching stored side-output evidence."
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
        This check covers this generator run. Invalid inputs failed validation;
        late payments arrived after the event-time cutoff. Neither is a fraud
        classification. Unresolved inputs need investigation. Control records
        require archiving only.
      </p>
    </section>
  );
}
