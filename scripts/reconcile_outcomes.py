"""Match persisted side outputs to the acknowledged ledger without inferring missing outcomes."""
import json


def reconcile_outcomes(db, report, sql):
    db.execute("UPDATE inputs SET invalid=0")
    db.execute("UPDATE payments SET late=NULL")
    invalid_examples, late_examples = [], []
    partition, offset = -1, -1
    while True:
        rows = sql("""SELECT (payload->>'input_partition')::int AS input_partition,(payload->>'input_offset')::bigint AS input_offset,
            min(payload->>'payload_sha256') AS hash,min(payload->>'reason') AS reason_value,
            count(DISTINCT (payload->>'payload_sha256',payload->>'reason')) AS variants
            FROM risk.outcomes WHERE source_topic={topic:String} AND payload->>'outcome'='INVALID'
            AND payload->>'input_topic'={input:String} AND ((payload->>'input_partition')::int,(payload->>'input_offset')::bigint)>({partition:Int64},{offset:Int64})
            GROUP BY 1,2 ORDER BY 1,2 LIMIT 200""",
            topic=report.get("dlq_topic", report["run_id"] + ".dlq"), input=report["input_topic"], partition=partition, offset=offset)
        if not rows:
            break
        for row in rows:
            partition, offset = int(row["input_partition"]), int(row["input_offset"])
            expected = db.execute("SELECT hash,kind FROM inputs WHERE partition=? AND offset=?", (partition, offset)).fetchone()
            if expected is None or expected[0] != row["hash"] or int(row["variants"]) != 1 or expected[1] == "marker":
                raise AssertionError("Invalid-input evidence is unexpected or conflicts with the ledger")
            db.execute("UPDATE inputs SET invalid=1 WHERE partition=? AND offset=?", (partition, offset))
            if len(invalid_examples) < 10:
                invalid_examples.append({"partition": partition, "offset": str(offset), "reason": row.get("reason_value", "")})
    after = ""
    while True:
        rows = sql("""SELECT payload->>'event_id' AS event_id,min(payload->>'input_sha256') AS hash,min(payload->>'reason') AS reason_value,
            min((payload->>'watermark')::bigint) AS cutoff,min((payload->>'lateness_ms')::bigint) AS delay_ms,
            count(DISTINCT (payload->>'input_sha256',payload->>'reason',payload->>'watermark',payload->>'lateness_ms')) AS variants
            FROM risk.outcomes WHERE source_topic={topic:String} AND payload->>'outcome'='LATE'
            AND starts_with(payload->>'event_id',{prefix:String}) AND payload->>'event_id'>{after:String}
            GROUP BY 1 ORDER BY 1 LIMIT 200""",
            topic=report.get("late_topic", report["run_id"] + ".late"), prefix=report["run_id"] + "-payment-", after=after)
        if not rows:
            break
        for row in rows:
            after = row["event_id"]
            expected = db.execute("SELECT hash,decision,late FROM payments WHERE id=?", (after,)).fetchone()
            if expected is None or expected[0] != row["hash"] or expected[1] is not None or expected[2] is not None or int(row["variants"]) != 1:
                raise AssertionError("Late-input evidence is unexpected, duplicated or conflicts with an assessment")
            db.execute("UPDATE payments SET late=? WHERE id=?", (json.dumps(row), after))
            if len(late_examples) < 10:
                late_examples.append({"event_id": after, "reason": row["reason_value"], "lateness_ms": int(row["delay_ms"])})
    # One business event cannot be both rejected by validation and evaluated/late.
    if db.execute("""SELECT count(*) FROM payments p WHERE (decision IS NOT NULL OR late IS NOT NULL)
        AND EXISTS (SELECT 1 FROM inputs i WHERE i.event_id=p.id AND i.invalid=1)""").fetchone()[0]:
        raise AssertionError("Input has contradictory processing outcomes")
    # No outcome is inferred from a missing assessment. Transport copies link to the same business result.
    unresolved = """FROM inputs i LEFT JOIN payments p ON p.id=i.event_id
        WHERE i.kind!='marker' AND i.invalid=0 AND p.decision IS NULL AND p.late IS NULL"""
    return {
        "invalid_input_examples": invalid_examples,
        "late_payment_examples": late_examples,
        "unidentified_inputs": db.execute("SELECT count(*) FROM inputs WHERE kind!='marker' AND event_id=''").fetchone()[0],
        "invalid_inputs": db.execute("SELECT count(*) FROM inputs WHERE invalid=1").fetchone()[0],
        "invalid_payments": db.execute("""SELECT count(*) FROM payments p WHERE decision IS NULL AND late IS NULL
            AND EXISTS(SELECT 1 FROM inputs i WHERE i.event_id=p.id AND i.invalid=1)
            AND NOT EXISTS(SELECT 1 FROM inputs i WHERE i.event_id=p.id AND i.invalid=0)""").fetchone()[0],
        "late_payments": db.execute("SELECT count(*) FROM payments WHERE late IS NOT NULL").fetchone()[0],
        "unresolved_inputs": db.execute("SELECT count(*) " + unresolved).fetchone()[0],
        "unresolved_input_examples": [{"partition": p, "offset": str(o)} for p,o in db.execute("SELECT i.partition,i.offset " + unresolved + " ORDER BY i.partition,i.offset LIMIT 10")],
        "unresolved_payment_examples": [row[0] for row in db.execute("SELECT DISTINCT i.event_id " + unresolved + " AND i.event_id!='' ORDER BY i.event_id LIMIT 10")],
        "control_records": db.execute("SELECT count(*) FROM inputs WHERE kind='marker'").fetchone()[0],
    }
