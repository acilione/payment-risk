-- Authoritative initial assessments and incrementally maintained read models.
-- Kafka coordinates describe deliveries; they never define a business transaction.
CREATE SCHEMA IF NOT EXISTS risk;
CREATE TABLE IF NOT EXISTS risk.projection_status (
  id boolean PRIMARY KEY DEFAULT true CHECK (id),
  ready boolean NOT NULL DEFAULT false
);
INSERT INTO risk.projection_status VALUES (true, false) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS risk.deliveries (
  source_topic text NOT NULL, source_partition integer NOT NULL, source_offset bigint NOT NULL,
  kind text NOT NULL, payload jsonb NOT NULL,
  stored_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (source_topic, source_partition, source_offset)
);
CREATE TABLE IF NOT EXISTS risk.incidents (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  source_topic text NOT NULL, source_partition integer NOT NULL, source_offset bigint NOT NULL,
  reason text NOT NULL, payload jsonb NOT NULL, resolved boolean NOT NULL DEFAULT false,
  UNIQUE (source_topic, source_partition, source_offset, reason)
);
CREATE INDEX IF NOT EXISTS unresolved_incidents ON risk.incidents(id) WHERE NOT resolved;

CREATE TABLE IF NOT EXISTS risk.assessments (
  source_system text NOT NULL,
  transaction_id text NOT NULL,
  assessment_request_id text NOT NULL CHECK (assessment_request_id = 'initial'),
  payload jsonb NOT NULL,
  business_content jsonb NOT NULL,
  customer_id text NOT NULL,
  event_id text NOT NULL,
  event_time bigint NOT NULL,
  processed_at bigint NOT NULL,
  source_topic text NOT NULL,
  inserted_xid bigint NOT NULL DEFAULT txid_current(),
  stored_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (source_system, transaction_id, assessment_request_id)
);
CREATE INDEX IF NOT EXISTS assessment_customer_time ON risk.assessments(customer_id, event_time DESC, transaction_id DESC);
CREATE INDEX IF NOT EXISTS assessment_recent ON risk.assessments(processed_at DESC, transaction_id DESC);
CREATE INDEX IF NOT EXISTS assessment_topic_event ON risk.assessments(source_topic, event_id COLLATE "C");
CREATE TABLE IF NOT EXISTS risk.identity_aliases (
  source_system text NOT NULL, kind text NOT NULL, identity text NOT NULL, transaction_id text NOT NULL,
  PRIMARY KEY (source_system, kind, identity)
);
CREATE TABLE IF NOT EXISTS risk.customer_totals (
  customer_id text COLLATE "C" PRIMARY KEY,
  payments bigint NOT NULL, approved bigint NOT NULL, review bigint NOT NULL, rejected bigint NOT NULL,
  max_score integer NOT NULL, last_event bigint NOT NULL
);
CREATE INDEX IF NOT EXISTS customer_review ON risk.customer_totals(customer_id) WHERE review > 0;
CREATE INDEX IF NOT EXISTS customer_rejected ON risk.customer_totals(customer_id) WHERE rejected > 0;
CREATE INDEX IF NOT EXISTS customer_approved ON risk.customer_totals(customer_id) WHERE approved > 0;
-- Retain versions for snapshot-pinned pagination. One version per customer per writer transaction.
CREATE TABLE IF NOT EXISTS risk.customer_versions (
  customer_id text COLLATE "C" NOT NULL, xid bigint NOT NULL,
  payload jsonb NOT NULL, PRIMARY KEY (customer_id, xid)
);
ALTER TABLE risk.customer_versions ADD COLUMN IF NOT EXISTS revision bigint GENERATED ALWAYS AS IDENTITY;
CREATE INDEX IF NOT EXISTS customer_version_order ON risk.customer_versions(customer_id, revision DESC);
CREATE TABLE IF NOT EXISTS risk.time_buckets (
  bucket bigint NOT NULL, decision text NOT NULL, currency text NOT NULL,
  payments bigint NOT NULL, amount_minor numeric(30,0) NOT NULL,
  PRIMARY KEY (bucket, decision, currency)
);
CREATE TABLE IF NOT EXISTS risk.rule_buckets (
  bucket bigint NOT NULL, rule_id text NOT NULL, matches bigint NOT NULL,
  PRIMARY KEY (bucket, rule_id)
);
CREATE TABLE IF NOT EXISTS risk.latency_buckets (
  bucket bigint NOT NULL, upper_ms bigint NOT NULL, samples bigint NOT NULL,
  PRIMARY KEY (bucket, upper_ms)
);
CREATE TABLE IF NOT EXISTS risk.topic_totals (
  source_topic text PRIMARY KEY, deliveries bigint NOT NULL DEFAULT 0, assessments bigint NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS risk.outcomes (
  source_topic text NOT NULL, source_partition integer NOT NULL, source_offset bigint NOT NULL,
  payload jsonb NOT NULL, PRIMARY KEY (source_topic, source_partition, source_offset)
);

CREATE OR REPLACE FUNCTION risk.incident(p jsonb, why text) RETURNS void LANGUAGE sql AS $$
  INSERT INTO risk.incidents(source_topic,source_partition,source_offset,reason,payload)
  VALUES(p->>'source_topic',(p->>'source_partition')::integer,(p->>'source_offset')::bigint,why,p)
  ON CONFLICT DO NOTHING;
$$;

CREATE OR REPLACE FUNCTION risk.accept(kind text, p jsonb, system_name text)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  old jsonb; content jsonb; tx text; eid text; cid text; label text;
  bucket_time bigint; latency bigint; upper_bound bigint; rule text;
  alias_kind text; alias_value text; mapped text; inserted boolean; field_name text;
BEGIN
  IF system_name IS NULL OR length(system_name) NOT BETWEEN 1 AND 128 THEN
    RAISE EXCEPTION 'Invalid source system';
  END IF;
  -- ClickHouse JSON may encode 64-bit values as strings during the one-time migration.
  FOREACH field_name IN ARRAY ARRAY['source_partition','source_offset','source_timestamp','event_time','processed_at',
    'amount_minor','risk_score','policy_version','watermark','lateness_ms','input_partition','input_offset','payload_bytes'] LOOP
    IF p ? field_name THEN p:=jsonb_set(p,ARRAY[field_name],to_jsonb((p->>field_name)::bigint)); END IF;
  END LOOP;
  IF kind='risk.evaluations' THEN
    p:=jsonb_build_object('transaction_facts','','assessment_request_id','initial') || p;
  END IF;
  -- The writer retries the entire bounded transaction after any failure.
  INSERT INTO risk.deliveries(source_topic,source_partition,source_offset,kind,payload)
  VALUES(p->>'source_topic',(p->>'source_partition')::integer,(p->>'source_offset')::bigint,kind,p)
  ON CONFLICT DO NOTHING;
  inserted := FOUND;
  IF NOT inserted THEN
    SELECT payload INTO old FROM risk.deliveries
      WHERE source_topic=p->>'source_topic' AND source_partition=(p->>'source_partition')::integer
      AND source_offset=(p->>'source_offset')::bigint;
    IF (old - ARRAY['ingested_at','observed_at']) IS DISTINCT FROM (p - ARRAY['ingested_at','observed_at']) THEN
      PERFORM risk.incident(p,'DELIVERY_CONTENT_CONFLICT');
      RETURN 'conflict';
    END IF;
    RETURN 'retry';
  END IF;
  INSERT INTO risk.topic_totals(source_topic,deliveries) VALUES(p->>'source_topic',1)
    ON CONFLICT (source_topic) DO UPDATE SET deliveries=risk.topic_totals.deliveries+1;
  IF kind='risk.materializer_rejections' THEN
    PERFORM risk.incident(p,'INVALID_OUTPUT'); RETURN 'conflict';
  ELSIF kind='risk.processing_outcomes' THEN
    INSERT INTO risk.outcomes VALUES(p->>'source_topic',(p->>'source_partition')::integer,(p->>'source_offset')::bigint,p);
    RETURN 'outcome';
  ELSIF kind!='risk.evaluations' THEN
    RAISE EXCEPTION 'Unsupported projection record';
  END IF;
  tx:=p->>'transaction_id'; eid:=p->>'evaluation_id'; cid:=p->>'customer_id'; label:=p->>'decision';
  IF tx IS NULL OR tx='' OR eid IS NULL OR cid IS NULL OR label NOT IN ('APPROVE','REVIEW','REJECT')
     OR coalesce(p->>'assessment_request_id','initial')!='initial' THEN
    PERFORM risk.incident(p,'INVALID_ASSESSMENT_IDENTITY'); RETURN 'conflict';
  END IF;
  -- Serialize only competing assessments of the same business transaction.
  PERFORM pg_advisory_xact_lock(hashtextextended(system_name || ':' || tx,0));
  content:=p - ARRAY['processed_at','source_topic','source_partition','source_offset','ingested_at','evaluation_id','assessment_request_id'];
  IF coalesce(p->>'transaction_facts','')!='' THEN
    -- Full canonical facts, not just their hash, permit a new transport event ID.
    content:=content - ARRAY['event_id','input_sha256'];
  ELSE
    content:=content - 'transaction_facts';
  END IF;
  FOR alias_kind,alias_value IN SELECT * FROM (VALUES ('evaluation',eid),('event',p->>'event_id')) AS a(k,v) LOOP
    INSERT INTO risk.identity_aliases VALUES(system_name,alias_kind,alias_value,tx) ON CONFLICT DO NOTHING;
    SELECT a.transaction_id INTO mapped FROM risk.identity_aliases a
      WHERE a.source_system=system_name AND a.kind=alias_kind AND a.identity=alias_value;
    IF mapped IS DISTINCT FROM tx THEN
      PERFORM risk.incident(p,'IDENTITY_ASSIGNED_TO_ANOTHER_TRANSACTION'); RETURN 'conflict';
    END IF;
  END LOOP;
  INSERT INTO risk.assessments(source_system,transaction_id,assessment_request_id,payload,business_content,
    customer_id,event_id,event_time,processed_at,source_topic)
  VALUES(system_name,tx,'initial',p,content,cid,p->>'event_id',(p->>'event_time')::bigint,
    (p->>'processed_at')::bigint,p->>'source_topic') ON CONFLICT DO NOTHING;
  IF NOT FOUND THEN
    SELECT business_content INTO old FROM risk.assessments
      WHERE source_system=system_name AND transaction_id=tx AND assessment_request_id='initial';
    IF old IS DISTINCT FROM content THEN
      PERFORM risk.incident(p,'ASSESSMENT_CONTENT_CONFLICT'); RETURN 'conflict';
    END IF;
    RETURN 'duplicate';
  END IF;
  INSERT INTO risk.customer_totals VALUES(cid,1,(label='APPROVE')::integer,(label='REVIEW')::integer,
    (label='REJECT')::integer,(p->>'risk_score')::integer,(p->>'event_time')::bigint)
  ON CONFLICT (customer_id) DO UPDATE SET
    payments=risk.customer_totals.payments+1, approved=risk.customer_totals.approved+EXCLUDED.approved,
    review=risk.customer_totals.review+EXCLUDED.review, rejected=risk.customer_totals.rejected+EXCLUDED.rejected,
    max_score=greatest(risk.customer_totals.max_score,EXCLUDED.max_score),
    last_event=greatest(risk.customer_totals.last_event,EXCLUDED.last_event);
  INSERT INTO risk.customer_versions SELECT cid,txid_current(),to_jsonb(c) FROM risk.customer_totals c WHERE customer_id=cid
    ON CONFLICT (customer_id,xid) DO UPDATE SET payload=EXCLUDED.payload,revision=DEFAULT;
  bucket_time:=(p->>'processed_at')::bigint / 10000 * 10000;
  INSERT INTO risk.time_buckets VALUES(bucket_time,label,p->>'currency',1,(p->>'amount_minor')::numeric)
    ON CONFLICT (bucket,decision,currency) DO UPDATE SET payments=risk.time_buckets.payments+1,
      amount_minor=risk.time_buckets.amount_minor+EXCLUDED.amount_minor;
  FOR rule IN SELECT DISTINCT jsonb_array_elements_text(p->'matched_rules') LOOP
    INSERT INTO risk.rule_buckets VALUES(bucket_time,rule,1)
      ON CONFLICT (bucket,rule_id) DO UPDATE SET matches=risk.rule_buckets.matches+1;
  END LOOP;
  latency:=greatest(0,(p->>'processed_at')::bigint-(p->>'event_time')::bigint);
  -- Logarithmic histogram: bounded state; exported p95 is an upper-bound estimate.
  upper_bound:=CASE WHEN latency=0 THEN 0 ELSE power(2,ceil(log(2,latency::numeric)))::bigint END;
  INSERT INTO risk.latency_buckets VALUES(bucket_time,upper_bound,1)
    ON CONFLICT (bucket,upper_ms) DO UPDATE SET samples=risk.latency_buckets.samples+1;
  UPDATE risk.topic_totals SET assessments=assessments+1 WHERE source_topic=p->>'source_topic';
  RETURN 'accepted';
END;
$$;
