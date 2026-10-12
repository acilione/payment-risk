package com.portfolio.paymentrisk.processor;

import java.time.Duration;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Deduplicate extends KeyedProcessFunction<String, String, String> {
  private final long ttl;
  private final String identity;
  private transient ValueState<Boolean> seen;
  private transient ValueState<String> fingerprint;
  private transient ValueState<String> businessFacts;
  private transient Counter duplicate;

  public Deduplicate(long ttl) {
    this(ttl, "EVENT");
  }

  public Deduplicate(long ttl, String identity) {
    this.ttl = ttl;
    this.identity = identity;
  }

  public void open(OpenContext c) {
    var d = new ValueStateDescriptor<>("event-seen-v1", Boolean.class);
    d.enableTimeToLive(
        StateTtlConfig.newBuilder(Duration.ofMillis(ttl))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build());
    seen = getRuntimeContext().getState(d);
    var hashes = new ValueStateDescriptor<>("event-fingerprint-v1", String.class);
    hashes.enableTimeToLive(
        StateTtlConfig.newBuilder(Duration.ofMillis(ttl))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build());
    fingerprint = getRuntimeContext().getState(hashes);
    var facts = new ValueStateDescriptor<>("transaction-facts-v2", String.class);
    facts.enableTimeToLive(
        StateTtlConfig.newBuilder(Duration.ofMillis(ttl))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build());
    businessFacts = getRuntimeContext().getState(facts);
    duplicate = getRuntimeContext().getMetricGroup().counter("risk_transactions_duplicate_total");
  }

  public void processElement(String value, Context c, Collector<String> out) throws Exception {
    var tx = com.portfolio.paymentrisk.Json.read(value);
    String hash = com.portfolio.paymentrisk.domain.Audit.inputHash(tx);
    String facts =
        identity.equals("TRANSACTION")
            ? com.portfolio.paymentrisk.domain.Audit.transactionFacts(tx)
            : null;
    if (Boolean.TRUE.equals(seen.value())) {
      String previousHash = fingerprint.value();
      String previousFacts = identity.equals("TRANSACTION") ? businessFacts.value() : null;
      if (previousHash == null)
        throw new IllegalStateException(
            identity + "_IDENTITY_UNVERIFIABLE: legacy state has no payload fingerprint");
      boolean matches =
          previousFacts != null ? facts.equals(previousFacts) : hash.equals(previousHash);
      if (!matches)
        throw new IllegalStateException(
            identity
                + "_IDENTITY_CONFLICT: existing_sha256="
                + previousHash
                + " incoming_sha256="
                + hash);
      duplicate.inc();
      // An unchanged retry safely upgrades legacy checkpoint state; unknown facts fail closed.
      if (identity.equals("TRANSACTION") && previousFacts == null) businessFacts.update(facts);
      return;
    }
    seen.update(true);
    fingerprint.update(hash);
    if (identity.equals("TRANSACTION")) businessFacts.update(facts);
    out.collect(value);
  }
}
