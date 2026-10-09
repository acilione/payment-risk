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
    duplicate = getRuntimeContext().getMetricGroup().counter("risk_transactions_duplicate_total");
  }

  public void processElement(String value, Context c, Collector<String> out) throws Exception {
    String hash =
        com.portfolio.paymentrisk.domain.Audit.inputHash(
            com.portfolio.paymentrisk.Json.read(value));
    if (Boolean.TRUE.equals(seen.value())) {
      if (fingerprint.value() == null)
        throw new IllegalStateException(
            identity + "_IDENTITY_UNVERIFIABLE: legacy state has no payload fingerprint");
      if (!hash.equals(fingerprint.value()))
        throw new IllegalStateException(
            identity + "_IDENTITY_CONFLICT: repeated identity has different business content");
      duplicate.inc();
      return;
    }
    seen.update(true);
    fingerprint.update(hash);
    out.collect(value);
  }
}
