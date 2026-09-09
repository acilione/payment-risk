package com.portfolio.paymentrisk.processor;

import java.time.Duration;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Deduplicate extends KeyedProcessFunction<String, String, String> {
  private final long ttl;
  private transient ValueState<Boolean> seen;
  private transient Counter duplicate;

  public Deduplicate(long ttl) {
    this.ttl = ttl;
  }

  public void open(OpenContext c) {
    var d = new ValueStateDescriptor<>("event-seen-v1", Boolean.class);
    d.enableTimeToLive(
        StateTtlConfig.newBuilder(Duration.ofMillis(ttl))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build());
    seen = getRuntimeContext().getState(d);
    duplicate = getRuntimeContext().getMetricGroup().counter("risk_transactions_duplicate_total");
  }

  public void processElement(String value, Context c, Collector<String> out) throws Exception {
    if (Boolean.TRUE.equals(seen.value())) {
      duplicate.inc();
      return;
    }
    seen.update(true);
    out.collect(value);
  }
}
