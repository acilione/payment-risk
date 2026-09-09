package com.portfolio.paymentrisk.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.portfolio.paymentrisk.*;
import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.domain.*;
import java.util.*;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.*;

public class CustomerRiskProcessor
    extends KeyedBroadcastProcessFunction<String, String, String, String> {
  public static final MapStateDescriptor<String, String> RULES =
      new MapStateDescriptor<>("rules-json-v1", Types.STRING, Types.STRING);
  public static final OutputTag<String> LATE = new OutputTag<>("late-events", Types.STRING);
  private final AppConfig config;
  private transient MapState<Long, String> pending, history;
  private transient MapState<String, Long> devices;
  private transient ValueState<Integer> pendingCount, historyCount, deviceCount;
  private transient ValueState<Long> cleanupAt, finalizedThrough;
  private transient Counter late, updates, stale;
  private transient Map<String, Counter> decisions, matches;
  private transient org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram evaluation;
  private transient Counter amount;

  public CustomerRiskProcessor(AppConfig config) {
    this.config = config;
  }

  public void open(OpenContext c) {
    finalizedThrough =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<>("finalized-through-v1", Types.LONG));
    cleanupAt =
        getRuntimeContext().getState(new ValueStateDescriptor<>("cleanup-at-v1", Types.LONG));
    pending =
        getRuntimeContext()
            .getMapState(new MapStateDescriptor<>("pending-json-v1", Types.LONG, Types.STRING));
    history =
        getRuntimeContext()
            .getMapState(new MapStateDescriptor<>("history-json-v1", Types.LONG, Types.STRING));
    devices =
        getRuntimeContext()
            .getMapState(new MapStateDescriptor<>("devices-v1", Types.STRING, Types.LONG));
    pendingCount =
        getRuntimeContext().getState(new ValueStateDescriptor<>("pending-count-v1", Types.INT));
    historyCount =
        getRuntimeContext().getState(new ValueStateDescriptor<>("history-count-v1", Types.INT));
    deviceCount =
        getRuntimeContext().getState(new ValueStateDescriptor<>("device-count-v1", Types.INT));
    var metrics = getRuntimeContext().getMetricGroup();
    late = metrics.counter("risk_transactions_late_total");
    updates = metrics.counter("risk_rule_updates_total");
    stale = metrics.counter("risk_rule_updates_stale_total");
    evaluation =
        metrics.histogram(
            "risk_evaluation_duration_us",
            new org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram(1024));
    amount = metrics.counter("risk_amount_minor_total");
    decisions = new HashMap<>();
    matches = new HashMap<>();
    for (String d : List.of("APPROVE", "REVIEW", "REJECT"))
      decisions.put(d, metrics.addGroup("decision", d).counter("risk_decisions_total"));
    for (var r : Rules.defaults())
      matches.put(
          r.path("rule_id").asText(),
          metrics
              .addGroup("rule_id", r.path("rule_id").asText())
              .counter("risk_rule_matches_total"));
  }

  private static int count(ValueState<Integer> s) throws Exception {
    return s.value() == null ? 0 : s.value();
  }

  private static void setCount(ValueState<Integer> s, int value) throws Exception {
    if (value == 0) s.clear();
    else s.update(value);
  }

  private void scheduleCleanup(OnTimerContext ctx) throws Exception {
    if (cleanupAt.value() != null) return;
    long next = Long.MAX_VALUE;
    for (long time : history.keys()) next = Math.min(next, time + config.historyMs());
    for (long time : devices.values()) next = Math.min(next, time + config.deviceMs());
    if (next != Long.MAX_VALUE) {
      cleanupAt.update(next);
      ctx.timerService().registerEventTimeTimer(next);
    }
  }

  private static List<JsonNode> rows(String json) {
    var result = new ArrayList<JsonNode>();
    if (json != null) Json.read(json).forEach(result::add);
    return result;
  }

  private List<JsonNode> snapshot(ReadOnlyBroadcastState<String, String> state) throws Exception {
    var rules = new TreeMap<String, JsonNode>();
    for (var r : Rules.defaults()) rules.put(r.path("rule_id").asText(), r);
    for (var e : state.immutableEntries()) rules.put(e.getKey(), Json.read(e.getValue()));
    return new ArrayList<>(rules.values());
  }

  public void processBroadcastElement(String value, Context ctx, Collector<String> out)
      throws Exception {
    var r = Json.read(value);
    Rules.validate(r, config);
    var state = ctx.getBroadcastState(RULES);
    String id = r.path("rule_id").asText(), previous = state.get(id);
    long version = previous == null ? 1 : Json.read(previous).path("version").asLong();
    if (r.path("version").asLong() <= version) {
      stale.inc();
      return;
    }
    state.put(id, value);
    updates.inc();
    org.slf4j.LoggerFactory.getLogger(getClass())
        .info("event=rule_update rule_id={} version={}", id, r.path("version").asLong());
  }

  public void processElement(String value, ReadOnlyContext ctx, Collector<String> out)
      throws Exception {
    var tx = Json.read(value);
    long t = tx.path("event_time").asLong(), watermark = ctx.timerService().currentWatermark();
    // Source watermarks restart from MIN_VALUE after recovery. Retained customer history
    // must never be revised behind its checkpointed evaluation frontier.
    if (finalizedThrough.value() != null) watermark = Math.max(watermark, finalizedThrough.value());
    if (t <= watermark) {
      late.inc();
      ctx.output(
          LATE,
          Json.write(
              Json.object()
                  .put("transaction", value)
                  .put("customer_id", tx.path("customer_id").asText())
                  .put("watermark", watermark)
                  .put("lateness_ms", watermark - t)
                  .put("observed_at", System.currentTimeMillis())
                  .put("reason", "TOO_LATE")));
      return;
    }
    if (count(pendingCount) >= config.maxEvents())
      throw new IllegalStateException("Customer pending state limit exceeded");
    var bucket = rows(pending.get(t));
    var item = Json.object();
    item.set("transaction", tx);
    item.set("rules", Json.MAPPER.valueToTree(snapshot(ctx.getBroadcastState(RULES))));
    bucket.add(item);
    pending.put(t, Json.write(bucket));
    pendingCount.update(count(pendingCount) + 1);
    ctx.timerService().registerEventTimeTimer(t);
  }

  public void onTimer(long t, OnTimerContext ctx, Collector<String> out) throws Exception {
    if (cleanupAt.value() != null && cleanupAt.value() == t) cleanupAt.clear();
    // Clean only the timer's logical time, never a jumped watermark: earlier pending events still
    // need history.
    var expired = new ArrayList<Long>();
    int removed = 0;
    for (var e : history.entries())
      if (e.getKey() <= t - config.historyMs()) {
        expired.add(e.getKey());
        removed += rows(e.getValue()).size();
      }
    for (long k : expired) history.remove(k);
    setCount(historyCount, count(historyCount) - removed);
    var oldDevices = new ArrayList<String>();
    for (var e : devices.entries())
      if (e.getValue() <= t - config.deviceMs()) oldDevices.add(e.getKey());
    for (String d : oldDevices) devices.remove(d);
    setCount(deviceCount, count(deviceCount) - oldDevices.size());
    var bucket = rows(pending.get(t));
    if (bucket.isEmpty()) {
      if (count(historyCount) == 0 && count(deviceCount) == 0 && count(pendingCount) == 0)
        finalizedThrough.clear();
      scheduleCleanup(ctx);
      return;
    }
    bucket.sort(Comparator.comparing(n -> n.path("transaction").path("event_id").asText()));
    var recent = new ArrayList<JsonNode>();
    for (String value : history.values()) recent.addAll(rows(value));
    var known = new HashMap<String, Long>();
    for (var e : devices.entries()) known.put(e.getKey(), e.getValue());
    var current = rows(history.get(t));
    for (var item : bucket) {
      if (count(historyCount) >= config.maxEvents())
        throw new IllegalStateException("Customer history state limit exceeded");
      var tx = item.path("transaction");
      String device = tx.path("device_id").asText();
      if (!known.containsKey(device) && count(deviceCount) >= config.maxDevices())
        throw new IllegalStateException("Customer device state limit exceeded");
      var rules = new ArrayList<JsonNode>();
      item.path("rules").forEach(rules::add);
      long started = System.nanoTime();
      var decision =
          RiskEngine.evaluate(
              tx,
              recent,
              known,
              rules,
              config.review(),
              config.reject(),
              System.currentTimeMillis());
      evaluation.update((System.nanoTime() - started) / 1000);
      amount.inc(tx.path("amount_minor").asLong());
      out.collect(Json.write(decision));
      decisions.get(decision.path("decision").asText()).inc();
      decision.path("matched_rules").forEach(r -> matches.get(r.asText()).inc());
      var feature =
          Json.object()
              .put("event_time", t)
              .put("amount_minor", tx.path("amount_minor").asLong())
              .put("currency", tx.path("currency").asText())
              .put("device_id", device)
              .put("status", tx.path("status").asText());
      recent.add(feature);
      current.add(feature);
      historyCount.update(count(historyCount) + 1);
      if (!known.containsKey(device)) deviceCount.update(count(deviceCount) + 1);
      known.put(device, t);
      devices.put(device, t);
    }
    history.put(t, Json.write(current));
    finalizedThrough.update(t);
    pending.remove(t);
    setCount(pendingCount, count(pendingCount) - bucket.size());
    scheduleCleanup(ctx);
  }
}
