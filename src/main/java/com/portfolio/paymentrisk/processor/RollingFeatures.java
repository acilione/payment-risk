package com.portfolio.paymentrisk.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.portfolio.paymentrisk.Json;
import com.portfolio.paymentrisk.domain.RiskFeatures;
import com.portfolio.paymentrisk.domain.Rules;
import java.util.*;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimerService;

/** Exact event-time windows. Only policy-window changes scan retained history. */
final class RollingFeatures {
  private final MapState<String, String> windows;
  private final ValueState<Boolean> active;
  private final MapState<String, Long> deviceReferences;

  RollingFeatures(RuntimeContext context) {
    active = context.getState(new ValueStateDescriptor<>("rolling-active-v1", Types.BOOLEAN));
    windows =
        context.getMapState(
            new MapStateDescriptor<>("rolling-windows-v1", Types.STRING, Types.STRING));
    deviceReferences =
        context.getMapState(
            new MapStateDescriptor<>("rolling-device-counts-v1", Types.STRING, Types.LONG));
  }

  Map<String, RiskFeatures.Observation> observe(
      JsonNode tx,
      Iterable<JsonNode> history,
      Long lastSeen,
      List<JsonNode> rules,
      TimerService timers)
      throws Exception {
    active.update(true);
    var result = new HashMap<String, RiskFeatures.Observation>();
    long time = tx.path("event_time").asLong();
    for (var rule : rules) {
      String id = rule.path("rule_id").asText(), type = rule.path("type").asText();
      long window = rule.path("window_seconds").asLong() * 1000;
      long observed;
      boolean eligible = true;
      if (type.equals("NEW_DEVICE_HIGH_AMOUNT")) {
        removeWindow(id);
        observed = tx.path("amount_minor").asLong();
        eligible = lastSeen == null || lastSeen <= time - window;
      } else {
        String signature = type + ":" + window + ":" + rule.path("currency").asText();
        String saved = windows.get(id);
        ObjectNode state = saved == null ? null : (ObjectNode) Json.read(saved);
        if (state == null || !state.path("signature").asText().equals(signature)) {
          removeWindow(id);
          state =
              Json.object()
                  .put("signature", signature)
                  .put("type", type)
                  .put("window", window)
                  .put("initialized_at", time)
                  .put("value", 0);
          for (var event : history) {
            long timestamp = event.path("event_time").asLong();
            if (timestamp > time - window && timestamp <= time) {
              change(id, state, event, 1);
              timers.registerEventTimeTimer(timestamp + window);
            }
          }
          windows.put(id, Json.write(state));
        }
        observed = state.path("value").asLong();
        switch (Rules.Type.valueOf(type)) {
          case TX_COUNT_VELOCITY -> observed++;
          case AMOUNT_VELOCITY -> {
            observed = Math.addExact(observed, tx.path("amount_minor").asLong());
            eligible = tx.path("currency").asText().equals(rule.path("currency").asText());
          }
          case UNIQUE_DEVICES -> {
            if (deviceReferences.get(id + ":" + tx.path("device_id").asText()) == null) observed++;
          }
          case DECLINE_THEN_APPROVAL -> eligible = tx.path("status").asText().equals("APPROVED");
          default -> throw new IllegalStateException("Unexpected rolling rule");
        }
      }
      long threshold = rule.path("threshold").asLong();
      boolean hit =
          type.equals("TX_COUNT_VELOCITY") || type.equals("AMOUNT_VELOCITY")
              ? observed > threshold
              : observed >= threshold;
      result.put(id, new RiskFeatures.Observation(observed, eligible, hit));
    }
    return result;
  }

  void add(JsonNode event, TimerService timers) throws Exception {
    // At most five active windows, including disabled rules whose observations remain auditable.
    for (String id : List.of("R001", "R002", "R003", "R004", "R005")) {
      String saved = windows.get(id);
      if (saved == null) continue;
      var state = (ObjectNode) Json.read(saved);
      change(id, state, event, 1);
      windows.put(id, Json.write(state));
      timers.registerEventTimeTimer(
          event.path("event_time").asLong() + state.path("window").asLong());
    }
  }

  void expire(long time, MapState<Long, String> history) throws Exception {
    for (String id : List.of("R001", "R002", "R003", "R004", "R005")) {
      String saved = windows.get(id);
      if (saved == null) continue;
      var state = (ObjectNode) Json.read(saved);
      if (time <= state.path("initialized_at").asLong()) continue;
      String bucket = history.get(time - state.path("window").asLong());
      if (bucket == null) continue;
      for (var event : StateHistory.rows(bucket)) change(id, state, event, -1);
      windows.put(id, Json.write(state));
    }
  }

  private void change(String id, ObjectNode state, JsonNode event, int direction) throws Exception {
    long delta =
        switch (Rules.Type.valueOf(state.path("type").asText())) {
          case TX_COUNT_VELOCITY -> 1;
          case AMOUNT_VELOCITY -> event.path("amount_minor").asLong();
          case DECLINE_THEN_APPROVAL -> event.path("status").asText().equals("DECLINED") ? 1 : 0;
          case UNIQUE_DEVICES -> {
            String key = id + ":" + event.path("device_id").asText();
            Long previous = deviceReferences.get(key);
            long before = previous == null ? 0 : previous, after = before + direction;
            if (after < 0) throw new IllegalStateException("Negative device occurrence count");
            if (after == 0) deviceReferences.remove(key);
            else deviceReferences.put(key, after);
            yield direction > 0 ? (before == 0 ? 1 : 0) : (after == 0 ? 1 : 0);
          }
          default -> throw new IllegalStateException("Unexpected rolling rule");
        };
    long value = Math.addExact(state.path("value").asLong(), direction * delta);
    if (value < 0) throw new IllegalStateException("Negative rolling feature");
    state.put("value", value);
  }

  boolean hasExpiry(long time, MapState<Long, String> history) throws Exception {
    for (String saved : windows.values()) {
      var state = Json.read(saved);
      if (time > state.path("initialized_at").asLong()
          && history.contains(time - state.path("window").asLong())) return true;
    }
    return false;
  }

  boolean active() throws Exception {
    return Boolean.TRUE.equals(active.value());
  }

  void clear() throws Exception {
    active.clear();
    windows.clear();
    deviceReferences.clear();
  }

  private void removeWindow(String id) throws Exception {
    if (windows.get(id) == null) return;
    var keys = new ArrayList<String>();
    for (String key : deviceReferences.keys()) if (key.startsWith(id + ":")) keys.add(key);
    for (String key : keys) deviceReferences.remove(key);
    windows.remove(id);
  }
}
