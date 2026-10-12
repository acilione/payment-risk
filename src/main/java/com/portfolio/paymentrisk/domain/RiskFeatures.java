package com.portfolio.paymentrisk.domain;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;

/** One pass over retained payments; only distinct device IDs need a set. */
public final class RiskFeatures {
  private RiskFeatures() {}

  public record Observation(long value, boolean eligible, boolean matched) {}

  public static Map<String, Observation> observe(
      JsonNode tx, Iterable<JsonNode> history, Long deviceLastSeen, List<JsonNode> rules) {
    long time = tx.path("event_time").asLong();
    int size = rules.size();
    long[] values = new long[size], boundaries = new long[size];
    var types = new Rules.Type[size];
    var unique = new HashMap<Integer, Set<String>>();
    for (int i = 0; i < size; i++) {
      var rule = rules.get(i);
      types[i] = Rules.Type.valueOf(rule.path("type").asText());
      boundaries[i] = time - rule.path("window_seconds").asLong() * 1000;
      switch (types[i]) {
        case TX_COUNT_VELOCITY -> values[i] = 1;
        case AMOUNT_VELOCITY, NEW_DEVICE_HIGH_AMOUNT ->
            values[i] = tx.path("amount_minor").asLong();
        case UNIQUE_DEVICES -> unique.put(i, new HashSet<>(Set.of(tx.path("device_id").asText())));
        default -> {}
      }
    }
    for (var payment : history) {
      long timestamp = payment.path("event_time").asLong();
      if (timestamp > time) continue;
      for (int i = 0; i < size; i++) {
        if (timestamp <= boundaries[i]) continue;
        switch (types[i]) {
          case TX_COUNT_VELOCITY -> values[i]++;
          case AMOUNT_VELOCITY -> {
            if (payment.path("currency").asText().equals(tx.path("currency").asText()))
              values[i] = Math.addExact(values[i], payment.path("amount_minor").asLong());
          }
          case UNIQUE_DEVICES -> unique.get(i).add(payment.path("device_id").asText());
          case DECLINE_THEN_APPROVAL -> {
            if (payment.path("status").asText().equals("DECLINED")) values[i]++;
          }
          default -> {}
        }
      }
    }
    var result = new HashMap<String, Observation>();
    for (int i = 0; i < size; i++) {
      long threshold = rules.get(i).path("threshold").asLong();
      boolean eligible = true;
      if (types[i] == Rules.Type.UNIQUE_DEVICES) values[i] = unique.get(i).size();
      if (types[i] == Rules.Type.AMOUNT_VELOCITY)
        eligible = tx.path("currency").asText().equals(rules.get(i).path("currency").asText());
      if (types[i] == Rules.Type.NEW_DEVICE_HIGH_AMOUNT)
        eligible = deviceLastSeen == null || deviceLastSeen <= boundaries[i];
      if (types[i] == Rules.Type.DECLINE_THEN_APPROVAL)
        eligible = tx.path("status").asText().equals("APPROVED");
      boolean hit =
          types[i] == Rules.Type.TX_COUNT_VELOCITY || types[i] == Rules.Type.AMOUNT_VELOCITY
              ? values[i] > threshold
              : values[i] >= threshold;
      result.put(rules.get(i).path("rule_id").asText(), new Observation(values[i], eligible, hit));
    }
    return result;
  }
}
