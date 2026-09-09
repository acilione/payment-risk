package com.portfolio.paymentrisk.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.portfolio.paymentrisk.Json;
import com.portfolio.paymentrisk.config.AppConfig;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

public final class Rules {
  public enum Type {
    TX_COUNT_VELOCITY,
    AMOUNT_VELOCITY,
    UNIQUE_DEVICES,
    NEW_DEVICE_HIGH_AMOUNT,
    DECLINE_THEN_APPROVAL
  }

  private Rules() {}

  public static List<JsonNode> defaults() {
    var list = new ArrayList<JsonNode>();
    String[] types = {
      "TX_COUNT_VELOCITY",
      "AMOUNT_VELOCITY",
      "UNIQUE_DEVICES",
      "NEW_DEVICE_HIGH_AMOUNT",
      "DECLINE_THEN_APPROVAL"
    };
    long[] windows = {120, 600, 900, 2_592_000, 600}, thresholds = {5, 300_000, 3, 80_000, 4};
    int[] scores = {25, 35, 20, 30, 40};
    for (int i = 0; i < 5; i++)
      list.add(
          Json.object()
              .put("rule_id", "R00" + (i + 1))
              .put("version", 1L)
              .put("type", types[i])
              .put("enabled", true)
              .put("score", scores[i])
              .put("window_seconds", windows[i])
              .put("threshold", thresholds[i])
              .put("currency", "EUR")
              .put("updated_at", 0L));
    return list;
  }

  public static void validate(JsonNode r, AppConfig c) {
    Type type;
    try {
      type = Type.valueOf(r.path("type").asText());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("UNKNOWN_RULE_TYPE");
    }
    long max = type == Type.NEW_DEVICE_HIGH_AMOUNT ? c.deviceMs() : c.historyMs();
    if (!r.path("rule_id").asText().matches("R00[1-5]")
        || r.path("version").asLong() < 1
        || r.path("score").asInt(-1) < 0
        || r.path("score").asInt() > 100
        || r.path("window_seconds").asLong() < 1
        || r.path("window_seconds").asLong() > max / 1000
        || r.path("threshold").asLong(-1) < 0
        || !r.path("enabled").isBoolean()
        || !r.path("currency").asText().equals("EUR")
        || r.path("updated_at").asLong(-1) < 0)
      throw new IllegalArgumentException("INVALID_RULE_CONFIGURATION");
  }

  public static String fingerprint(List<JsonNode> rules, int review, int reject) {
    var canonical = new TreeMap<String, String>();
    for (var r : rules) {
      var fields = new TreeMap<String, Object>();
      r.fields().forEachRemaining(e -> fields.put(e.getKey(), e.getValue()));
      canonical.put(r.path("rule_id").asText(), Json.write(fields));
    }
    try {
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256")
                  .digest(
                      (review + ":" + reject + ":" + Json.write(canonical))
                          .getBytes(StandardCharsets.UTF_8)));
    } catch (java.security.NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
