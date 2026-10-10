package com.portfolio.paymentrisk.tools.generation;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;

public record GeneratorConfig(
    long seed,
    int customers,
    int minPayments,
    int maxPayments,
    int historyMinutes,
    int rate,
    int retryPercent,
    long ordinaryMin,
    long ordinaryMax,
    long largeMin,
    long largeMax,
    Map<String, Integer> profiles) {
  public static final List<String> PROFILES =
      List.of("normal", "new_device", "takeover", "card_testing", "low_burst", "trusted_device");

  public static GeneratorConfig parse(JsonNode n) {
    var allowed =
        Set.of(
            "seed",
            "customers",
            "min_payments",
            "max_payments",
            "history_minutes",
            "publish_rate",
            "retry_percent",
            "ordinary_amount_min",
            "ordinary_amount_max",
            "large_amount_min",
            "large_amount_max",
            "profiles");
    if (!n.isObject())
      throw new IllegalArgumentException("Generator configuration must be an object");
    n.fieldNames()
        .forEachRemaining(
            k -> {
              if (!allowed.contains(k)) throw new IllegalArgumentException("Unknown setting: " + k);
            });
    var weights = new LinkedHashMap<String, Integer>();
    var profiles = n.path("profiles");
    if (!profiles.isObject()) throw new IllegalArgumentException("profiles must be an object");
    profiles
        .fieldNames()
        .forEachRemaining(
            k -> {
              if (!PROFILES.contains(k))
                throw new IllegalArgumentException("Unknown profile: " + k);
            });
    for (var key : PROFILES) weights.put(key, (int) integer(profiles, key, 0, 100));
    if (weights.values().stream().mapToInt(x -> x).sum() != 100)
      throw new IllegalArgumentException("Profile weights must sum to 100");
    int customers = (int) integer(n, "customers", 1, 2000),
        min = (int) integer(n, "min_payments", 7, 100),
        max = (int) integer(n, "max_payments", min, 100);
    if ((long) customers * max > 100000)
      throw new IllegalArgumentException("Maximum generated payments is 100000");
    long ordinaryMin = integer(n, "ordinary_amount_min", 1, 100000000),
        ordinaryMax = integer(n, "ordinary_amount_max", ordinaryMin, 100000000);
    long largeMin = integer(n, "large_amount_min", 1, 100000000),
        largeMax = integer(n, "large_amount_max", largeMin, 100000000);
    return new GeneratorConfig(
        integer(n, "seed", 0, Long.MAX_VALUE),
        customers,
        min,
        max,
        (int) integer(n, "history_minutes", 5, 1440),
        (int) integer(n, "publish_rate", 1, 1000),
        (int) integer(n, "retry_percent", 0, 100),
        ordinaryMin,
        ordinaryMax,
        largeMin,
        largeMax,
        Collections.unmodifiableMap(weights));
  }

  private static long integer(JsonNode n, String key, long min, long max) {
    var v = n.path(key);
    if (!v.isIntegralNumber() || !v.canConvertToLong() || v.asLong() < min || v.asLong() > max)
      throw new IllegalArgumentException("Invalid integer setting: " + key);
    return v.asLong();
  }
}
