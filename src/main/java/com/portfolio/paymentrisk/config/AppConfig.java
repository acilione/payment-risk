package com.portfolio.paymentrisk.config;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

public record AppConfig(
    String bootstrap,
    String registry,
    String group,
    String prefix,
    long disorderMs,
    long idleMs,
    long dedupMs,
    long historyMs,
    long deviceMs,
    int maxEvents,
    int maxDevices,
    int review,
    int reject,
    String offsets,
    String environment,
    Map<String, String> topicNames)
    implements Serializable {
  public AppConfig {
    topicNames = Map.copyOf(topicNames);
    if (new java.util.HashSet<>(topicNames.values()).size() != topicNames.size())
      throw new IllegalArgumentException("Kafka topics must be distinct");
    if (topicNames.values().stream().anyMatch(v -> !v.matches("[a-zA-Z0-9._-]{1,249}")))
      throw new IllegalArgumentException("Invalid Kafka topic name");
    if (disorderMs < 0
        || idleMs < 1
        || dedupMs < 1
        || historyMs < 900_000
        || deviceMs < 2_592_000_000L
        || historyMs > deviceMs
        || deviceMs > 31_536_000_000L
        || maxEvents < 1
        || maxDevices < 1
        || review < 1
        || reject <= review
        || reject > 100
        || prefix.isBlank()
        || !java.util.Set.of("earliest", "committed").contains(offsets))
      throw new IllegalArgumentException("Invalid application configuration");
  }

  public static AppConfig fromEnv() {
    return from(System.getenv());
  }

  public static AppConfig from(Map<String, String> e) {
    return new AppConfig(
        e.getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        e.getOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:28081/apis/ccompat/v7"),
        e.getOrDefault("KAFKA_GROUP", "payment-risk-v1"),
        e.getOrDefault("TRANSACTIONAL_PREFIX", "payment-risk-local-v1"),
        n(e, "OUT_OF_ORDER_MS", 10_000),
        n(e, "IDLE_TIMEOUT_MS", 60_000),
        n(e, "DEDUP_TTL_MS", 86_400_000),
        n(e, "HISTORY_MS", 3_600_000),
        n(e, "DEVICE_HISTORY_MS", 2_592_000_000L),
        Math.toIntExact(n(e, "MAX_EVENTS_PER_CUSTOMER", 100_000)),
        Math.toIntExact(n(e, "MAX_DEVICES_PER_CUSTOMER", 10_000)),
        Math.toIntExact(n(e, "REVIEW_THRESHOLD", 30)),
        Math.toIntExact(n(e, "REJECT_THRESHOLD", 70)),
        e.getOrDefault("STARTUP_OFFSETS", "earliest"),
        e.getOrDefault("APP_ENVIRONMENT", "local"),
        Map.of(
            "payments.raw",
            e.getOrDefault("PAYMENTS_TOPIC", "payments.raw"),
            "risk.rules",
            e.getOrDefault("RULES_TOPIC", "risk.rules"),
            "risk.decisions",
            e.getOrDefault("DECISIONS_TOPIC", "risk.decisions"),
            "payments.dlq",
            e.getOrDefault("DLQ_TOPIC", "payments.dlq"),
            "payments.late",
            e.getOrDefault("LATE_TOPIC", "payments.late")));
  }

  private static long n(Map<String, String> e, String k, long d) {
    return Long.parseLong(e.getOrDefault(k, "" + d));
  }

  public String topic(String logical) {
    return topicNames.getOrDefault(logical, logical);
  }

  public Properties kafkaProperties() {
    Properties p = new Properties();
    String file = System.getenv("KAFKA_PROPERTIES_FILE");
    if (file != null)
      try (var in = java.nio.file.Files.newInputStream(java.nio.file.Path.of(file))) {
        p.load(in);
      } catch (java.io.IOException ex) {
        throw new IllegalStateException("Cannot load Kafka credentials", ex);
      }
    if (environment.equals("production")
        && (!registry.startsWith("https://")
            || !java.util.Set.of("SSL", "SASL_SSL").contains(p.getProperty("security.protocol", ""))
            || offsets.equals("earliest")
            || prefix.equals("payment-risk-local-v1")))
      throw new IllegalArgumentException(
          "Production requires TLS, explicit committed offsets and a deployment transaction prefix");
    return p;
  }
}
