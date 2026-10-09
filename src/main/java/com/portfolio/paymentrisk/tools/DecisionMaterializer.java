package com.portfolio.paymentrisk.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.portfolio.paymentrisk.*;
import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.serialization.AvroCodec;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;

/** Committed Kafka records -> durable deliveries/quarantine -> synchronous offset commit. */
public final class DecisionMaterializer {
  private DecisionMaterializer() {}

  public static void main(String[] args) throws Exception {
    var c = AppConfig.fromEnv();
    var p = c.kafkaProperties();
    p.put("bootstrap.servers", c.bootstrap());
    p.put(
        "group.id",
        System.getenv().getOrDefault("MATERIALIZER_GROUP", "risk-clickhouse-evaluations-v2"));
    p.put("key.deserializer", StringDeserializer.class.getName());
    p.put("value.deserializer", ByteArrayDeserializer.class.getName());
    p.put("enable.auto.commit", "false");
    p.put("isolation.level", "read_committed");
    p.put("auto.offset.reset", c.environment().equals("local") ? "earliest" : "none");
    p.put("max.poll.records", "500");
    p.put("max.poll.interval.ms", "300000");
    var codec = new AvroCodec(c.registry());
    String endpoint = System.getenv().getOrDefault("CLICKHOUSE_URL", "http://localhost:28123");
    if (!c.environment().equals("local") && !endpoint.startsWith("https://"))
      throw new IllegalArgumentException("ClickHouse TLS required outside local mode");
    try (var metrics =
            new MaterializerMetrics(
                Integer.parseInt(
                    System.getenv().getOrDefault("MATERIALIZER_METRICS_PORT", "9405")));
        var consumer = new KafkaConsumer<String, byte[]>(p)) {
      var writer =
          new ClickHouseWriter(
              endpoint,
              System.getenv().getOrDefault("CLICKHOUSE_USER", "risk"),
              Objects.requireNonNull(
                  System.getenv("CLICKHOUSE_PASSWORD"), "CLICKHOUSE_PASSWORD required"),
              metrics);
      consumer.subscribe(List.of(c.topic("risk.decisions")));
      long lastProbe = 0;
      while (!Thread.currentThread().isInterrupted()) {
        var records = consumer.poll(Duration.ofSeconds(1));
        metrics.lastPoll.set(System.currentTimeMillis());
        if (records.isEmpty()) {
          if (System.currentTimeMillis() - lastProbe > 15000) {
            writer.ping();
            lastProbe = System.currentTimeMillis();
          }
          continue;
        }
        var evaluations = new StringBuilder();
        var rejections = new StringBuilder();
        long rejected = 0;
        long decodeDeadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        for (var record : records) {
          if (System.nanoTime() > decodeDeadline)
            throw new java.io.IOException("Decode budget exceeded; offsets remain uncommitted");
          // Registry/network outages propagate; they are not malformed payment records.
          try {
            var decision = (ObjectNode) Json.read(codec.decode(record.value(), "risk-decision"));
            evaluations
                .append(Json.write(MaterializerRows.convert(decision, record).evaluation()))
                .append('\n');
          } catch (IllegalArgumentException | org.apache.avro.AvroRuntimeException e) {
            rejections
                .append(Json.write(MaterializerRows.rejection(record, "INVALID_DECISION")))
                .append('\n');
            rejected++;
          }
        }
        persist(writer, evaluations.toString(), rejections.toString(), consumer::commitSync);
        metrics.committed.addAndGet(records.count());
        metrics.rejected.addAndGet(rejected);
        long lag = 0;
        for (var partition : consumer.assignment())
          lag = Math.max(lag, consumer.currentLag(partition).orElse(0));
        metrics.lag.set(lag);
      }
    }
  }

  // If either insert fails or the process dies before commit, every row is replayable.
  public static void persist(
      ClickHouseWriter writer, String evaluations, String rejections, Runnable commit)
      throws java.io.IOException, InterruptedException {
    writer.insert("risk.evaluations", evaluations);
    writer.insert("risk.materializer_rejections", rejections);
    commit.run();
  }
}
