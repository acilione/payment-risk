package com.portfolio.paymentrisk.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.portfolio.paymentrisk.*;
import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.serialization.AvroCodec;
import com.portfolio.paymentrisk.storage.*;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;

/** Committed Kafka records -> durable deliveries/quarantine -> synchronous offset commit. */
public final class DecisionMaterializer {
  private DecisionMaterializer() {}

  public static void main(String[] args) throws Exception {
    var c = AppConfig.fromEnv();
    var p =
        KafkaIngestion.properties(
            c,
            System.getenv().getOrDefault("MATERIALIZER_GROUP", "risk-clickhouse-evaluations-v2"),
            false);
    var codec = new AvroCodec(c.registry());
    try (var metrics =
            new MaterializerMetrics(
                Integer.parseInt(
                    System.getenv().getOrDefault("MATERIALIZER_METRICS_PORT", "9405")));
        var consumer = new KafkaConsumer<String, byte[]>(p)) {
      var writer = KafkaIngestion.writer(c, metrics);
      KafkaIngestion.subscribe(
          consumer, c.topic("risk.decisions"), c.environment().equals("local"));
      long lastProbe = 0;
      while (!Thread.currentThread().isInterrupted()) {
        var records = consumer.poll(Duration.ofSeconds(1));
        metrics.lastPoll.set(System.currentTimeMillis());
        if (System.currentTimeMillis() - lastProbe > 15000) {
          writer.inspectIntegrity();
          lastProbe = System.currentTimeMillis();
        }
        if (records.isEmpty()) continue;
        var batch =
            new DurableKafkaBatch(
                writer, consumer::commitSync, metrics, DurableKafkaBatch.DEFAULT_BYTES);
        long decodeDeadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        for (var record : records) {
          if (System.nanoTime() > decodeDeadline)
            throw new java.io.IOException("Decode budget exceeded; offsets remain uncommitted");
          // Registry/network outages propagate; they are not malformed payment records.
          String table, row;
          try {
            var decision = (ObjectNode) Json.read(codec.decode(record.value(), "risk-decision"));
            table = "risk.evaluations";
            row = Json.write(MaterializerRows.convert(decision, record));
          } catch (IllegalArgumentException | org.apache.avro.AvroRuntimeException e) {
            table = "risk.materializer_rejections";
            row = Json.write(MaterializerRows.rejection(record, "INVALID_DECISION"));
          }
          batch.add(table, row, record);
        }
        batch.flush();
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
