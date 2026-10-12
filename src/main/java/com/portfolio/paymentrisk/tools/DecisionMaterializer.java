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
    var limits = BatchLimits.from(System.getenv());
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
        var consumer = new KafkaConsumer<String, byte[]>(p);
        var writer = new PostgresWriter(System.getenv(), metrics)) {
      KafkaIngestion.subscribe(
          consumer,
          List.of(c.topic("risk.decisions"), c.topic("payments.dlq"), c.topic("payments.late")),
          c.environment().equals("local"));
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
            new DurableKafkaBatch(writer, consumer::commitSync, metrics, limits, System::nanoTime);
        long decodeDeadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        for (var record : records) {
          if (System.nanoTime() > decodeDeadline)
            throw new java.io.IOException("Decode budget exceeded; offsets remain uncommitted");
          // Registry/network outages propagate; they are not malformed payment records.
          String table, row;
          long decodeStarted = System.nanoTime();
          try {
            if (record.topic().equals(c.topic("risk.decisions"))) {
              var decision = (ObjectNode) Json.read(codec.decode(record.value(), "risk-decision"));
              table = "risk.evaluations";
              row = Json.write(MaterializerRows.convert(decision, record));
            } else {
              boolean invalid = record.topic().equals(c.topic("payments.dlq"));
              var outcome =
                  Json.read(codec.decode(record.value(), invalid ? "dead-letter" : "late-event"));
              table = "risk.processing_outcomes";
              row = Json.write(ProcessingOutcomeRows.convert(outcome, record, invalid));
            }
          } catch (IllegalArgumentException | org.apache.avro.AvroRuntimeException e) {
            table = "risk.materializer_rejections";
            row =
                Json.write(
                    MaterializerRows.rejection(
                        record,
                        record.topic().equals(c.topic("risk.decisions"))
                            ? "INVALID_DECISION"
                            : "INVALID_OUTCOME"));
          } finally {
            metrics.decodeNanos.addAndGet(System.nanoTime() - decodeStarted);
            metrics.decoded.incrementAndGet();
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
