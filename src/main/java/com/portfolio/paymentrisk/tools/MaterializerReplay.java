package com.portfolio.paymentrisk.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.portfolio.paymentrisk.*;
import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.serialization.AvroCodec;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;

/** Bounded decision-log replay. Never feeds historical payments into live customer state. */
public final class MaterializerReplay {
  private MaterializerReplay() {}

  public static void main(String[] args) throws Exception {
    boolean execute = false;
    String reason = "";
    long maxRecords = 100000, rate = 500;
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--execute" -> execute = true;
        case "--reason" -> reason = args[++i];
        case "--max-records" -> maxRecords = Long.parseLong(args[++i]);
        case "--rate" -> rate = Long.parseLong(args[++i]);
        default -> throw new IllegalArgumentException("Unknown replay option");
      }
    }
    if ((execute && reason.isBlank())
        || reason.length() > 256
        || maxRecords < 1
        || maxRecords > 1000000
        || rate < 1
        || rate > 5000)
      throw new IllegalArgumentException("Replay requires bounded limits and an execution reason");
    var c = AppConfig.fromEnv();
    var p = c.kafkaProperties();
    String replayId = UUID.randomUUID().toString();
    p.put("bootstrap.servers", c.bootstrap());
    p.put("group.id", "risk-replay-" + replayId);
    p.put("enable.auto.commit", "false");
    p.put("isolation.level", "read_committed");
    p.put("auto.offset.reset", "none");
    p.put("max.poll.records", "100");
    p.put("key.deserializer", StringDeserializer.class.getName());
    p.put("value.deserializer", ByteArrayDeserializer.class.getName());
    var report =
        Json.object()
            .put("replay_id", replayId)
            .put("execute", execute)
            .put("reason", reason)
            .put("max_records", maxRecords)
            .put("records_per_second", rate)
            .put("records", 0)
            .put("rejected", 0)
            .put("status", "STARTED");
    Path audit = Path.of("artifacts", "replay-" + replayId + ".json");
    Files.createDirectories(audit.getParent());
    Files.writeString(audit, Json.write(report));
    try (var consumer = new KafkaConsumer<String, byte[]>(p);
        var metrics = new MaterializerMetrics(0)) {
      String topic = c.topic("risk.decisions");
      var partitions =
          consumer.partitionsFor(topic).stream()
              .map(x -> new TopicPartition(topic, x.partition()))
              .toList();
      consumer.assign(partitions);
      var starts = consumer.beginningOffsets(partitions);
      var ends = consumer.endOffsets(partitions); // last stable offsets under read_committed
      var ranges = report.putArray("ranges");
      for (var partition : partitions) {
        consumer.seek(partition, starts.get(partition));
        ranges
            .addObject()
            .put("topic", topic)
            .put("partition", partition.partition())
            .put("start", starts.get(partition))
            .put("end_exclusive", ends.get(partition));
      }
      Files.writeString(audit, Json.write(report));
      var codec = new AvroCodec(c.registry());
      String endpoint = System.getenv().getOrDefault("CLICKHOUSE_URL", "http://localhost:28123");
      if (execute && !c.environment().equals("local") && !endpoint.startsWith("https://"))
        throw new IllegalArgumentException("ClickHouse TLS required");
      var writer =
          execute
              ? new ClickHouseWriter(
                  endpoint,
                  System.getenv().getOrDefault("CLICKHOUSE_USER", "risk"),
                  Objects.requireNonNull(System.getenv("CLICKHOUSE_PASSWORD")),
                  metrics)
              : null;
      long records = 0, rejected = 0;
      var positions = new TreeSet<String>();
      long deadline = System.nanoTime() + Duration.ofMinutes(10).toNanos();
      while (true) {
        boolean complete = true;
        for (var partition : partitions) {
          if (consumer.position(partition) < ends.get(partition)) complete = false;
          else consumer.pause(List.of(partition));
        }
        if (complete) break;
        if (System.nanoTime() > deadline) throw new IllegalStateException("Replay timed out");
        long started = System.nanoTime();
        var batch = consumer.poll(Duration.ofMillis(500));
        var rows = new StringBuilder();
        var failures = new StringBuilder();
        long batchSize = 0;
        for (var record : batch) {
          if (record.offset() >= ends.get(new TopicPartition(record.topic(), record.partition())))
            continue;
          if (System.nanoTime() > deadline) throw new IllegalStateException("Replay timed out");
          positions.add(record.topic() + ":" + record.partition() + ":" + record.offset());
          if (++records > maxRecords)
            throw new IllegalStateException(
                "Replay record limit exceeded; partial writes may exist");
          batchSize++;
          try {
            rows.append(
                    Json.write(
                        MaterializerRows.convert(
                                (ObjectNode)
                                    Json.read(codec.decode(record.value(), "risk-decision")),
                                record)
                            .evaluation()))
                .append('\n');
          } catch (IllegalArgumentException | org.apache.avro.AvroRuntimeException e) {
            failures
                .append(Json.write(MaterializerRows.rejection(record, "INVALID_DECISION")))
                .append('\n');
            rejected++;
          }
        }
        if (execute)
          DecisionMaterializer.persist(writer, rows.toString(), failures.toString(), () -> {});
        long wait = batchSize * 1000 / rate - (System.nanoTime() - started) / 1000000;
        if (wait > 0) Thread.sleep(wait);
        report.put("records", records).put("rejected", rejected);
        Files.writeString(audit, Json.write(report));
      }
      report.put(
          "source_positions_sha256",
          com.portfolio.paymentrisk.domain.Audit.sha256(String.join("\n", positions)));
      report.put("status", "COMPLETE");
    } catch (Exception e) {
      report.put("status", "FAILED").put("failure_type", e.getClass().getSimpleName());
      throw e;
    } finally {
      Files.writeString(audit, Json.write(report));
      System.out.println(Json.write(report));
    }
  }
}
