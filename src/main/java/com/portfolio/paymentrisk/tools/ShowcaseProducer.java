package com.portfolio.paymentrisk.tools;

import com.portfolio.paymentrisk.*;
import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.domain.Audit;
import com.portfolio.paymentrisk.serialization.AvroCodec;
import com.portfolio.paymentrisk.tools.generation.*;
import java.nio.file.*;
import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;

/** Sends generated inputs only; classification is performed by the running Flink job. */
public final class ShowcaseProducer {
  public static void main(String[] args) throws Exception {
    if (args.length != 2) throw new IllegalArgumentException("Usage: CONFIG_JSON RUN_ID");
    var configJson = Json.read(Files.readString(Path.of(args[0])));
    var generatorConfig = GeneratorConfig.parse(configJson);
    if (args[1].equals("--validate")) {
      System.out.println("Generator configuration valid");
      return;
    }
    var c = AppConfig.fromEnv();
    if (!c.environment().equals("local"))
      throw new IllegalArgumentException("Showcase requires local synthetic deployment");
    long anchor = System.currentTimeMillis();
    var generator = new GeneratedPayments(generatorConfig, args[1], anchor);
    var props = c.kafkaProperties();
    props.put("bootstrap.servers", c.bootstrap());
    List<TopicPartition> partitions;
    try (var admin = Admin.create(props)) {
      var description =
          admin
              .describeTopics(List.of(c.topic("payments.raw")))
              .allTopicNames()
              .get()
              .get(c.topic("payments.raw"));
      partitions =
          description.partitions().stream()
              .map(p -> new TopicPartition(description.name(), p.partition()))
              .toList();
      var offsets = new HashMap<TopicPartition, OffsetSpec>();
      partitions.forEach(p -> offsets.put(p, OffsetSpec.latest()));
      if (admin.listOffsets(offsets).all().get().values().stream().anyMatch(o -> o.offset() != 0))
        throw new IllegalStateException(
            "Use fresh showcase topics and a new job; existing input is preserved");
    }
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("value.serializer", ByteArraySerializer.class.getName());
    props.put("acks", "all");
    props.put("enable.idempotence", "true");
    props.put("compression.type", "zstd");
    var codec = new AvroCodec(c.registry());
    var start =
        Json.object()
            .put("kind", "start")
            .put("run_id", args[1])
            .put("generator_version", GeneratedPayments.VERSION)
            .put("event_time_anchor", anchor)
            .put("expected_payments", generator.size())
            .put("input_topic", c.topic("payments.raw"))
            .put("decision_topic", c.topic("risk.decisions"));
    start.set("config", configJson);
    start.set("customers", Json.MAPPER.valueToTree(generator.customers()));
    emit(start);
    long begun = System.nanoTime(), nextOffset = 0;
    int unique = 0, deliveries = 0;
    try (var producer = new KafkaProducer<String, byte[]>(props)) {
      for (var sample : generator) {
        var tx = sample.value();
        tx.put("producer_time", System.currentTimeMillis());
        var bytes = codec.encode(c.topic("payments.raw"), "transaction", Json.write(tx));
        for (int copy = 0; copy < (sample.retry() ? 2 : 1); copy++) {
          var metadata =
              producer
                  .send(
                      new ProducerRecord<>(
                          c.topic("payments.raw"), 0, tx.path("customer_id").asText(), bytes))
                  .get();
          nextOffset = metadata.offset() + 1;
          deliveries++;
          var row =
              Json.object()
                  .put("kind", "payment")
                  .put("retry", copy > 0)
                  .put("target", sample.target())
                  .put("source_partition", metadata.partition())
                  .put("source_offset", metadata.offset())
                  .put("payload_sha256", Audit.sha256(bytes));
          row.set("payment", tx);
          emit(row);
        }
        unique++;
        long delay = unique * 1000000000L / generatorConfig.rate() - (System.nanoTime() - begun);
        if (delay > 0) java.util.concurrent.TimeUnit.NANOSECONDS.sleep(delay);
      }
      IntegrationScenario.awaitSourceCheckpoint(c, nextOffset);
      // Every source split advances only after all generated history was checkpointed.
      for (var partition : partitions) {
        String id = "showcase-control-" + args[1] + "-" + partition.partition();
        var tx =
            IntegrationScenario.transaction(
                id, id, anchor + c.disorderMs() + 2000, 1, "control", "APPROVED");
        var bytes = codec.encode(c.topic("payments.raw"), "transaction", Json.write(tx));
        var m =
            producer
                .send(new ProducerRecord<>(partition.topic(), partition.partition(), id, bytes))
                .get();
        emit(
            Json.object()
                .put("kind", "marker")
                .put("source_partition", m.partition())
                .put("source_offset", m.offset())
                .put("payload_sha256", Audit.sha256(bytes)));
      }
    }
    emit(
        Json.object()
            .put("kind", "complete")
            .put("acknowledged_payments", unique)
            .put("acknowledged_deliveries", deliveries)
            .put("transport_retries", deliveries - unique)
            .put("control_records", partitions.size())
            .put("producer_elapsed_ms", (System.nanoTime() - begun) / 1000000));
  }

  private static void emit(Object row) {
    System.out.println(Json.write(row));
    System.out.flush();
  }
}
