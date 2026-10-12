package com.portfolio.paymentrisk.tools;

import com.portfolio.paymentrisk.*;
import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.domain.Audit;
import com.portfolio.paymentrisk.serialization.AvroCodec;
import com.portfolio.paymentrisk.tools.generation.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;

/** Sends generated inputs only; classification is performed by the running Flink job. */
public final class ShowcaseProducer {
  public static void main(String[] args) throws Exception {
    if (args.length < 2)
      throw new IllegalArgumentException("Usage: CONFIG_JSON RUN_ID [--include-outcomes] [--load]");
    var options = new HashSet<>(Arrays.asList(args).subList(2, args.length));
    if (!Set.of("--include-outcomes", "--load").containsAll(options))
      throw new IllegalArgumentException("Unknown producer option");
    boolean includeOutcomes = options.contains("--include-outcomes");
    boolean load = options.contains("--load");
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
    props.put("linger.ms", load ? "5" : "0");
    var codec = new AvroCodec(c.registry());
    var start =
        Json.object()
            .put("kind", "start")
            .put("load_mode", load)
            .put("max_in_flight", load ? 512 : 1)
            .put("payment_partitions", load ? partitions.size() : 1)
            .put("run_id", args[1])
            .put("generator_version", GeneratedPayments.VERSION)
            .put("event_time_anchor", anchor)
            .put("expected_payments", generator.size() + (includeOutcomes ? 1 : 0))
            .put("input_topic", c.topic("payments.raw"))
            .put("decision_topic", c.topic("risk.decisions"))
            .put("dlq_topic", c.topic("payments.dlq"))
            .put("late_topic", c.topic("payments.late"));
    start.set("config", configJson);
    start.set("customers", Json.MAPPER.valueToTree(generator.customers()));
    emit(start);
    long begun;
    long publishingStartedAt;
    var nextOffsets = new HashMap<TopicPartition, Long>();
    int unique = 0, deliveries = 0;
    long publishingMs = 0, finalEventTime = anchor;
    com.fasterxml.jackson.databind.JsonNode lastPayment = null;
    try (var producer = new KafkaProducer<String, byte[]>(props);
        var publisher =
            new BoundedPaymentPublisher(producer, load ? 512 : 1, ShowcaseProducer::emit)) {
      publishingStartedAt = System.currentTimeMillis();
      begun = System.nanoTime();
      emit(
          Json.object()
              .put("kind", "publishing")
              .put("publishing_started_at", publishingStartedAt));
      for (var sample : generator) {
        if (load) {
          long delay = unique * 1000000000L / generatorConfig.rate() - (System.nanoTime() - begun);
          if (delay > 0) java.util.concurrent.TimeUnit.NANOSECONDS.sleep(delay);
        }
        var tx = sample.value();
        lastPayment = tx;
        tx.put("producer_time", System.currentTimeMillis());
        // Load mode measures live arrival, not accelerated historical scenario effectiveness.
        if (load) tx.put("event_time", System.currentTimeMillis());
        finalEventTime = Math.max(finalEventTime, tx.path("event_time").asLong());
        var bytes = codec.encode(c.topic("payments.raw"), "transaction", Json.write(tx));
        for (int copy = 0; copy < (sample.retry() ? 2 : 1); copy++) {
          int partition =
              load
                  ? Math.floorMod(tx.path("customer_id").asText().hashCode(), partitions.size())
                  : 0;
          var row =
              Json.object()
                  .put("kind", "payment")
                  .put("retry", copy > 0)
                  .put("target", sample.target())
                  .put("payload_sha256", Audit.sha256(bytes));
          row.set("payment", tx);
          // Keep the offered schedule outside the payment contract. A blocked generator must
          // not hide queueing delay by moving the latency origin to its eventual send time.
          if (load)
            row.put("scheduled_at", publishingStartedAt + unique * 1000L / generatorConfig.rate());
          publisher.send(
              new ProducerRecord<>(
                  c.topic("payments.raw"), partition, tx.path("customer_id").asText(), bytes),
              row);
          deliveries++;
        }
        unique++;
        if (!load) {
          publisher.drain();
          long delay = unique * 1000000000L / generatorConfig.rate() - (System.nanoTime() - begun);
          if (delay > 0) java.util.concurrent.TimeUnit.NANOSECONDS.sleep(delay);
        }
      }
      publisher.drain();
      publishingMs = (System.nanoTime() - begun) / 1000000;
      nextOffsets.putAll(publisher.offsets());
      IntegrationScenario.awaitSourceCheckpoint(c, nextOffsets);
      // Every source split advances only after all generated history was checkpointed.
      for (var partition : partitions) {
        String id = "showcase-control-" + args[1] + "-" + partition.partition();
        var tx =
            IntegrationScenario.transaction(
                id, id, finalEventTime + c.disorderMs() + 2000, 1, "control", "APPROVED");
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
      if (includeOutcomes) {
        // A committed assessment proves finalization for this customer even while ClickHouse is
        // down.
        awaitAssessment(c, codec, lastPayment.path("event_id").asText());
        var late =
            IntegrationScenario.transaction(
                args[1] + "-payment-late",
                lastPayment.path("customer_id").asText(),
                lastPayment.path("event_time").asLong() - 1,
                100,
                "late-device",
                "APPROVED");
        var bytes = codec.encode(c.topic("payments.raw"), "transaction", Json.write(late));
        var metadata =
            producer
                .send(
                    new ProducerRecord<>(
                        c.topic("payments.raw"), 0, late.path("customer_id").asText(), bytes))
                .get();
        var row =
            Json.object()
                .put("kind", "late")
                .put("source_partition", metadata.partition())
                .put("source_offset", metadata.offset())
                .put("payload_sha256", Audit.sha256(bytes));
        row.set("payment", late);
        emit(row);
        // An undecodable input has no trustworthy event ID; reconciliation must use coordinates and
        // wire hash.
        byte[] invalid = new byte[] {99};
        metadata =
            producer
                .send(new ProducerRecord<>(c.topic("payments.raw"), 0, "invalid", invalid))
                .get();
        emit(
            Json.object()
                .put("kind", "invalid")
                .put("source_partition", metadata.partition())
                .put("source_offset", metadata.offset())
                .put("payload_sha256", Audit.sha256(invalid)));
      }
    }
    emit(
        Json.object()
            .put("kind", "complete")
            .put("acknowledged_payments", unique + (includeOutcomes ? 1 : 0))
            .put("acknowledged_deliveries", deliveries + (includeOutcomes ? 2 : 0))
            .put("transport_retries", deliveries - unique)
            .put("control_records", partitions.size())
            .put("publishing_started_at", publishingStartedAt)
            .put("publishing_elapsed_ms", publishingMs)
            .put("achieved_payments_per_second", unique * 1000.0 / Math.max(1, publishingMs))
            .put("achieved_deliveries_per_second", deliveries * 1000.0 / Math.max(1, publishingMs))
            .put("producer_elapsed_ms", (System.nanoTime() - begun) / 1000000));
  }

  private static void awaitAssessment(AppConfig config, AvroCodec codec, String eventId)
      throws Exception {
    var props =
        com.portfolio.paymentrisk.storage.KafkaIngestion.properties(
            config, "showcase-proof-" + UUID.randomUUID(), false);
    props.put("auto.offset.reset", "earliest");
    long deadline = System.nanoTime() + Duration.ofSeconds(180).toNanos();
    try (var consumer = new KafkaConsumer<String, byte[]>(props)) {
      consumer.subscribe(List.of(config.topic("risk.decisions")));
      while (System.nanoTime() < deadline) {
        for (var record : consumer.poll(Duration.ofSeconds(1))) {
          if (Json.read(codec.decode(record.value(), "risk-decision"))
              .path("event_id")
              .asText()
              .equals(eventId)) return;
        }
      }
    }
    throw new IllegalStateException(
        "Final assessment was not committed; cannot demonstrate a late input");
  }

  private static void emit(Object row) {
    System.out.println(Json.write(row));
    System.out.flush();
  }
}
