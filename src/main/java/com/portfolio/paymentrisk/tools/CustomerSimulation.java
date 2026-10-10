package com.portfolio.paymentrisk.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.portfolio.paymentrisk.Json;
import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.domain.Rules;
import com.portfolio.paymentrisk.serialization.AvroCodec;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;

/**
 * Replays customer timelines through the local Kafka/Flink pipeline and evaluates committed output.
 */
public final class CustomerSimulation {
  private CustomerSimulation() {}

  public static void main(String[] args) throws Exception {
    var c = AppConfig.fromEnv();
    if (!c.environment().equals("local")
        || c.review() != 30
        || c.reject() != 70
        || c.disorderMs() != 10_000
        || !c.policyJson().isBlank())
      throw new IllegalArgumentException(
          "Simulation requires local mode and the default dynamic policy and watermark allowance");
    var properties = c.kafkaProperties();
    properties.put("bootstrap.servers", c.bootstrap());
    List<TopicPartition> inputs = new ArrayList<>();
    // Historical warm-up cannot safely enter an existing live watermark. Never reset user data.
    try (var admin = Admin.create(properties)) {
      var topics =
          List.of(
              c.topic("payments.raw"),
              c.topic("risk.rules"),
              c.topic("risk.decisions"),
              c.topic("payments.late"),
              c.topic("payments.dlq"));
      var requests = new HashMap<TopicPartition, OffsetSpec>();
      for (var topic : admin.describeTopics(topics).allTopicNames().get().values()) {
        for (var partition : topic.partitions()) {
          var tp = new TopicPartition(topic.name(), partition.partition());
          requests.put(tp, OffsetSpec.latest());
          if (topic.name().equals(c.topic("payments.raw"))) inputs.add(tp);
        }
      }
      for (var offset : admin.listOffsets(requests).all().get().values())
        if (offset.offset() != 0)
          throw new IllegalStateException(
              "Simulation needs fresh local topics and a new job without restored state; existing data is preserved");
    }
    String run = "sim-" + UUID.randomUUID();
    long endTime = System.currentTimeMillis();
    var samples = CustomerScenarios.samples(run, endTime);
    var expected = new HashSet<String>();
    samples.forEach(s -> expected.add(s.payment().path("event_id").asText()));
    var codec = new AvroCodec(c.registry());
    var producerProperties = new Properties();
    producerProperties.putAll(properties);
    producerProperties.put("key.serializer", StringSerializer.class.getName());
    producerProperties.put("value.serializer", ByteArraySerializer.class.getName());
    producerProperties.put("acks", "all");
    producerProperties.put("enable.idempotence", "true");
    int retries = 0;
    try (var producer = new KafkaProducer<String, byte[]>(producerProperties)) {
      long nextOffset = 0;
      // One source partition, chronological history; this is behavior coverage, not a load test.
      for (var sample : samples) {
        var tx = sample.payment();
        var record =
            new ProducerRecord<>(
                c.topic("payments.raw"),
                0,
                tx.path("customer_id").asText(),
                codec.encode(c.topic("payments.raw"), "transaction", Json.write(tx)));
        nextOffset = producer.send(record).get().offset() + 1;
        if (sample.target()) {
          nextOffset = producer.send(record).get().offset() + 1;
          retries++;
        }
      }
      IntegrationScenario.awaitSourceCheckpoint(c, nextOffset);
      for (var tp : inputs) {
        var marker =
            IntegrationScenario.transaction(
                run + "-marker-" + tp.partition(),
                run + "-marker-" + tp.partition(),
                endTime + 20_000,
                1,
                "marker",
                "APPROVED");
        producer
            .send(
                new ProducerRecord<>(
                    tp.topic(),
                    tp.partition(),
                    marker.path("customer_id").asText(),
                    codec.encode(tp.topic(), "transaction", Json.write(marker))))
            .get();
      }
    }
    var consumerProperties = new Properties();
    consumerProperties.putAll(properties);
    consumerProperties.put("group.id", run);
    consumerProperties.put("enable.auto.commit", "false");
    consumerProperties.put("auto.offset.reset", "earliest");
    consumerProperties.put("isolation.level", "read_committed");
    consumerProperties.put("key.deserializer", StringDeserializer.class.getName());
    consumerProperties.put("value.deserializer", ByteArrayDeserializer.class.getName());
    var decisions = new ArrayList<JsonNode>();
    var received = new HashSet<String>();
    long deadline = System.nanoTime() + Duration.ofSeconds(150).toNanos();
    long doneAt = Long.MAX_VALUE;
    try (var consumer = new KafkaConsumer<String, byte[]>(consumerProperties)) {
      consumer.subscribe(
          List.of(c.topic("risk.decisions"), c.topic("payments.late"), c.topic("payments.dlq")));
      while (System.nanoTime() < deadline && System.nanoTime() < doneAt) {
        for (var record : consumer.poll(Duration.ofSeconds(1))) {
          if (!record.topic().equals(c.topic("risk.decisions")))
            throw new AssertionError(
                "Fresh simulation produced late or invalid input on " + record.topic());
          var decision = Json.read(codec.decode(record.value(), "risk-decision"));
          String id = decision.path("event_id").asText();
          if (!expected.contains(id)) continue; // Watermark markers are not labeled customers.
          if (!decision
              .path("rules_fingerprint")
              .asText()
              .equals(Rules.fingerprint(Rules.defaults(), 30, 70)))
            throw new AssertionError("Simulation ran with a different policy");
          if (!received.add(id)) throw new AssertionError("Duplicate committed decision: " + id);
          decisions.add(decision);
        }
        if (received.equals(expected) && doneAt == Long.MAX_VALUE)
          doneAt = System.nanoTime() + Duration.ofSeconds(15).toNanos();
      }
    }
    var report = CustomerScenarios.report(samples, decisions);
    report
        .put("run_id", run)
        .put("transport_retries", retries)
        .put("duplicate_decisions", 0)
        .put("watermark_markers", inputs.size());
    System.out.println(Json.write(report));
  }
}
