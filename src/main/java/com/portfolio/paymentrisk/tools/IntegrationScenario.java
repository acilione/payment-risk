package com.portfolio.paymentrisk.tools;

import com.portfolio.paymentrisk.*;
import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.serialization.AvroCodec;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

/** Real Kafka/Flink/registry acceptance; consumes committed records and reconciles business IDs. */
public final class IntegrationScenario {
  private IntegrationScenario() {}

  public static void main(String[] args) throws Exception {
    var c = AppConfig.fromEnv();
    var codec = new AvroCodec(c.registry());
    var p = c.kafkaProperties();
    p.put("bootstrap.servers", c.bootstrap());
    p.put("key.serializer", StringSerializer.class.getName());
    p.put("value.serializer", ByteArraySerializer.class.getName());
    p.put("acks", "all");
    p.put("enable.idempotence", "true");
    String run = "it-" + UUID.randomUUID();
    long base = System.currentTimeMillis();
    var expected = new HashSet<String>();
    var received = new HashSet<String>();
    int duplicates = 0;
    boolean dlq = false, late = false;
    long malformedOffset;
    try (var producer = new KafkaProducer<String, byte[]>(p)) {
      // Six customer transactions, intentionally reversed inside the out-of-order allowance.
      for (int i = 5; i >= 0; i--) {
        var t =
            transaction(
                run + "-" + i,
                run,
                base + i * 20,
                90_000,
                "d" + i,
                i < 4 ? "DECLINED" : "APPROVED");
        byte[] value = codec.encode(c.topic("payments.raw"), "transaction", Json.write(t));
        expected.add(t.path("transaction_id").asText());
        producer.send(new ProducerRecord<>(c.topic("payments.raw"), 0, run, value)).get();
        producer.send(new ProducerRecord<>(c.topic("payments.raw"), 0, run, value)).get();
      }
      malformedOffset =
          producer
              .send(new ProducerRecord<>(c.topic("payments.raw"), 0, run, new byte[] {99}))
              .get()
              .offset();
      // Keep every partition advancing, including after worker recovery.
      for (int round = 0; round < 15; round++) {
        for (int partition = 0; partition < 3; partition++) {
          var marker =
              transaction(
                  run + "-marker-" + round + "-" + partition,
                  run + "-marker-" + partition,
                  base + 20_000 + round * 1000,
                  1,
                  "d",
                  "APPROVED");
          producer
              .send(
                  new ProducerRecord<>(
                      c.topic("payments.raw"),
                      partition,
                      marker.path("customer_id").asText(),
                      codec.encode(c.topic("payments.raw"), "transaction", Json.write(marker))))
              .get();
        }
        Thread.sleep(1000);
      }
    }
    var cp = c.kafkaProperties();
    cp.put("bootstrap.servers", c.bootstrap());
    cp.put("group.id", run);
    cp.put("enable.auto.commit", "false");
    cp.put("auto.offset.reset", "earliest");
    cp.put("isolation.level", "read_committed");
    cp.put("key.deserializer", StringDeserializer.class.getName());
    cp.put("value.deserializer", ByteArrayDeserializer.class.getName());
    long deadline = System.nanoTime() + Duration.ofSeconds(150).toNanos();
    long doneAt = Long.MAX_VALUE;
    boolean lateSent = false;
    try (var consumer = new KafkaConsumer<String, byte[]>(cp);
        var producer = new KafkaProducer<String, byte[]>(p)) {
      consumer.subscribe(
          List.of(c.topic("risk.decisions"), c.topic("payments.dlq"), c.topic("payments.late")));
      while (System.nanoTime() < deadline && System.nanoTime() < doneAt) {
        for (var r : consumer.poll(Duration.ofSeconds(1))) {
          String schema =
              r.topic().equals(c.topic("risk.decisions"))
                  ? "risk-decision"
                  : r.topic().equals(c.topic("payments.dlq")) ? "dead-letter" : "late-event";
          var record = Json.read(codec.decode(r.value(), schema));
          if (r.topic().equals(c.topic("risk.decisions"))
              && record.path("customer_id").asText().equals(run)) {
            String id = record.path("transaction_id").asText();
            if (!received.add(id)) duplicates++;
            if (id.equals("txn-" + run + "-5")
                && (record.path("risk_score").asInt() != 100
                    || record.path("matched_rules").size() != 5))
              throw new AssertionError("Sixth event must match all five rules: " + record);
          }
          if (r.topic().equals(c.topic("payments.dlq"))
              && record.path("source_topic").asText().equals(c.topic("payments.raw"))
              && record.path("source_partition").asInt() == 0
              && record.path("source_offset").asLong() == malformedOffset
              && record.path("raw_payload").asText().equals("Yw==")) dlq = true;
          if (r.topic().equals(c.topic("payments.late"))
              && record.path("customer_id").asText().equals(run)) late = true;
        }
        // Prove finalization before injecting late data. Wall-clock sleeps cannot prove
        // watermark progress while a worker is down and its input is accumulating.
        if (received.equals(expected) && !lateSent) {
          var old = transaction(run + "-late", run, base - 3_600_000, 1, "d", "APPROVED");
          producer
              .send(
                  new ProducerRecord<>(
                      c.topic("payments.raw"),
                      0,
                      run,
                      codec.encode(c.topic("payments.raw"), "transaction", Json.write(old))))
              .get();
          lateSent = true;
        }
        if (received.equals(expected) && dlq && late && doneAt == Long.MAX_VALUE)
          doneAt = System.nanoTime() + Duration.ofSeconds(15).toNanos();
      }
    }
    if (!received.equals(expected) || duplicates != 0 || !dlq || !late)
      throw new AssertionError(
          "Reconciliation failed expected="
              + expected
              + " received="
              + received
              + " duplicates="
              + duplicates
              + " dlq="
              + dlq
              + " late="
              + late);
    System.out.println(
        Json.write(
            Map.of(
                "result",
                "PASS",
                "run_id",
                run,
                "expected",
                expected.size(),
                "committed",
                received.size(),
                "duplicates",
                duplicates,
                "dlq",
                dlq,
                "late",
                late)));
  }

  static com.fasterxml.jackson.databind.node.ObjectNode transaction(
      String id, String customer, long t, long amount, String device, String status) {
    return Json.object()
        .put("event_id", id)
        .put("transaction_id", "txn-" + id)
        .put("customer_id", customer)
        .put("merchant_id", "synthetic-merchant")
        .put("amount_minor", amount)
        .put("currency", "EUR")
        .put("country", "IT")
        .put("device_id", device)
        .put("status", status)
        .put("event_time", t)
        .put("producer_time", System.currentTimeMillis());
  }
}
