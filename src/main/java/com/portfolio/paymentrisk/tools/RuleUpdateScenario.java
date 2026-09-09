package com.portfolio.paymentrisk.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.portfolio.paymentrisk.*;
import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.domain.Rules;
import com.portfolio.paymentrisk.serialization.AvroCodec;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

/** Exercises live policy activation and disable without relying on fixed propagation sleeps. */
public final class RuleUpdateScenario {
  private RuleUpdateScenario() {}

  public static void main(String[] args) throws Exception {
    var c = AppConfig.fromEnv();
    var codec = new AvroCodec(c.registry());
    var pp = c.kafkaProperties();
    pp.put("bootstrap.servers", c.bootstrap());
    pp.put("key.serializer", StringSerializer.class.getName());
    pp.put("value.serializer", ByteArraySerializer.class.getName());
    pp.put("acks", "all");
    pp.put("enable.idempotence", "true");
    var cp = c.kafkaProperties();
    cp.put("bootstrap.servers", c.bootstrap());
    cp.put("group.id", "rules-it-" + UUID.randomUUID());
    cp.put("key.deserializer", StringDeserializer.class.getName());
    cp.put("value.deserializer", ByteArrayDeserializer.class.getName());
    cp.put("enable.auto.commit", "false");
    cp.put("auto.offset.reset", "earliest");
    cp.put("isolation.level", "read_committed");
    long version = System.currentTimeMillis();
    try (var producer = new KafkaProducer<String, byte[]>(pp);
        var consumer = new KafkaConsumer<String, byte[]>(cp)) {
      consumer.subscribe(List.of(c.topic("risk.decisions")));
      try {
        var rule = (ObjectNode) Rules.defaults().get(0);
        rule.put("version", version).put("threshold", 0).put("score", 70);
        publish(producer, codec, c, rule);
        awaitPolicy(producer, consumer, codec, c, rule, 70);
        rule.put("version", version + 1).put("enabled", false);
        publish(producer, codec, c, rule);
        awaitPolicy(producer, consumer, codec, c, rule, 0);
        var stale = rule.deepCopy().put("version", version).put("enabled", true);
        publish(producer, codec, c, stale);
        awaitPolicy(producer, consumer, codec, c, rule, 0);
      } finally {
        var restored = (ObjectNode) Rules.defaults().get(0);
        restored.put("version", version + 2);
        publish(producer, codec, c, restored);
        awaitPolicy(producer, consumer, codec, c, restored, 0);
      }
    }
    System.out.println(
        Json.write(
            Map.of(
                "result",
                "PASS",
                "scenario",
                "dynamic-rule-update-disable-stale-restore",
                "version",
                version)));
  }

  private static void publish(
      KafkaProducer<String, byte[]> p, AvroCodec codec, AppConfig c, ObjectNode r)
      throws Exception {
    p.send(
            new ProducerRecord<>(
                c.topic("risk.rules"),
                "R001",
                codec.encode(c.topic("risk.rules"), "risk-rule", Json.write(r))))
        .get();
  }

  private static void awaitPolicy(
      KafkaProducer<String, byte[]> p,
      KafkaConsumer<String, byte[]> consumer,
      AvroCodec codec,
      AppConfig c,
      ObjectNode rule,
      int score)
      throws Exception {
    var rules = Rules.defaults();
    rules.set(0, rule);
    String fingerprint = Rules.fingerprint(rules, c.review(), c.reject());
    for (int attempt = 0; attempt < 6; attempt++) {
      String id = "rule-probe-" + UUID.randomUUID();
      long now = System.currentTimeMillis();
      var tx = IntegrationScenario.transaction(id, id, now, 1, "device", "APPROVED");
      p.send(
              new ProducerRecord<>(
                  c.topic("payments.raw"),
                  0,
                  id,
                  codec.encode(c.topic("payments.raw"), "transaction", Json.write(tx))))
          .get();
      for (int partition = 0; partition < 3; partition++) {
        var marker =
            IntegrationScenario.transaction(
                id + "-mark-" + partition,
                id + "-mark-" + partition,
                now + c.disorderMs() + 2,
                1,
                "device",
                "APPROVED");
        p.send(
                new ProducerRecord<>(
                    c.topic("payments.raw"),
                    partition,
                    marker.path("customer_id").asText(),
                    codec.encode(c.topic("payments.raw"), "transaction", Json.write(marker))))
            .get();
      }
      long end = System.nanoTime() + Duration.ofSeconds(20).toNanos();
      while (System.nanoTime() < end) {
        for (var r : consumer.poll(Duration.ofSeconds(1))) {
          var decision = Json.read(codec.decode(r.value(), "risk-decision"));
          if (decision.path("event_id").asText().equals(id)
              && decision.path("rules_fingerprint").asText().equals(fingerprint)) {
            if (decision.path("risk_score").asInt() != score)
              throw new AssertionError("Unexpected live rule score: " + decision);
            return;
          }
        }
      }
    }
    throw new AssertionError("Policy fingerprint was not activated: " + fingerprint);
  }
}
