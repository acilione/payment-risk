package com.portfolio.paymentrisk.tools;

import com.portfolio.paymentrisk.*;
import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.domain.Rules;
import com.portfolio.paymentrisk.serialization.AvroCodec;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

/** Small operational CLI. All generated identifiers and values are synthetic. */
public final class PlatformCli {
  private PlatformCli() {}

  public static void main(String[] args) throws Exception {
    var c = AppConfig.fromEnv();
    var codec = new AvroCodec(c.registry());
    var p = c.kafkaProperties();
    p.put("bootstrap.servers", c.bootstrap());
    String command = args.length == 0 ? "help" : args[0];
    if (command.equals("bootstrap")) {
      try (var admin = Admin.create(p)) {
        var existing = admin.listTopics().names().get();
        var topics = new ArrayList<NewTopic>();
        for (String topic :
            List.of(
                c.topic("payments.raw"),
                c.topic("risk.rules"),
                c.topic("risk.decisions"),
                c.topic("payments.dlq"),
                c.topic("payments.late"))) {
          var settings = new HashMap<String, String>();
          settings.put("retention.ms", "604800000");
          settings.put("min.insync.replicas", System.getenv().getOrDefault("KAFKA_MIN_ISR", "1"));
          if (topic.equals(c.topic("risk.rules"))) settings.put("cleanup.policy", "compact");
          if (!existing.contains(topic))
            topics.add(
                new NewTopic(
                        topic,
                        topic.equals(c.topic("risk.rules")) ? 1 : 3,
                        Short.parseShort(
                            System.getenv().getOrDefault("KAFKA_REPLICATION_FACTOR", "1")))
                    .configs(settings));
        }
        admin.createTopics(topics).all().get();
      }
      String[] topics = {
        c.topic("payments.raw"),
        c.topic("risk.rules"),
        c.topic("risk.decisions"),
        c.topic("payments.dlq"),
        c.topic("payments.late")
      };
      String[] schemas = {"transaction", "risk-rule", "risk-decision", "dead-letter", "late-event"};
      for (int i = 0; i < topics.length; i++) codec.register(topics[i], schemas[i]);
      return;
    }
    p.put("key.serializer", StringSerializer.class.getName());
    p.put("value.serializer", ByteArraySerializer.class.getName());
    p.put("enable.idempotence", "true");
    p.put("acks", "all");
    p.put("compression.type", "zstd");
    try (var producer = new KafkaProducer<String, byte[]>(p)) {
      if (command.equals("rule")) {
        var rule = Json.read(Files.readString(Path.of(args[1])));
        Rules.validate(rule, c);
        producer
            .send(
                new ProducerRecord<>(
                    c.topic("risk.rules"),
                    rule.path("rule_id").asText(),
                    codec.encode(c.topic("risk.rules"), "risk-rule", Json.write(rule))))
            .get();
        return;
      }
      if (!command.equals("generate"))
        throw new IllegalArgumentException(
            "Usage: bootstrap | rule FILE | generate SCENARIO COUNT RATE [RUN_ID]");
      String scenario = args.length > 1 ? args[1] : "steady";
      int count = args.length > 2 ? Integer.parseInt(args[2]) : 1000;
      int rate = args.length > 3 ? Integer.parseInt(args[3]) : 100;
      if (count < 1
          || rate < 1
          || !Set.of(
                  "steady",
                  "burst",
                  "hot-key",
                  "high-cardinality",
                  "duplicates",
                  "out-of-order",
                  "late",
                  "malformed",
                  "fixture")
              .contains(scenario))
        throw new IllegalArgumentException("Invalid generator arguments");
      String run = args.length > 4 ? args[4] : UUID.randomUUID().toString();
      long start = System.currentTimeMillis(), begun = System.nanoTime();
      var random = new Random(42);
      for (int i = 0; i < count; i++) {
        long t = start + i * 1000L / rate;
        if (scenario.equals("late")) t -= 3_600_000;
        if (scenario.equals("out-of-order") && i % 2 == 0) t += 100;
        var tx =
            Json.object()
                .put("event_id", run + "-evt-" + i)
                .put("transaction_id", run + "-txn-" + i)
                .put(
                    "customer_id",
                    run
                        + "-customer-"
                        + (Set.of("hot-key", "fixture", "burst").contains(scenario)
                            ? 0
                            : scenario.equals("high-cardinality") ? i : i % 100))
                .put("merchant_id", "merchant-" + random.nextInt(20))
                .put(
                    "amount_minor",
                    scenario.equals("fixture") ? 90_000 : 100 + random.nextInt(100_000))
                .put("currency", "EUR")
                .put("country", "IT")
                .put("device_id", "device-" + (i % 3))
                .put("status", scenario.equals("fixture") && i < 4 ? "DECLINED" : "APPROVED")
                .put("event_time", t)
                .put("producer_time", System.currentTimeMillis());
        byte[] value =
            scenario.equals("malformed")
                ? "not-avro".getBytes(StandardCharsets.UTF_8)
                : codec.encode(c.topic("payments.raw"), "transaction", Json.write(tx));
        producer
            .send(
                new ProducerRecord<>(
                    c.topic("payments.raw"), tx.path("customer_id").asText(), value))
            .get();
        if (scenario.equals("duplicates"))
          producer
              .send(
                  new ProducerRecord<>(
                      c.topic("payments.raw"), tx.path("customer_id").asText(), value))
              .get();
        long delay = (i + 1) * 1_000_000_000L / rate - (System.nanoTime() - begun);
        if (delay > 0) java.util.concurrent.TimeUnit.NANOSECONDS.sleep(delay);
      }
      producer.flush();
      System.out.println(
          Json.write(
              Map.of(
                  "run_id",
                  run,
                  "scenario",
                  scenario,
                  "events",
                  count,
                  "elapsed_ms",
                  (System.nanoTime() - begun) / 1_000_000)));
    }
  }
}
