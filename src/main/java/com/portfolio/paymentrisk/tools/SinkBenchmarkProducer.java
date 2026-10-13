package com.portfolio.paymentrisk.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.portfolio.paymentrisk.Json;
import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.domain.Audit;
import com.portfolio.paymentrisk.serialization.AvroCodec;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;

/** Storage-only load from bounded samples of real Flink output; never measures scoring accuracy. */
public final class SinkBenchmarkProducer {
  private SinkBenchmarkProducer() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 6)
      throw new IllegalArgumentException(
          "source-topic target-topic namespace records rate variant");
    String source = args[0], target = args[1], namespace = args[2], variant = args[5];
    int count = Integer.parseInt(args[3]), rate = Integer.parseInt(args[4]);
    if (!target.startsWith("bench.")
        || !namespace.matches("[a-zA-Z0-9_-]{1,64}")
        || count < 1
        || count > 2_000_000
        || rate < 0
        || rate > 100_000
        || !Set.of("initial", "retry", "new-event", "conflict").contains(variant))
      throw new IllegalArgumentException("Invalid benchmark arguments");
    var config = AppConfig.fromEnv();
    var properties = config.kafkaProperties();
    properties.put("bootstrap.servers", config.bootstrap());
    try (var admin = Admin.create(properties)) {
      var existing = admin.listTopics().names().get();
      var topics = new ArrayList<NewTopic>();
      for (String topic : List.of(target, target + ".dlq", target + ".late"))
        if (!existing.contains(topic)) topics.add(new NewTopic(topic, 3, (short) 1));
      if (!topics.isEmpty()) admin.createTopics(topics).all().get();
    }
    var codec = new AvroCodec(config.registry());
    codec.register(target, "risk-decision");
    var templates = templates(properties, codec, source);
    properties.put("key.serializer", StringSerializer.class.getName());
    properties.put("value.serializer", ByteArraySerializer.class.getName());
    properties.put("acks", "all");
    properties.put("enable.idempotence", "true");
    properties.put("compression.type", "zstd");
    properties.put("linger.ms", "5");
    properties.put("batch.size", "131072");
    properties.put("buffer.memory", "16777216");
    var failure = new AtomicReference<Exception>();
    var acknowledged = new AtomicLong();
    var inFlight = new Semaphore(512);
    long[] partitions = new long[3], drift = new long[120001];
    long amount = 0, score = 0;
    var labels = new TreeMap<String, Long>();
    long startedAt = System.currentTimeMillis(), start = System.nanoTime();
    long nextProgress = start + 1_000_000_000L;
    try (var producer = new KafkaProducer<String, byte[]>(properties)) {
      for (int i = 0; i < count; i++) {
        if (System.nanoTime() >= nextProgress) {
          System.out.println(
              Json.write(
                  Json.object()
                      .put("acknowledged", acknowledged.get())
                      .put("at_ms", System.currentTimeMillis())));
          nextProgress = System.nanoTime() + 1_000_000_000L;
        }
        long schedule = rate == 0 ? System.nanoTime() : start + i * 1_000_000_000L / rate;
        while (System.nanoTime() < schedule) {
          long wait = schedule - System.nanoTime();
          if (wait > 0) java.util.concurrent.locks.LockSupport.parkNanos(wait);
        }
        inFlight.acquire();
        if (failure.get() != null) throw failure.get();
        long delay = Math.max(0, (System.nanoTime() - schedule) / 1_000_000L);
        drift[(int) Math.min(delay, drift.length - 1)]++;
        ObjectNode row = assessment(templates.get(i % templates.size()), namespace, i, variant);
        amount = Math.addExact(amount, row.path("amount_minor").asLong());
        score += row.path("risk_score").asInt();
        labels.merge(row.path("decision").asText(), 1L, Long::sum);
        byte[] bytes = codec.encode(target, "risk-decision", Json.write(row));
        producer.send(
            new ProducerRecord<>(target, row.path("customer_id").asText(), bytes),
            (metadata, error) -> {
              if (error != null) failure.compareAndSet(null, error);
              else {
                partitions[metadata.partition()]++;
                acknowledged.incrementAndGet();
              }
              inFlight.release();
            });
      }
      producer.flush();
      if (failure.get() != null) throw failure.get();
    }
    long elapsed = (System.nanoTime() - start) / 1_000_000;
    long wallElapsed = System.currentTimeMillis() - startedAt;
    long cumulative = 0, p95 = 0;
    for (int i = 0; i < drift.length; i++) {
      cumulative += drift[i];
      if (cumulative >= Math.ceil(count * .95)) {
        p95 = i;
        break;
      }
    }
    var report =
        Json.object()
            .put("source_topic", source)
            .put("topic", target)
            .put("namespace", namespace)
            .put("variant", variant)
            .put("templates", templates.size())
            .put("records", count)
            .put("amount_minor", amount)
            .put("risk_score_sum", score)
            .put("started_at", startedAt)
            .put("elapsed_ms", elapsed)
            .put("wall_clock_elapsed_ms", wallElapsed)
            .put("wall_minus_monotonic_elapsed_ms", wallElapsed - elapsed)
            .put("requested_rate", rate)
            .put("achieved_records_per_second", count * 1000.0 / Math.max(1, elapsed))
            .put("schedule_delay_p95_ms_capped_at_120s", p95);
    report.set("records_by_partition", Json.MAPPER.valueToTree(partitions));
    report.set("decisions", Json.MAPPER.valueToTree(labels));
    System.out.println(Json.write(report));
  }

  private static List<ObjectNode> templates(Properties source, AvroCodec codec, String topic)
      throws Exception {
    var p = new Properties();
    p.putAll(source);
    p.put("key.deserializer", StringDeserializer.class.getName());
    p.put("value.deserializer", ByteArrayDeserializer.class.getName());
    p.put("enable.auto.commit", "false");
    p.put("isolation.level", "read_committed");
    p.put("max.poll.records", "128");
    var result = new ArrayList<ObjectNode>();
    try (var consumer = new KafkaConsumer<String, byte[]>(p)) {
      var partitions = new ArrayList<>(consumer.partitionsFor(topic));
      partitions.sort(Comparator.comparingInt(org.apache.kafka.common.PartitionInfo::partition));
      for (var info : partitions) {
        var partition = new TopicPartition(topic, info.partition());
        consumer.assign(List.of(partition));
        consumer.seekToBeginning(List.of(partition));
        long end = consumer.endOffsets(List.of(partition)).get(partition);
        int added = 0;
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (added < 128 && consumer.position(partition) < end) {
          if (System.nanoTime() > deadline)
            throw new IllegalStateException("Source sample timed out");
          for (var record : consumer.poll(Duration.ofMillis(250))) {
            if (added == 128) break;
            var row = (ObjectNode) Json.read(codec.decode(record.value(), "risk-decision"));
            MaterializerRows.convert(row, record);
            result.add(row);
            added++;
          }
        }
      }
    }
    if (result.isEmpty())
      throw new IllegalArgumentException("Source topic has no committed assessments");
    return result;
  }

  public static ObjectNode assessment(
      ObjectNode template, String namespace, long sequence, String variant) {
    var row = template.deepCopy();
    String transaction = namespace + "-" + sequence;
    row.put("transaction_id", transaction).put("decision_id", "risk_" + transaction);
    row.put("customer_id", namespace + "-" + template.path("customer_id").asText());
    row.put("event_id", transaction + (variant.equals("new-event") ? "-retry" : "-event"));
    row.put("processed_at", System.currentTimeMillis());
    var facts = (ObjectNode) Json.read(template.path("transaction_facts").asText());
    facts.put("transaction_id", transaction).put("customer_id", row.path("customer_id").asText());
    row.put("transaction_facts", Audit.canonical(facts));
    facts.put("event_id", row.path("event_id").asText());
    row.put("input_sha256", Audit.inputHash(facts));
    row.put(
        "evaluation_id",
        Audit.evaluationId(
            row.path("event_id").asText(),
            row.path("engine_version").asText(),
            row.path("policy_id").asText(),
            row.path("policy_version").asLong()));
    if (variant.equals("conflict"))
      row.put(
          "risk_score",
          row.path("risk_score").asInt() == 100 ? 99 : row.path("risk_score").asInt() + 1);
    return row;
  }
}
