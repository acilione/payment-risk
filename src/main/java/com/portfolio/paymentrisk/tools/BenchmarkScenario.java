package com.portfolio.paymentrisk.tools;

import com.portfolio.paymentrisk.*;
import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.serialization.AvroCodec;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

/** Bounded baseline measurement, including watermark and committed-output latency. */
public final class BenchmarkScenario {
  private BenchmarkScenario() {}

  public static void main(String[] args) throws Exception {
    int count = args.length > 0 ? Integer.parseInt(args[0]) : 1000;
    int rate = args.length > 1 ? Integer.parseInt(args[1]) : 100;
    if (count < 1 || count > 100_000 || rate < 1)
      throw new IllegalArgumentException("COUNT must be 1..100000 and RATE positive");
    var c = AppConfig.fromEnv();
    var pp = c.kafkaProperties();
    pp.put("bootstrap.servers", c.bootstrap());
    pp.put("key.serializer", StringSerializer.class.getName());
    pp.put("value.serializer", ByteArraySerializer.class.getName());
    pp.put("acks", "all");
    pp.put("enable.idempotence", "true");
    var cp = c.kafkaProperties();
    cp.put("bootstrap.servers", c.bootstrap());
    cp.put("key.deserializer", StringDeserializer.class.getName());
    cp.put("value.deserializer", ByteArrayDeserializer.class.getName());
    cp.put("group.id", "benchmark-" + UUID.randomUUID());
    cp.put("enable.auto.commit", "false");
    cp.put("isolation.level", "read_committed");
    String run = "bench-" + UUID.randomUUID();
    var sent = new ConcurrentHashMap<String, Long>();
    var received = new HashSet<String>();
    var visibleMs = new ArrayList<Double>();
    var finalizedMs = new ArrayList<Double>();
    var executor = Executors.newSingleThreadExecutor();
    long start = System.nanoTime();
    try (var consumer = new KafkaConsumer<String, byte[]>(cp)) {
      var partitions =
          consumer.partitionsFor(c.topic("risk.decisions")).stream()
              .map(p -> new org.apache.kafka.common.TopicPartition(p.topic(), p.partition()))
              .toList();
      consumer.assign(partitions);
      consumer.seekToEnd(partitions);
      for (var partition : partitions) consumer.position(partition);
      var production =
          executor.submit(
              () -> {
                var codec = new AvroCodec(c.registry());
                try (var producer = new KafkaProducer<String, byte[]>(pp)) {
                  long begun = System.nanoTime();
                  for (int i = 0; i < count; i++) {
                    String id = run + "-" + i;
                    var tx =
                        IntegrationScenario.transaction(
                            id,
                            run + "-customer-" + (i % 100),
                            System.currentTimeMillis(),
                            100,
                            "d" + (i % 3),
                            "APPROVED");
                    byte[] bytes =
                        codec.encode(c.topic("payments.raw"), "transaction", Json.write(tx));
                    sent.put(id, System.nanoTime());
                    producer
                        .send(
                            new ProducerRecord<>(
                                c.topic("payments.raw"),
                                i % 3,
                                tx.path("customer_id").asText(),
                                bytes))
                        .get();
                    long delay = (i + 1) * 1_000_000_000L / rate - (System.nanoTime() - begun);
                    if (delay > 0) TimeUnit.NANOSECONDS.sleep(delay);
                  }
                  long producedMs = (System.nanoTime() - begun) / 1_000_000;
                  // Real-time markers on every partition finalize the bounded workload's tail.
                  for (int round = 0; round < 25; round++) {
                    for (int partition = 0; partition < 3; partition++) {
                      String id = run + "-marker-" + round + "-" + partition;
                      var marker =
                          IntegrationScenario.transaction(
                              id, id, System.currentTimeMillis(), 1, "d", "APPROVED");
                      producer
                          .send(
                              new ProducerRecord<>(
                                  c.topic("payments.raw"),
                                  partition,
                                  id,
                                  codec.encode(
                                      c.topic("payments.raw"), "transaction", Json.write(marker))))
                          .get();
                    }
                    Thread.sleep(1000);
                  }
                  return producedMs;
                }
              });
      var codec = new AvroCodec(c.registry());
      int duplicates = 0;
      long end = start + Duration.ofSeconds(180 + count / rate).toNanos();
      long completedAt = Long.MAX_VALUE;
      while (System.nanoTime() < end && System.nanoTime() < completedAt) {
        if (production.isDone()) production.get();
        for (var record : consumer.poll(Duration.ofMillis(250))) {
          var decision = Json.read(codec.decode(record.value(), "risk-decision"));
          String id = decision.path("event_id").asText();
          Long at = sent.get(id);
          if (at == null) continue;
          if (!received.add(id)) {
            duplicates++;
            continue;
          }
          visibleMs.add((System.nanoTime() - at) / 1_000_000.0);
          finalizedMs.add(
              (double)
                  (decision.path("processed_at").asLong() - decision.path("event_time").asLong()));
        }
        if (received.size() == count && completedAt == Long.MAX_VALUE)
          completedAt = System.nanoTime() + Duration.ofSeconds(15).toNanos();
      }
      long producedMs = production.get(60, TimeUnit.SECONDS);
      if (received.size() != count || duplicates != 0)
        throw new AssertionError(
            "Benchmark reconciliation failed: received="
                + received.size()
                + " expected="
                + count
                + " duplicates="
                + duplicates);
      var report = new TreeMap<String, Object>();
      report.put("result", "PASS");
      report.put("run_id", run);
      report.put("acknowledged", sent.size());
      report.put("committed", received.size());
      report.put("duplicates", duplicates);
      report.put("requested_rate_per_second", rate);
      report.put("producer_elapsed_ms", producedMs);
      report.put("acknowledged_rate_per_second", count * 1000.0 / producedMs);
      report.put("arrival_to_committed_ms", percentiles(visibleMs));
      report.put("event_to_finalized_ms", percentiles(finalizedMs));
      System.out.println(Json.write(report));
    } finally {
      executor.shutdownNow();
    }
  }

  private static Map<String, Double> percentiles(List<Double> samples) {
    Collections.sort(samples);
    var result = new TreeMap<String, Double>();
    for (int p : List.of(50, 95, 99))
      result.put(
          "p" + p, samples.get(Math.max(0, (int) Math.ceil(samples.size() * p / 100.0) - 1)));
    return result;
  }
}
