package com.portfolio.paymentrisk.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.portfolio.paymentrisk.Json;
import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.domain.*;
import com.portfolio.paymentrisk.serialization.AvroCodec;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;

/** Bounded, read-only transport audit and reference evaluation for generated pipeline tests. */
public final class PipelineAudit {
  private PipelineAudit() {}

  public static void main(String[] args) throws Exception {
    if (args.length == 3 && args[0].equals("read")) read(args[1], args[2]);
    else if (args.length == 2 && args[0].equals("verify")) {
      try (var input = Files.newBufferedReader(Path.of(args[1]), StandardCharsets.UTF_8)) {
        System.out.println(Json.write(verify(input)));
      }
    } else throw new IllegalArgumentException("read TOPIC SCHEMA|wire, or verify SORTED_JSONL");
  }

  private static void read(String topic, String schema) throws Exception {
    var config = AppConfig.fromEnv();
    var p = config.kafkaProperties();
    p.put("bootstrap.servers", config.bootstrap());
    p.put("key.deserializer", StringDeserializer.class.getName());
    p.put("value.deserializer", ByteArrayDeserializer.class.getName());
    p.put("enable.auto.commit", "false");
    p.put("isolation.level", "read_committed");
    p.put("max.poll.records", "500");
    var codec = new AvroCodec(config.registry());
    try (var consumer = new KafkaConsumer<String, byte[]>(p)) {
      var partitions =
          consumer.partitionsFor(topic).stream()
              .map(info -> new TopicPartition(topic, info.partition()))
              .toList();
      consumer.assign(partitions);
      var ends = consumer.endOffsets(partitions);
      consumer.seekToBeginning(partitions);
      long deadline = System.nanoTime() + Duration.ofMinutes(5).toNanos();
      while (true) {
        boolean complete = true;
        for (var partition : partitions)
          if (consumer.position(partition) < ends.get(partition)) complete = false;
        if (complete) break;
        if (System.nanoTime() > deadline) throw new IllegalStateException("Audit timed out");
        for (var record : consumer.poll(Duration.ofMillis(250))) {
          if (record.offset() >= ends.get(new TopicPartition(topic, record.partition()))) continue;
          if (record.value() == null) throw new IllegalStateException("Unexpected tombstone");
          var row =
              Json.object()
                  .put("partition", record.partition())
                  .put("offset", record.offset())
                  .put("payload_sha256", Audit.sha256(record.value()));
          if (!schema.equals("wire"))
            row.set("value", Json.read(codec.decode(record.value(), schema)));
          System.out.println(Json.write(row));
        }
      }
    }
  }

  public static ObjectNode verify(BufferedReader input) throws IOException {
    var history = new ArrayList<JsonNode>();
    var devices = new HashMap<String, Long>();
    String customer = "", previousEvent = "";
    long previousTime = -1, checked = 0;
    var labels = new TreeMap<String, Long>();
    String line;
    while ((line = input.readLine()) != null) {
      var pair = Json.read(line);
      var payment = pair.path("payment");
      var actual = pair.path("decision");
      String current = payment.path("customer_id").asText();
      long time = payment.path("event_time").asLong();
      String event = payment.path("event_id").asText();
      if (!current.equals(customer)) {
        if (current.compareTo(customer) <= 0)
          throw new IllegalArgumentException("Unsorted customers");
        history.clear();
        devices.clear();
        previousTime = -1;
        previousEvent = "";
        customer = current;
      }
      if (time < previousTime || (time == previousTime && event.compareTo(previousEvent) <= 0))
        throw new IllegalArgumentException("Unsorted or repeated customer payment");
      if (history.size() >= 100)
        throw new IllegalArgumentException("Generated customer exceeds 100 payments");
      var expected =
          RiskEngine.evaluate(
              payment,
              history,
              devices,
              Rules.defaults(),
              30,
              70,
              actual.path("processed_at").asLong());
      if (!Audit.canonical(expected).equals(Audit.canonical(actual)))
        throw new AssertionError("Reference assessment differs for " + event);
      history.add(payment);
      devices.put(payment.path("device_id").asText(), time);
      previousTime = time;
      previousEvent = event;
      checked++;
      labels.merge(actual.path("decision").asText(), 1L, Long::sum);
    }
    if (checked == 0) throw new IllegalArgumentException("No assessments to verify");
    var result =
        Json.object()
            .put("verified", true)
            .put("assessments", checked)
            .put("maximum_retained_customer_payments", 100);
    result.set("labels", Json.MAPPER.valueToTree(labels));
    return result;
  }
}
