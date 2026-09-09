package com.portfolio.paymentrisk.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.portfolio.paymentrisk.*;
import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.serialization.AvroCodec;
import java.net.*;
import java.net.http.*;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;

/** Read committed -> synchronous ClickHouse batch insert -> synchronous Kafka commit. */
public final class DecisionMaterializer {
  private DecisionMaterializer() {}

  public static void main(String[] args) throws Exception {
    var c = AppConfig.fromEnv();
    var p = c.kafkaProperties();
    p.put("bootstrap.servers", c.bootstrap());
    p.put("group.id", System.getenv().getOrDefault("MATERIALIZER_GROUP", "risk-clickhouse-v1"));
    p.put("key.deserializer", StringDeserializer.class.getName());
    p.put("value.deserializer", ByteArrayDeserializer.class.getName());
    p.put("enable.auto.commit", "false");
    p.put("isolation.level", "read_committed");
    p.put("auto.offset.reset", "earliest");
    p.put("max.poll.records", "500");
    var codec = new AvroCodec(c.registry());
    var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    String endpoint = System.getenv().getOrDefault("CLICKHOUSE_URL", "http://localhost:28123");
    if (c.environment().equals("production") && !endpoint.startsWith("https://"))
      throw new IllegalArgumentException("ClickHouse TLS required");
    try (var consumer = new KafkaConsumer<String, byte[]>(p)) {
      consumer.subscribe(List.of(c.topic("risk.decisions")));
      while (!Thread.currentThread().isInterrupted()) {
        var records = consumer.poll(Duration.ofSeconds(1));
        if (records.isEmpty()) continue;
        var body = new StringBuilder("INSERT INTO risk.decisions FORMAT JSONEachRow\n");
        for (var r : records) {
          var row = (ObjectNode) Json.read(codec.decode(r.value(), "risk-decision"));
          row.put("source_partition", r.partition()).put("source_offset", r.offset());
          body.append(Json.write(row)).append('\n');
        }
        var request =
            HttpRequest.newBuilder(URI.create(endpoint))
                .timeout(Duration.ofSeconds(30))
                .header(
                    "X-ClickHouse-User", System.getenv().getOrDefault("CLICKHOUSE_USER", "risk"))
                .header(
                    "X-ClickHouse-Key",
                    Objects.requireNonNull(
                        System.getenv("CLICKHOUSE_PASSWORD"), "CLICKHOUSE_PASSWORD required"))
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                .build();
        var response = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2)
          throw new IllegalStateException("ClickHouse insert failed HTTP " + response.statusCode());
        consumer.commitSync();
      }
    }
  }
}
