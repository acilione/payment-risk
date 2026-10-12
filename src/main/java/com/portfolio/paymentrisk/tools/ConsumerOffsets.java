package com.portfolio.paymentrisk.tools;

import com.portfolio.paymentrisk.Json;
import com.portfolio.paymentrisk.config.AppConfig;
import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;

/** Read-only consumer positions for the local database-outage demonstration. */
public final class ConsumerOffsets {
  private ConsumerOffsets() {}

  public static void main(String[] args) throws Exception {
    var config = AppConfig.fromEnv();
    if (!config.environment().equals("local"))
      throw new IllegalArgumentException("Showcase requires a local deployment");
    var properties = config.kafkaProperties();
    properties.put("bootstrap.servers", config.bootstrap());
    var groups =
        Map.of(
            "archive", System.getenv().getOrDefault("ARCHIVER_GROUP", "risk-payment-archive-v1"),
            "decisions",
                System.getenv()
                    .getOrDefault("MATERIALIZER_GROUP", "risk-clickhouse-evaluations-v2"));
    var topics =
        Map.of(
            "archive",
            config.topic("payments.raw"),
            "decisions",
            config.topic("risk.decisions"),
            "invalid",
            config.topic("payments.dlq"),
            "late",
            config.topic("payments.late"));
    var result = Json.object();
    try (var admin = Admin.create(properties)) {
      for (var kind : List.of("archive", "decisions", "invalid", "late")) {
        String topic = topics.get(kind);
        var description = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic);
        var requests = new HashMap<TopicPartition, OffsetSpec>();
        description
            .partitions()
            .forEach(
                p -> requests.put(new TopicPartition(topic, p.partition()), OffsetSpec.latest()));
        var ends = admin.listOffsets(requests).all().get();
        var offsets =
            admin
                .listConsumerGroupOffsets(
                    groups.get(kind.equals("archive") ? "archive" : "decisions"))
                .partitionsToOffsetAndMetadata()
                .get();
        var entries = result.putArray(kind);
        for (var partition : description.partitions()) {
          var tp = new TopicPartition(topic, partition.partition());
          entries
              .addObject()
              .put("topic", topic)
              .put("partition", partition.partition())
              .put("committed", offsets.containsKey(tp) ? offsets.get(tp).offset() : -1)
              .put("log_end", ends.get(tp).offset());
        }
      }
    }
    System.out.println(Json.write(result));
  }
}
