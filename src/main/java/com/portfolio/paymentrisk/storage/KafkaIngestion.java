package com.portfolio.paymentrisk.storage;

import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.tools.*;
import java.util.*;
import org.apache.kafka.common.serialization.*;

/** Shared transport limits and connection validation for independent database consumers. */
public final class KafkaIngestion {
  private KafkaIngestion() {}

  public static Properties properties(AppConfig config, String group, boolean binaryKey) {
    var p = config.kafkaProperties();
    p.put("bootstrap.servers", config.bootstrap());
    p.put("group.id", group);
    p.put(
        "key.deserializer",
        binaryKey ? ByteArrayDeserializer.class.getName() : StringDeserializer.class.getName());
    p.put("value.deserializer", ByteArrayDeserializer.class.getName());
    p.put("enable.auto.commit", "false");
    p.put("isolation.level", "read_committed");
    // Fail on expired committed offsets; never silently skip an unarchived retention gap.
    p.put("auto.offset.reset", "none");
    p.put("max.poll.records", "250");
    p.put("fetch.max.bytes", "4194304");
    p.put("max.partition.fetch.bytes", "1048576");
    p.put("max.poll.interval.ms", "300000");
    p.put("default.api.timeout.ms", "30000");
    return p;
  }

  public static ClickHouseWriter writer(AppConfig config, MaterializerMetrics metrics) {
    String endpoint = System.getenv().getOrDefault("CLICKHOUSE_URL", "http://localhost:28123");
    if (!config.environment().equals("local") && !endpoint.startsWith("https://"))
      throw new IllegalArgumentException("ClickHouse TLS required outside local mode");
    return new ClickHouseWriter(
        endpoint,
        System.getenv().getOrDefault("CLICKHOUSE_USER", "risk"),
        Objects.requireNonNull(
            System.getenv("CLICKHOUSE_PASSWORD"), "CLICKHOUSE_PASSWORD required"),
        metrics);
  }

  public static <K> void subscribe(
      org.apache.kafka.clients.consumer.Consumer<K, byte[]> consumer,
      String topic,
      boolean allowNewLocalGroup) {
    consumer.subscribe(
        List.of(topic),
        new org.apache.kafka.clients.consumer.ConsumerRebalanceListener() {
          public void onPartitionsRevoked(
              Collection<org.apache.kafka.common.TopicPartition> partitions) {}

          public void onPartitionsAssigned(
              Collection<org.apache.kafka.common.TopicPartition> partitions) {
            if (!allowNewLocalGroup || partitions.isEmpty()) return;
            var committed = consumer.committed(Set.copyOf(partitions));
            var fresh = partitions.stream().filter(tp -> committed.get(tp) == null).toList();
            // Only a new local group's partitions may start at their retained beginning.
            // Existing, out-of-range offsets keep auto.offset.reset=none and fail visibly.
            if (!fresh.isEmpty()) {
              var beginnings = consumer.beginningOffsets(fresh);
              if (beginnings.values().stream().anyMatch(offset -> offset != 0))
                throw new IllegalStateException(
                    "New ingestion group cannot prove complete coverage: retained log starts after zero");
              var initial =
                  new HashMap<
                      org.apache.kafka.common.TopicPartition,
                      org.apache.kafka.clients.consumer.OffsetAndMetadata>();
              for (var partition : fresh) {
                consumer.seek(partition, 0);
                initial.put(partition, new org.apache.kafka.clients.consumer.OffsetAndMetadata(0));
              }
              consumer.commitSync(initial);
            }
          }
        });
  }
}
