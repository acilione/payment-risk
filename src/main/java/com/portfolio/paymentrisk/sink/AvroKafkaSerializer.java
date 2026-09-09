package com.portfolio.paymentrisk.sink;

import com.portfolio.paymentrisk.Json;
import com.portfolio.paymentrisk.serialization.AvroCodec;
import java.nio.charset.StandardCharsets;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

public class AvroKafkaSerializer implements KafkaRecordSerializationSchema<String> {
  private final String registry, topic, schema, key;
  private transient AvroCodec codec;

  public AvroKafkaSerializer(String registry, String topic, String schema, String key) {
    this.registry = registry;
    this.topic = topic;
    this.schema = schema;
    this.key = key;
  }

  public ProducerRecord<byte[], byte[]> serialize(
      String value, KafkaSinkContext context, Long timestamp) {
    if (codec == null) codec = new AvroCodec(registry);
    try {
      return new ProducerRecord<>(
          topic,
          Json.read(value).path(key).asText().getBytes(StandardCharsets.UTF_8),
          codec.encode(topic, schema, value));
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }
}
