package com.portfolio.paymentrisk.tools;

import com.portfolio.paymentrisk.Json;
import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.serialization.AvroCodec;
import com.portfolio.paymentrisk.storage.PaymentArchiveRows;
import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** Local fault-test fixture. Labels and expected bytes remain outside the production consumers. */
public final class ArchiveScenario {
  private ArchiveScenario() {}

  public static void main(String[] args) throws Exception {
    var c = AppConfig.fromEnv();
    if (!c.environment().equals("local")) throw new IllegalArgumentException("Local fixture only");
    var p = c.kafkaProperties();
    p.put("bootstrap.servers", c.bootstrap());
    if (args.length > 0 && args[0].equals("offsets")) {
      try (var admin = Admin.create(p)) {
        var partitions =
            admin
                .describeTopics(List.of(c.topic("payments.raw")))
                .allTopicNames()
                .get()
                .get(c.topic("payments.raw"))
                .partitions();
        var requests = new HashMap<TopicPartition, OffsetSpec>();
        partitions.forEach(
            partition ->
                requests.put(
                    new TopicPartition(c.topic("payments.raw"), partition.partition()),
                    OffsetSpec.latest()));
        var committed =
            admin
                .listConsumerGroupOffsets("risk-payment-archive-v1")
                .partitionsToOffsetAndMetadata()
                .get();
        var ends = admin.listOffsets(requests).all().get();
        var report = Json.object();
        for (var tp : requests.keySet()) {
          var value = report.putObject(Integer.toString(tp.partition()));
          value.put("end", ends.get(tp).offset());
          value.put("committed", committed.get(tp) == null ? -1 : committed.get(tp).offset());
        }
        System.out.println(Json.write(report));
      }
      return;
    }
    p.put("key.serializer", ByteArraySerializer.class.getName());
    p.put("value.serializer", ByteArraySerializer.class.getName());
    p.put("acks", "all");
    p.put("enable.idempotence", "true");
    var codec = new AvroCodec(c.registry());
    String run = "archive-" + UUID.randomUUID();
    long now = System.currentTimeMillis();
    byte[] normal =
        codec.encode(
            c.topic("payments.raw"),
            "transaction",
            Json.write(IntegrationScenario.transaction(run, run, now, 100, "phone", "APPROVED")));
    byte[] late =
        codec.encode(
            c.topic("payments.raw"),
            "transaction",
            Json.write(
                IntegrationScenario.transaction(
                    run + "-late", run, now - 3600000, 200, "phone", "APPROVED")));
    byte[][] values = {normal, normal, late, new byte[] {99, -1, 0}, null};
    var expected = Json.MAPPER.createArrayNode();
    try (var producer = new KafkaProducer<byte[], byte[]>(p)) {
      for (int i = 0; i < values.length; i++) {
        byte[] key = i == 4 ? null : new byte[] {0, -1, 42};
        var headers = new RecordHeaders().add("trace", new byte[] {1, 2}).add("nullable", null);
        var record =
            new ProducerRecord<byte[], byte[]>(
                c.topic("payments.raw"), 0, now, key, values[i], headers);
        var metadata = producer.send(record).get();
        expected.add(
            PaymentArchiveRows.convert(
                new ConsumerRecord<>(
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset(),
                    metadata.timestamp(),
                    TimestampType.CREATE_TIME,
                    key == null ? -1 : key.length,
                    values[i] == null ? -1 : values[i].length,
                    key,
                    values[i],
                    headers,
                    Optional.empty())));
      }
    }
    var report = Json.object().put("run_id", run);
    report.set("expected", expected);
    System.out.println(Json.write(report));
  }
}
