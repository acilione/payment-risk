package com.portfolio.paymentrisk;

import static org.junit.jupiter.api.Assertions.*;

import com.portfolio.paymentrisk.storage.*;
import com.portfolio.paymentrisk.tools.*;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class BoundedIngestionTest {
  @Test
  void byteChunksCommitOnlyTheirOwnOffsetsAndKeepOversizedRows() throws Exception {
    var bodies = new ArrayList<String>();
    var commits = new ArrayList<Map<TopicPartition, OffsetAndMetadata>>();
    AtomicInteger calls = new AtomicInteger();
    var server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          exchange.sendResponseHeaders(calls.incrementAndGet() == 4 ? 503 : 200, -1);
          exchange.close();
        });
    server.start();
    try (var metrics = new MaterializerMetrics(0)) {
      var writer =
          new ClickHouseWriter(
              "http://localhost:" + server.getAddress().getPort(), "test", "test", metrics, 1);
      var batch =
          new DurableKafkaBatch(writer, offsets -> commits.add(Map.copyOf(offsets)), metrics, 8);
      batch.add(
          "risk.payment_ingress", "abc", new ConsumerRecord<>("raw", 0, 10, null, new byte[0]));
      batch.add(
          "risk.payment_ingress", "def", new ConsumerRecord<>("raw", 1, 20, null, new byte[0]));
      assertEquals(1, commits.size());
      assertEquals(11, commits.get(0).get(new TopicPartition("raw", 0)).offset());
      assertEquals(21, commits.get(0).get(new TopicPartition("raw", 1)).offset());
      batch.add(
          "risk.payment_ingress",
          "oversized-single-row",
          new ConsumerRecord<>("raw", 0, 11, null, new byte[0]));
      assertEquals(2, commits.size());
      assertEquals(Set.of(new TopicPartition("raw", 0)), commits.get(1).keySet());
      assertTrue(bodies.get(1).endsWith("oversized-single-row\n"));
      batch.add("risk.evaluations", "{}", new ConsumerRecord<>("raw", 0, 12, null, new byte[0]));
      batch.add(
          "risk.materializer_rejections",
          "{}",
          new ConsumerRecord<>("raw", 1, 21, null, new byte[0]));
      assertThrows(java.io.IOException.class, batch::flush);
      assertEquals(2, commits.size(), "Successful first table cannot commit a failed second table");
      assertEquals(3, metrics.committed.get());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void wireArchivePreservesBinaryNullAndHeadersWithoutAvroDecoding() {
    var record =
        new ConsumerRecord<byte[], byte[]>("raw", 0, 1, new byte[] {-1, 0}, new byte[] {99, -1});
    record.headers().add("binary", new byte[] {0, -1}).add("missing", null);
    var row = PaymentArchiveRows.convert(record);
    assertArrayEquals(
        record.value(), Base64.getDecoder().decode(row.path("payload_base64").asText()));
    assertArrayEquals(record.key(), Base64.getDecoder().decode(row.path("key_base64").asText()));
    assertTrue(
        Json.read(row.path("headers_json").asText()).get(1).path("value_is_null").asBoolean());
    var empty =
        PaymentArchiveRows.convert(
            new ConsumerRecord<byte[], byte[]>("raw", 0, 2, null, new byte[0]));
    var tombstone =
        PaymentArchiveRows.convert(new ConsumerRecord<byte[], byte[]>("raw", 0, 3, null, null));
    assertFalse(empty.path("value_is_null").asBoolean());
    assertTrue(tombstone.path("value_is_null").asBoolean());
    assertEquals(empty.path("payload_sha256"), tombstone.path("payload_sha256"));
  }

  @Test
  void incrementalDigestIsStableAcrossPartitionInterleavingAndDetectsMissingOffsets() {
    var a = new SourcePositionDigest();
    var b = new SourcePositionDigest();
    var missing = new SourcePositionDigest();
    a.add("t", 0, 9);
    a.add("t", 1, 1);
    a.add("t", 0, 11);
    b.add("t", 1, 1);
    b.add("t", 0, 9);
    b.add("t", 0, 11);
    missing.add("t", 0, 9);
    missing.add("t", 1, 1);
    String hash = a.finish();
    assertEquals(hash, b.finish());
    assertNotEquals(hash, missing.finish());
    assertThrows(IllegalArgumentException.class, () -> missing.add("t", 0, 9));
  }

  @Test
  void newLocalGroupsEstablishZeroOffsetAndRejectAlreadyExpiredHistory() {
    var tp = new TopicPartition("raw", 0);
    try (var consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.NONE)) {
      consumer.updateBeginningOffsets(Map.of(tp, 0L));
      KafkaIngestion.subscribe(consumer, "raw", true);
      consumer.rebalance(List.of(tp));
      assertEquals(0, consumer.committed(Set.of(tp)).get(tp).offset());
      consumer.commitSync(Map.of(tp, new OffsetAndMetadata(10)));
      consumer.rebalance(List.of(tp));
      assertEquals(10, consumer.committed(Set.of(tp)).get(tp).offset());
    }
    try (var consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.NONE)) {
      consumer.updateBeginningOffsets(Map.of(tp, 5L));
      KafkaIngestion.subscribe(consumer, "raw", true);
      assertThrows(IllegalStateException.class, () -> consumer.rebalance(List.of(tp)));
      assertNull(consumer.committed(Set.of(tp)).get(tp));
    }
    assertEquals(
        "none",
        KafkaIngestion.properties(
                com.portfolio.paymentrisk.config.AppConfig.from(Map.of()), "archive", true)
            .get("auto.offset.reset"));
  }
}
