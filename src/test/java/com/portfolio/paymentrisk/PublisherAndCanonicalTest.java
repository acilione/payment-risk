package com.portfolio.paymentrisk;

import static org.junit.jupiter.api.Assertions.*;

import com.portfolio.paymentrisk.domain.Audit;
import com.portfolio.paymentrisk.tools.BoundedPaymentPublisher;
import java.util.*;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.junit.jupiter.api.Test;

class PublisherAndCanonicalTest {
  @Test
  void canonicalBytesRemainCompatibleWithStoredIdentities() {
    var input = Json.read("{\"z\":[{\"b\":2,\"a\":1},null,true,\"é\"],\"a\":-12.5}");
    String before = Json.write(input);
    assertEquals("{\"a\":-12.5,\"z\":[{\"a\":1,\"b\":2},null,true,\"é\"]}", Audit.canonical(input));
    assertEquals(before, Json.write(input));
    var facts = Json.read("{\"event_id\":\"evt-1\",\"amount_minor\":100,\"producer_time\":1}");
    assertEquals(
        Audit.sha256("{\"amount_minor\":100,\"event_id\":\"evt-1\"}"), Audit.inputHash(facts));
  }

  @Test
  void boundedPublisherEmitsOnlyAcknowledgedCoordinatesAndPropagatesFailure() throws Exception {
    var node = new org.apache.kafka.common.Node(0, "localhost", 9092);
    var cluster =
        new org.apache.kafka.common.Cluster(
            "test",
            List.of(node),
            List.of(
                new org.apache.kafka.common.PartitionInfo(
                    "payments",
                    0,
                    node,
                    new org.apache.kafka.common.Node[] {node},
                    new org.apache.kafka.common.Node[] {node}),
                new org.apache.kafka.common.PartitionInfo(
                    "payments",
                    1,
                    node,
                    new org.apache.kafka.common.Node[] {node},
                    new org.apache.kafka.common.Node[] {node})),
            Set.of(),
            Set.of());
    var producer =
        new MockProducer<String, byte[]>(
            cluster, false, null, new StringSerializer(), new ByteArraySerializer());
    var rows = new ArrayList<com.fasterxml.jackson.databind.node.ObjectNode>();
    var publisher = new BoundedPaymentPublisher(producer, 2, rows::add);
    publisher.send(new ProducerRecord<>("payments", 0, "a", new byte[] {1}), Json.object());
    publisher.send(new ProducerRecord<>("payments", 1, "b", new byte[] {2}), Json.object());
    assertTrue(rows.isEmpty());
    producer.completeNext();
    publisher.send(new ProducerRecord<>("payments", 0, "c", new byte[] {3}), Json.object());
    assertEquals(1, rows.size());
    assertEquals(0, rows.get(0).path("source_partition").asInt());
    assertTrue(rows.get(0).path("acknowledged_at").asLong() > 0);
    producer.completeNext();
    producer.errorNext(new IllegalStateException("broker unavailable"));
    assertThrows(ExecutionException.class, publisher::drain);
    assertEquals(2, rows.size());
    assertEquals(2, publisher.offsets().size());
    producer.close();
  }
}
