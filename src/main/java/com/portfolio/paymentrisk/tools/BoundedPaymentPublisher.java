package com.portfolio.paymentrisk.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;

/** Bounded asynchronous sends; only acknowledged coordinates are emitted to the input ledger. */
public final class BoundedPaymentPublisher implements AutoCloseable {
  private record Pending(CompletableFuture<RecordMetadata> result, ObjectNode row) {}

  private final Producer<String, byte[]> producer;
  private final int limit;
  private final Consumer<ObjectNode> output;
  private final Deque<Pending> pending = new ArrayDeque<>();
  private final Map<TopicPartition, Long> offsets = new HashMap<>();

  public BoundedPaymentPublisher(
      Producer<String, byte[]> producer, int limit, Consumer<ObjectNode> output) {
    if (limit < 1 || limit > 10000) throw new IllegalArgumentException("Invalid in-flight limit");
    this.producer = producer;
    this.limit = limit;
    this.output = output;
  }

  public void send(ProducerRecord<String, byte[]> record, ObjectNode row) throws Exception {
    if (pending.size() >= limit) acknowledge();
    var result = new CompletableFuture<RecordMetadata>();
    producer.send(
        record,
        (metadata, error) -> {
          if (error != null) result.completeExceptionally(error);
          else {
            row.put("acknowledged_at", System.currentTimeMillis());
            result.complete(metadata);
          }
        });
    pending.addLast(new Pending(result, row));
  }

  private void acknowledge() throws Exception {
    var next = pending.getFirst();
    var metadata = next.result().get(120, TimeUnit.SECONDS);
    pending.removeFirst();
    offsets.merge(
        new TopicPartition(metadata.topic(), metadata.partition()),
        metadata.offset() + 1,
        Math::max);
    next.row()
        .put("source_partition", metadata.partition())
        .put("source_offset", metadata.offset());
    output.accept(next.row());
  }

  public void drain() throws Exception {
    while (!pending.isEmpty()) acknowledge();
  }

  public Map<TopicPartition, Long> offsets() {
    return Map.copyOf(offsets);
  }

  public void close() throws Exception {
    drain();
  }
}
