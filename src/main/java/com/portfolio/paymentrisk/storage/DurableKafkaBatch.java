package com.portfolio.paymentrisk.storage;

import com.portfolio.paymentrisk.tools.MaterializerMetrics;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

/** A byte-bounded insert unit; only offsets covered by successful writes may be committed. */
public final class DurableKafkaBatch {
  public static final int DEFAULT_BYTES = 1_048_576;
  private final RowWriter writer;
  private final Consumer<Map<TopicPartition, OffsetAndMetadata>> commit;
  private final MaterializerMetrics metrics;
  private final BatchLimits limits;
  private final java.util.function.LongSupplier clock;
  private long openedAt;
  private final Map<String, StringBuilder> rows = new LinkedHashMap<>();
  private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
  private long bytes, count, rejected;

  public DurableKafkaBatch(
      RowWriter writer,
      Consumer<Map<TopicPartition, OffsetAndMetadata>> commit,
      MaterializerMetrics metrics,
      int limit) {
    this(writer, commit, metrics, new BatchLimits(limit, 10000, 10000), System::nanoTime);
  }

  public DurableKafkaBatch(
      RowWriter writer,
      Consumer<Map<TopicPartition, OffsetAndMetadata>> commit,
      MaterializerMetrics metrics,
      BatchLimits limits,
      java.util.function.LongSupplier clock) {
    this.writer = writer;
    this.commit = commit;
    this.metrics = metrics;
    this.limits = limits;
    this.clock = clock;
  }

  public void add(String table, String row, ConsumerRecord<?, ?> record)
      throws IOException, InterruptedException {
    long size = row.getBytes(StandardCharsets.UTF_8).length + 1L;
    if (count > 0
        && (bytes + size > limits.bytes()
            || clock.getAsLong() - openedAt >= limits.milliseconds() * 1_000_000L)) flush();
    if (count == 0) openedAt = clock.getAsLong();
    rows.computeIfAbsent(table, ignored -> new StringBuilder()).append(row).append('\n');
    offsets.put(
        new TopicPartition(record.topic(), record.partition()),
        new OffsetAndMetadata(record.offset() + 1));
    bytes += size;
    count++;
    if (table.equals("risk.materializer_rejections")) rejected++;
    // A single oversized row is preserved, written alone and never truncated.
    if (bytes >= limits.bytes() || count >= limits.rows()) flush();
  }

  public void flush() throws IOException, InterruptedException {
    if (count == 0) return;
    for (var entry : rows.entrySet()) writer.insert(entry.getKey(), entry.getValue().toString());
    commit.accept(Map.copyOf(offsets));
    metrics.batchBytes.addAndGet(bytes);
    metrics.batchRows.addAndGet(count);
    metrics.batches.incrementAndGet();
    metrics.committed.addAndGet(count);
    metrics.rejected.addAndGet(rejected);
    rows.clear();
    offsets.clear();
    bytes = 0;
    count = 0;
    rejected = 0;
  }
}
