package com.portfolio.paymentrisk.storage;

import com.portfolio.paymentrisk.tools.ClickHouseWriter;
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
  private final ClickHouseWriter writer;
  private final Consumer<Map<TopicPartition, OffsetAndMetadata>> commit;
  private final MaterializerMetrics metrics;
  private final int limit;
  private final Map<String, StringBuilder> rows = new LinkedHashMap<>();
  private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
  private long bytes, count, rejected;

  public DurableKafkaBatch(
      ClickHouseWriter writer,
      Consumer<Map<TopicPartition, OffsetAndMetadata>> commit,
      MaterializerMetrics metrics,
      int limit) {
    if (limit < 1) throw new IllegalArgumentException("Batch limit must be positive");
    this.writer = writer;
    this.commit = commit;
    this.metrics = metrics;
    this.limit = limit;
  }

  public void add(String table, String row, ConsumerRecord<?, ?> record)
      throws IOException, InterruptedException {
    long size = row.getBytes(StandardCharsets.UTF_8).length + 1L;
    if (bytes > 0 && bytes + size > limit) flush();
    rows.computeIfAbsent(table, ignored -> new StringBuilder()).append(row).append('\n');
    offsets.put(
        new TopicPartition(record.topic(), record.partition()),
        new OffsetAndMetadata(record.offset() + 1));
    bytes += size;
    count++;
    if (table.equals("risk.materializer_rejections")) rejected++;
    // A single oversized row is preserved, written alone and never truncated.
    if (bytes >= limit) flush();
  }

  public void flush() throws IOException, InterruptedException {
    if (count == 0) return;
    for (var entry : rows.entrySet()) writer.insert(entry.getKey(), entry.getValue().toString());
    commit.accept(Map.copyOf(offsets));
    metrics.committed.addAndGet(count);
    metrics.rejected.addAndGet(rejected);
    rows.clear();
    offsets.clear();
    bytes = 0;
    count = 0;
    rejected = 0;
  }
}
