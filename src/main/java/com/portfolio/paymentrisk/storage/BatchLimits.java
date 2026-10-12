package com.portfolio.paymentrisk.storage;

import java.util.Map;

/**
 * Bounds are shared by archival and output ingestion; a single larger record is preserved alone.
 */
public record BatchLimits(int bytes, int rows, long milliseconds) {
  public BatchLimits {
    if (bytes < 1
        || bytes > 67_108_864
        || rows < 1
        || rows > 10_000
        || milliseconds < 1
        || milliseconds > 10_000)
      throw new IllegalArgumentException("Invalid database batch bounds");
  }

  public static BatchLimits from(Map<String, String> env) {
    return new BatchLimits(
        Integer.parseInt(env.getOrDefault("INGEST_BATCH_BYTES", "1048576")),
        Integer.parseInt(env.getOrDefault("INGEST_BATCH_ROWS", "1000")),
        Long.parseLong(env.getOrDefault("INGEST_BATCH_MS", "1000")));
  }
}
