package com.portfolio.paymentrisk.tools;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

/** Aggregated metrics contain no customer IDs, payloads or credentials. */
public final class MaterializerMetrics implements AutoCloseable {
  public final AtomicLong committed = new AtomicLong(),
      rejected = new AtomicLong(),
      retries = new AtomicLong(),
      insertErrors = new AtomicLong(),
      insertNanos = new AtomicLong(),
      inserts = new AtomicLong(),
      lastPoll = new AtomicLong(),
      lastInsert = new AtomicLong(),
      lag = new AtomicLong();
  public final AtomicLong unresolved = new AtomicLong(-1);
  public volatile boolean storageHealthy;
  private final HttpServer server;

  public MaterializerMetrics(int port) throws java.io.IOException {
    server = HttpServer.create(new InetSocketAddress(port), 0);
    server.createContext(
        "/metrics",
        e -> {
          byte[] body = prometheus().getBytes(StandardCharsets.UTF_8);
          e.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4");
          e.sendResponseHeaders(200, body.length);
          e.getResponseBody().write(body);
          e.close();
        });
    server.createContext(
        "/health/ready",
        e -> {
          e.sendResponseHeaders(
              storageHealthy
                      && unresolved.get() == 0
                      && System.currentTimeMillis() - lastPoll.get() < 60_000
                  ? 200
                  : 503,
              -1);
          e.close();
        });
    server.start();
  }

  public String prometheus() {
    return metric("risk_materializer_unresolved_records", "gauge", unresolved.get())
        + metric("risk_materializer_committed_records_total", "counter", committed.get())
        + metric("risk_materializer_rejected_records_total", "counter", rejected.get())
        + metric("risk_materializer_insert_retries_total", "counter", retries.get())
        + metric("risk_materializer_insert_errors_total", "counter", insertErrors.get())
        + metric(
            "risk_materializer_insert_duration_seconds_sum",
            "counter",
            insertNanos.get() / 1_000_000_000.0)
        + metric("risk_materializer_insert_duration_seconds_count", "counter", inserts.get())
        + metric(
            "risk_materializer_last_insert_timestamp_seconds", "gauge", lastInsert.get() / 1000.0)
        + metric("risk_materializer_records_lag_max", "gauge", lag.get())
        + metric("risk_materializer_storage_available", "gauge", storageHealthy ? 1 : 0);
  }

  private static String metric(String name, String type, double value) {
    return "# TYPE " + name + " " + type + "\n" + name + " " + value + "\n";
  }

  public void close() {
    server.stop(0);
  }
}
