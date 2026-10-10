package com.portfolio.paymentrisk.tools;

import java.io.IOException;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/** Bounded retry budget stays below the consumer's five-minute max poll interval. */
public final class ClickHouseWriter {
  private final URI endpoint;
  private final String user, password;
  private final HttpClient http;
  private final MaterializerMetrics metrics;
  private final int attempts;

  public ClickHouseWriter(
      String endpoint, String user, String password, MaterializerMetrics metrics) {
    this(endpoint, user, password, metrics, 5);
  }

  public ClickHouseWriter(
      String endpoint, String user, String password, MaterializerMetrics metrics, int attempts) {
    this.endpoint =
        URI.create(
            endpoint + (endpoint.contains("?") ? "&" : "?") + "wait_end_of_query=1&async_insert=0");
    this.user = user;
    this.password = password;
    this.metrics = metrics;
    this.attempts = attempts;
    if (!Set.of("http", "https").contains(this.endpoint.getScheme())
        || this.endpoint.getUserInfo() != null
        || attempts < 1
        || attempts > 5)
      throw new IllegalArgumentException("Invalid ClickHouse connection or retry budget");
    http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
  }

  public void insert(String table, String rows) throws IOException, InterruptedException {
    if (!Set.of("risk.evaluations", "risk.materializer_rejections", "risk.payment_ingress")
        .contains(table)) throw new IllegalArgumentException("Unknown insertion target");
    if (!rows.isEmpty()) execute("INSERT INTO " + table + " FORMAT JSONEachRow\n" + rows, true);
  }

  public void ping() throws IOException, InterruptedException {
    execute("SELECT 1", false);
  }

  // One bounded probe: persisted incidents remain visible after consumer restarts.
  public void inspectIntegrity() throws IOException, InterruptedException {
    var request =
        HttpRequest.newBuilder(endpoint)
            .timeout(Duration.ofSeconds(5))
            .header("X-ClickHouse-User", user)
            .header("X-ClickHouse-Key", password)
            .POST(
                HttpRequest.BodyPublishers.ofString(
                    "SELECT (SELECT count() FROM risk.integrity_conflicts) + (SELECT count() FROM risk.materializer_rejections FINAL) SETTINGS max_execution_time=4, max_memory_usage=268435456, max_bytes_before_external_group_by=67108864, max_bytes_before_external_sort=67108864 FORMAT TabSeparated"))
            .build();
    metrics.storageHealthy = false;
    var response = http.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() / 100 != 2)
      throw new IOException("ClickHouse integrity probe failed HTTP " + response.statusCode());
    try {
      metrics.unresolved.set(Long.parseLong(response.body().trim()));
    } catch (NumberFormatException e) {
      throw new IOException("Invalid ClickHouse integrity probe response");
    }
    metrics.storageHealthy = true;
  }

  private void execute(String body, boolean insert) throws IOException, InterruptedException {
    IOException last = null;
    for (int attempt = 0; attempt < attempts; attempt++) {
      long started = System.nanoTime();
      try {
        var request =
            HttpRequest.newBuilder(endpoint)
                .timeout(Duration.ofSeconds(10))
                .header("X-ClickHouse-User", user)
                .header("X-ClickHouse-Key", password)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        var response = http.send(request, HttpResponse.BodyHandlers.discarding());
        if (response.statusCode() / 100 == 2) {
          metrics.storageHealthy = true;
          if (insert) metrics.lastInsert.set(System.currentTimeMillis());
          return;
        }
        metrics.storageHealthy = false;
        if (response.statusCode() != 429 && response.statusCode() < 500)
          throw new FatalInsertException(
              "ClickHouse rejected request HTTP " + response.statusCode());
        last = new IOException("ClickHouse unavailable HTTP " + response.statusCode());
      } catch (FatalInsertException e) {
        metrics.insertErrors.incrementAndGet();
        throw e;
      } catch (IOException e) {
        last = e;
        metrics.storageHealthy = false;
      } finally {
        if (insert) {
          metrics.insertNanos.addAndGet(System.nanoTime() - started);
          metrics.inserts.incrementAndGet();
        }
      }
      metrics.insertErrors.incrementAndGet();
      if (attempt + 1 < attempts) {
        metrics.retries.incrementAndGet();
        Thread.sleep(Math.min(3200, 200L << attempt) + ThreadLocalRandom.current().nextLong(100));
      }
    }
    throw new IOException(
        "ClickHouse retry budget exhausted; Kafka offsets remain uncommitted", last);
  }

  private static final class FatalInsertException extends IOException {
    FatalInsertException(String message) {
      super(message);
    }
  }
}
