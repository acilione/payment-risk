package com.portfolio.paymentrisk.tools;

import com.portfolio.paymentrisk.storage.PostgresWriter;
import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Duration;
import java.util.List;

/** One-time, restartable migration. Stop the old materializer before switching deployments. */
public final class ProjectionBootstrap {
  private ProjectionBootstrap() {}

  public static void main(String[] args) throws Exception {
    var env = System.getenv();
    try (var db =
            DriverManager.getConnection(
                env.get("RISK_JDBC_URL"),
                env.getOrDefault("RISK_DB_USER", "risk"),
                env.get("RISK_DB_PASSWORD"));
        var statement = db.createStatement()) {
      try (var schema = ProjectionBootstrap.class.getResourceAsStream("/storage/postgres.sql")) {
        if (schema == null)
          throw new IOException("PostgreSQL schema missing from application image");
        statement.execute(new String(schema.readAllBytes(), StandardCharsets.UTF_8));
      }
      try (var result =
          statement.executeQuery("SELECT ready FROM risk.projection_status WHERE id")) {
        if (result.next() && result.getBoolean(1)) return;
      }
      try (var metrics = new MaterializerMetrics(0);
          var writer = new PostgresWriter(env, metrics)) {
        var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        // ClickHouse is needed only for first migration, never to restart a ready projection.
        boolean sourceReady = false;
        for (int attempt = 0; attempt < 60; attempt++) {
          var probe =
              HttpRequest.newBuilder(URI.create(env.get("CLICKHOUSE_URL")))
                  .timeout(Duration.ofSeconds(5))
                  .header("X-ClickHouse-User", env.getOrDefault("CLICKHOUSE_USER", "risk"))
                  .header("X-ClickHouse-Key", env.get("CLICKHOUSE_PASSWORD"))
                  .POST(
                      HttpRequest.BodyPublishers.ofString(
                          "SELECT count() FROM system.tables WHERE database='risk' AND name IN ('evaluations','processing_outcomes','materializer_rejections')"))
                  .build();
          try {
            var response = http.send(probe, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200 && response.body().trim().equals("3")) {
              sourceReady = true;
              break;
            }
          } catch (IOException unavailable) {
            /* Retry before importing any history. */
          }
          Thread.sleep(1000);
        }
        if (!sourceReady) throw new IOException("Migration source did not become ready");
        for (String table :
            List.of(
                "risk.evaluations", "risk.processing_outcomes", "risk.materializer_rejections")) {
          var request =
              HttpRequest.newBuilder(URI.create(env.get("CLICKHOUSE_URL")))
                  .header("X-ClickHouse-User", env.getOrDefault("CLICKHOUSE_USER", "risk"))
                  .header("X-ClickHouse-Key", env.get("CLICKHOUSE_PASSWORD"))
                  .POST(
                      HttpRequest.BodyPublishers.ofString(
                          "SELECT * FROM "
                              + table
                              + " SETTINGS max_threads=1, max_block_size=512 FORMAT JSONEachRow"))
                  .build();
          var response = http.send(request, HttpResponse.BodyHandlers.ofInputStream());
          try (var input = response.body();
              var lines =
                  new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
            if (response.statusCode() != 200)
              throw new IOException("Migration source unavailable HTTP " + response.statusCode());
            var rows = new StringBuilder();
            long count = 0;
            String row;
            while ((row = lines.readLine()) != null) {
              rows.append(row).append('\n');
              if (++count % 200 == 0 || rows.length() >= 1_048_576) {
                writer.insert(table, rows.toString());
                rows.setLength(0);
              }
            }
            writer.insert(table, rows.toString());
            System.out.println("Migration completed: " + table + " physical rows=" + count);
          }
        }
      }
      statement.executeUpdate("UPDATE risk.projection_status SET ready=true WHERE id");
    }
  }
}
