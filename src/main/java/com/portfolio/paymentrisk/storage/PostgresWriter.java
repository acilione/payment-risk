package com.portfolio.paymentrisk.storage;

import com.portfolio.paymentrisk.tools.MaterializerMetrics;
import java.io.IOException;
import java.sql.*;
import java.util.Map;
import java.util.Objects;

/** Indexed acceptance and summaries commit together; an uncertain commit is safe to retry. */
public final class PostgresWriter implements RowWriter, AutoCloseable {
  private final Connection connection;
  private final String sourceSystem;
  private final MaterializerMetrics metrics;

  public PostgresWriter(Map<String, String> env, MaterializerMetrics metrics) throws SQLException {
    String url = env.getOrDefault("RISK_JDBC_URL", "jdbc:postgresql://localhost:25432/risk");
    if (!env.getOrDefault("APP_ENVIRONMENT", "local").equals("local")
        && !url.contains("sslmode=verify-full"))
      throw new IllegalArgumentException(
          "PostgreSQL certificate verification required outside local mode");
    var properties = new java.util.Properties();
    properties.setProperty("user", env.getOrDefault("RISK_DB_USER", "risk"));
    properties.setProperty(
        "password",
        Objects.requireNonNull(env.get("RISK_DB_PASSWORD"), "RISK_DB_PASSWORD required"));
    properties.setProperty("connectTimeout", "5");
    properties.setProperty("socketTimeout", "60");
    connection = DriverManager.getConnection(url, properties);
    connection.setAutoCommit(false);
    sourceSystem = env.getOrDefault("PAYMENT_SOURCE_SYSTEM", "payment-risk");
    this.metrics = metrics;
    try (var statement = connection.createStatement()) {
      statement.execute("SET statement_timeout='45s'");
      statement.execute("SET lock_timeout='10s'");
    }
    connection.commit();
  }

  @Override
  public void insert(String table, String rows) throws IOException {
    if (rows.isEmpty()) return;
    long started = System.nanoTime();
    try (var statement =
        connection.prepareStatement(
            "SELECT risk.accept(?,value,?) FROM jsonb_array_elements(?::jsonb)")) {
      statement.setString(1, table);
      statement.setString(2, sourceSystem);
      // The caller already bounds bytes/rows. No whole-history collection is built here.
      statement.setString(3, "[" + String.join(",", rows.strip().split("\n")) + "]");
      long executeStarted = System.nanoTime();
      try {
        statement.execute();
      } finally {
        metrics.sqlNanos.addAndGet(System.nanoTime() - executeStarted);
      }
      long commitStarted = System.nanoTime();
      try {
        connection.commit();
      } finally {
        metrics.databaseCommitNanos.addAndGet(System.nanoTime() - commitStarted);
      }
      metrics.lastInsert.set(System.currentTimeMillis());
      metrics.storageHealthy = true;
    } catch (SQLException error) {
      metrics.storageHealthy = false;
      metrics.insertErrors.incrementAndGet();
      try {
        connection.rollback();
      } catch (SQLException rollback) {
        error.addSuppressed(rollback);
      }
      // Exit and rejoin the consumer group; source offsets were not acknowledged.
      throw new IOException(
          "PostgreSQL write failed; Kafka offsets remain uncommitted (SQLSTATE "
              + error.getSQLState()
              + ")",
          error);
    } finally {
      metrics.insertNanos.addAndGet(System.nanoTime() - started);
      metrics.inserts.incrementAndGet();
    }
  }

  public void inspectIntegrity() throws IOException {
    try (var statement = connection.createStatement();
        var result =
            statement.executeQuery(
                "SELECT EXISTS(SELECT 1 FROM risk.incidents WHERE NOT resolved)")) {
      result.next();
      metrics.unresolved.set(result.getBoolean(1) ? 1 : 0);
      connection.commit();
      metrics.storageHealthy = true;
    } catch (SQLException error) {
      throw new IOException("PostgreSQL integrity probe failed", error);
    }
  }

  @Override
  public void close() throws SQLException {
    connection.close();
  }
}
