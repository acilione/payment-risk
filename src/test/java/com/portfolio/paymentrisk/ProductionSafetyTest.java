package com.portfolio.paymentrisk;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.domain.*;
import com.portfolio.paymentrisk.serialization.AvroCodec;
import com.portfolio.paymentrisk.tools.*;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class ProductionSafetyTest {
  private ObjectNode decision(long now) {
    return RiskEngine.evaluate(
        RiskEngineTest.tx("event", 1000, 100, "device", "APPROVED"),
        List.of(),
        Map.of(),
        Rules.defaults(),
        30,
        70,
        now);
  }

  @Test
  void retryIdentityIsIndependentOfProcessingTimeAndKafkaCoordinates() {
    var first = decision(2000);
    var retry = decision(9000);
    assertEquals(first.path("evaluation_id"), retry.path("evaluation_id"));
    var a =
        MaterializerRows.convert(
            first, new ConsumerRecord<>("decisions", 0, 99, null, new byte[0]));
    var b =
        MaterializerRows.convert(retry, new ConsumerRecord<>("decisions", 4, 1, null, new byte[0]));
    assertEquals(a.path("evaluation_id"), b.path("evaluation_id"));
    assertEquals(a.path("input_sha256"), b.path("input_sha256"));
    var changed =
        RiskEngine.evaluate(
            RiskEngineTest.tx("event", 1000, 999, "other", "APPROVED"),
            List.of(),
            Map.of(),
            Rules.defaults(),
            30,
            70,
            9000);
    assertEquals(
        first.path("evaluation_id"),
        changed.path("evaluation_id"),
        "Conflicting content must not mint another identity");
    assertNotEquals(first.path("input_sha256"), changed.path("input_sha256"));
    var changedRules = new ArrayList<>(Rules.defaults());
    ((ObjectNode) changedRules.get(0)).put("threshold", 0);
    var divergent =
        RiskEngine.evaluate(
            RiskEngineTest.tx("event", 1000, 100, "device", "APPROVED"),
            List.of(),
            Map.of(),
            changedRules,
            30,
            70,
            9000);
    assertEquals(first.path("evaluation_id"), divergent.path("evaluation_id"));
    assertNotEquals(first.path("rules_fingerprint"), divergent.path("rules_fingerprint"));
  }

  @Test
  void auditSurvivesAvroAndExplainsEveryRule() throws Exception {
    var value = decision(2000);
    var schema = AvroCodec.schema("risk-decision");
    assertEquals(
        Json.read(Json.write(value)),
        Json.read(AvroCodec.decode(AvroCodec.encode(schema, 1, value), schema, schema)));
    assertEquals(5, value.path("rule_evidence").size());
    assertEquals(1, value.path("rule_evidence").get(0).path("observed").asLong());
    assertEquals(5, Json.read(value.path("policy_snapshot").asText()).path("rules").size());
  }

  @Test
  void immutablePolicyRequiredOutsideLocalAndChecksumVerified() throws Exception {
    assertThrows(
        IllegalArgumentException.class,
        () -> AppConfig.from(Map.of("APP_ENVIRONMENT", "production")));
    String policy = Files.readString(Path.of("config/policy-default.json"));
    var config =
        AppConfig.from(
            Map.of(
                "APP_ENVIRONMENT",
                "staging",
                "POLICY_CATALOG_JSON",
                policy,
                "POLICY_SHA256",
                Audit.sha256(policy)));
    assertEquals(5, PolicyCatalog.from(config).rules().size());
    assertThrows(
        IllegalArgumentException.class,
        () -> AppConfig.from(Map.of("POLICY_CATALOG_JSON", policy, "POLICY_SHA256", "incorrect")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            AppConfig.from(
                Map.of(
                    "APP_ENVIRONMENT",
                    "staging",
                    "POLICY_CATALOG_JSON",
                    policy,
                    "DLQ_PAYLOAD_MODE",
                    "capture")));
  }

  @Test
  void unknownInsertOutcomeRetriesIdenticalRowsAndCommitsOnlyAfterQuarantine() throws Exception {
    var calls = new ArrayList<String>();
    var server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          assertTrue(exchange.getRequestURI().getQuery().contains("wait_end_of_query=1"));
          calls.add(
              new String(
                  exchange.getRequestBody().readAllBytes(),
                  java.nio.charset.StandardCharsets.UTF_8));
          if (calls.size() == 1) {
            exchange.close();
            return;
          } // accepted insert, lost response
          exchange.sendResponseHeaders(200, -1);
          exchange.close();
        });
    server.start();
    try (var metrics = new MaterializerMetrics(0)) {
      var writer =
          new ClickHouseWriter(
              "http://localhost:" + server.getAddress().getPort(),
              "test",
              "not-a-secret",
              metrics,
              2);
      AtomicBoolean committed = new AtomicBoolean();
      DecisionMaterializer.persist(
          writer,
          "{evaluation}\n",
          "{rejection}\n",
          () -> {
            assertEquals(3, calls.size());
            committed.set(true);
          });
      assertTrue(committed.get());
      assertEquals(calls.get(0), calls.get(1), "Retry after ambiguous outcome is identical");
      assertTrue(calls.get(2).startsWith("INSERT INTO risk.materializer_rejections"));
      assertEquals(1, metrics.retries.get());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void quarantineFailureLeavesEntireBatchUncommittedAndFatalErrorsAreNotRetried() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    var server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          exchange.getRequestBody().readAllBytes();
          exchange.sendResponseHeaders(calls.incrementAndGet() == 1 ? 200 : 403, -1);
          exchange.close();
        });
    server.start();
    try (var metrics = new MaterializerMetrics(0)) {
      var writer =
          new ClickHouseWriter(
              "http://localhost:" + server.getAddress().getPort(), "test", "not-a-secret", metrics);
      AtomicBoolean committed = new AtomicBoolean();
      assertThrows(
          java.io.IOException.class,
          () -> DecisionMaterializer.persist(writer, "{}\n", "{}\n", () -> committed.set(true)));
      assertFalse(committed.get());
      assertEquals(2, calls.get());
      assertFalse(metrics.storageHealthy);
    } finally {
      server.stop(0);
    }
  }

  @Test
  void integrityProbeReportsPersistedIncidentsWithoutNewRecords() throws Exception {
    var server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          exchange.getRequestBody().readAllBytes();
          byte[] body = "2\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    server.start();
    try (var metrics = new MaterializerMetrics(0)) {
      new ClickHouseWriter(
              "http://localhost:" + server.getAddress().getPort(), "test", "not-a-secret", metrics)
          .inspectIntegrity();
      assertEquals(2, metrics.unresolved.get());
      assertTrue(metrics.storageHealthy);
      assertTrue(metrics.prometheus().contains("risk_materializer_unresolved_records 2.0"));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void invalidPayloadIsOmittedFromDeadLettersByDefault() throws Exception {
    var op =
        new org.apache.flink.streaming.api.operators.ProcessOperator<
            com.portfolio.paymentrisk.source.RawRecord, String>(
            new com.portfolio.paymentrisk.processor.Validate(AppConfig.from(Map.of()), false));
    try (var h =
        new org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness<
            com.portfolio.paymentrisk.source.RawRecord, String>(op)) {
      h.open();
      byte[] payload =
          "synthetic-private-payment".getBytes(java.nio.charset.StandardCharsets.UTF_8);
      var raw =
          new com.portfolio.paymentrisk.source.RawRecord("payments.raw", 1, 42, 1000, payload);
      raw.validationError = "DESERIALIZATION_ERROR";
      h.processElement(new org.apache.flink.streaming.runtime.streamrecord.StreamRecord<>(raw));
      var error =
          Json.read(
              h.getSideOutput(com.portfolio.paymentrisk.processor.Validate.DLQ)
                  .element()
                  .getValue());
      assertEquals("", error.path("raw_payload").asText());
      assertEquals(Audit.sha256(payload), error.path("payload_sha256").asText());
      assertEquals(payload.length, error.path("payload_bytes").asInt());
      assertEquals(42, error.path("source_offset").asLong());
      assertEquals("omit", error.path("payload_mode").asText());
      assertTrue(error.path("payload_truncated").asBoolean());
    }
  }

  @Test
  void exhaustedStorageRetriesNeverCommitKafkaOffsets() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    var server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          exchange.getRequestBody().readAllBytes();
          calls.incrementAndGet();
          exchange.sendResponseHeaders(503, -1);
          exchange.close();
        });
    server.start();
    try (var metrics = new MaterializerMetrics(0)) {
      var writer =
          new ClickHouseWriter(
              "http://localhost:" + server.getAddress().getPort(),
              "test",
              "not-a-secret",
              metrics,
              2);
      AtomicBoolean committed = new AtomicBoolean();
      assertThrows(
          java.io.IOException.class,
          () -> DecisionMaterializer.persist(writer, "{}\n", "", () -> committed.set(true)));
      assertFalse(committed.get());
      assertEquals(2, calls.get());
      assertEquals(1, metrics.retries.get());
      assertFalse(metrics.storageHealthy);
    } finally {
      server.stop(0);
    }
  }

  @Test
  void negativePolicyVersionIsRejectedBeforeClickHouseInsertion() {
    var value = decision(2000).put("policy_version", -1);
    value.put("evaluation_id", Audit.evaluationId("event", Audit.ENGINE_VERSION, "dynamic", -1));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            MaterializerRows.convert(
                value, new ConsumerRecord<>("decisions", 0, 0, null, new byte[0])));
  }

  @Test
  void policyNumbersAreNeverSilentlyTruncatedOrCoerced() throws Exception {
    var catalog = (ObjectNode) Json.read(Files.readString(Path.of("config/policy-default.json")));
    catalog.put("review_threshold", 30.9);
    assertThrows(
        IllegalArgumentException.class,
        () -> AppConfig.from(Map.of("POLICY_CATALOG_JSON", Json.write(catalog))));
    catalog.put("review_threshold", 30);
    ((ObjectNode) catalog.path("rules").get(0)).put("threshold", "5");
    assertThrows(
        IllegalArgumentException.class,
        () -> AppConfig.from(Map.of("POLICY_CATALOG_JSON", Json.write(catalog))));
    ((ObjectNode) catalog.path("rules").get(0)).put("threshold", 5.5);
    assertThrows(
        IllegalArgumentException.class,
        () -> AppConfig.from(Map.of("POLICY_CATALOG_JSON", Json.write(catalog))));
  }
}
