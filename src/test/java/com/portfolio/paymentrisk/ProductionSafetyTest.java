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
        MaterializerRows.convert(first, new ConsumerRecord<>("decisions", 0, 99, null, new byte[0]))
            .evaluation();
    var b =
        MaterializerRows.convert(retry, new ConsumerRecord<>("decisions", 4, 1, null, new byte[0]))
            .evaluation();
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
}
