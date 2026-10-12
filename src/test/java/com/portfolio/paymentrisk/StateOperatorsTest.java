package com.portfolio.paymentrisk;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.domain.Rules;
import com.portfolio.paymentrisk.processor.*;
import java.util.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.*;
import org.junit.jupiter.api.Test;

class StateOperatorsTest {
  private KeyedBroadcastOperatorTestHarness<String, String, String, String> harness()
      throws Exception {
    return harness(AppConfig.from(Map.of()));
  }

  private KeyedBroadcastOperatorTestHarness<String, String, String, String> harness(
      AppConfig config) throws Exception {
    var operator =
        new CoBroadcastWithKeyedOperator<String, String, String, String>(
            new CustomerRiskProcessor(config), List.of(CustomerRiskProcessor.RULES));
    var h =
        new KeyedBroadcastOperatorTestHarness<String, String, String, String>(
            operator, s -> Json.read(s).path("customer_id").asText(), Types.STRING, 128, 1, 0);
    h.open();
    h.getTwoInputOperator()
        .processWatermarkStatus2(
            org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus.IDLE);
    return h;
  }

  private String tx(String id, long t) {
    return Json.write(RiskEngineTest.tx(id, t, 100, "d", "APPROVED"));
  }

  @Test
  void sameTimestampLimitsIncludeEarlierPaymentsInTheCallback() throws Exception {
    try (var h = harness(AppConfig.from(Map.of("MAX_DEVICES_PER_CUSTOMER", "1")))) {
      h.processElement(tx("a", 1000), 1000);
      h.processElement(Json.write(RiskEngineTest.tx("b", 1000, 100, "other", "APPROVED")), 1000);
      var error = assertThrows(IllegalStateException.class, () -> h.processWatermark(1000));
      assertTrue(error.getMessage().contains("device state limit"));
      assertEquals(1, h.extractOutputValues().size());
    }
    try (var h = harness(AppConfig.from(Map.of("MAX_EVENTS_PER_CUSTOMER", "2")))) {
      h.processElement(tx("old", 1000), 1000);
      h.processWatermark(1000);
      h.processElement(tx("a", 2000), 2000);
      h.processElement(tx("b", 2000), 2000);
      var error = assertThrows(IllegalStateException.class, () -> h.processWatermark(2000));
      assertTrue(error.getMessage().contains("history state limit"));
      assertEquals(2, h.extractOutputValues().size());
    }
  }

  @Test
  void rollingFeaturesMatchHistoryAcrossWindowChangesAndExpiryBoundaries() throws Exception {
    var history = new ArrayList<com.fasterxml.jackson.databind.JsonNode>();
    var seen = new HashMap<String, Long>();
    var rules = new ArrayList<com.fasterxml.jackson.databind.JsonNode>(Rules.defaults());
    var random = new Random(42);
    long time = 100_000;
    try (var h = harness()) {
      for (int i = 0; i < 600; i++) {
        if (i % 75 == 0) {
          int slot = (i / 75) % 5;
          var rule = ((ObjectNode) rules.get(slot)).deepCopy();
          rule.put("version", i + 2).put("window_seconds", 120 + (i % 3) * 300);
          rule.put("enabled", i % 150 == 0);
          if (slot == 0 && i > 0) rule.put("type", "UNIQUE_DEVICES");
          rules.set(slot, rule);
          h.processBroadcastElement(Json.write(rule), 0);
        }
        // Frequent exact boundaries as well as long gaps exercise expiration before evaluation.
        time += i % 151 == 0 ? 120_000 : 1 + random.nextInt(1_000);
        var tx =
            RiskEngineTest.tx(
                "diff-" + i,
                time,
                random.nextInt(100_000),
                "device-" + random.nextInt(12),
                i % 3 == 0 ? "DECLINED" : "APPROVED");
        String device = tx.path("device_id").asText();
        var expected =
            com.portfolio.paymentrisk.domain.RiskEngine.evaluate(
                tx, history, seen.get(device), rules, 30, 70, 0);
        h.processElement(Json.write(tx), time);
        h.processWatermark(time);
        var actual = Json.read(h.extractOutputValues().get(i));
        assertEquals(expected.path("risk_score"), actual.path("risk_score"), "score at " + i);
        assertEquals(
            expected.path("rule_evidence").toString(),
            actual.path("rule_evidence").toString(),
            "evidence at " + i);
        history.add(tx);
        seen.put(device, time);
      }
    }
  }

  @Test
  void sortsOutOfOrderRoutesLateAndExpiresHistory() throws Exception {
    try (var h = harness()) {
      h.processElement(tx("later", 100_100), 100_100);
      h.processElement(tx("earlier", 100_000), 100_000);
      h.processWatermark(new Watermark(100_100));
      var output = h.extractOutputValues();
      assertEquals(2, output.size());
      assertEquals("earlier", Json.read(output.get(0)).path("event_id").asText());
      h.processElement(tx("late", 100_000), 100_000);
      assertEquals(1, h.getSideOutput(CustomerRiskProcessor.LATE).size());
      h.processWatermark(new Watermark(4_000_000));
      h.processElement(tx("fresh", 4_100_000), 4_100_000);
      h.processWatermark(new Watermark(4_100_000));
      assertEquals(0, Json.read(h.extractOutputValues().get(2)).path("risk_score").asInt());
    }
  }

  @Test
  void idleInputsPreservePendingEventsAndCustomerHistory() throws Exception {
    try (var h = harness()) {
      var rule = (ObjectNode) Rules.defaults().get(0);
      rule.put("version", 2).put("threshold", 1);
      h.processBroadcastElement(Json.write(rule), 0);
      h.processElement(tx("first", 100_000), 100_000);
      h.processElement(tx("pending", 100_010), 100_010);
      h.processWatermark(100_000);
      assertEquals(1, h.extractOutputValues().size());
      h.getTwoInputOperator()
          .processWatermarkStatus1(
              org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus.IDLE);
      assertEquals(1, h.extractOutputValues().size(), "Idleness must not finalize a pending tail");
      assertTrue(h.numEventTimeTimers() >= 2, "Pending evaluation and expiry remain registered");
      h.getTwoInputOperator()
          .processWatermarkStatus1(
              org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus.ACTIVE);
      h.processElement(tx("resumed", 100_020), 100_020);
      h.processWatermark(100_020);
      assertEquals(3, h.extractOutputValues().size());
      assertEquals(
          25,
          Json.read(h.extractOutputValues().get(2)).path("risk_score").asInt(),
          "Resumed input retains preceding customer history");
    }
  }

  @Test
  void finalizedCustomerFrontierSurvivesWatermarkResetOnRecovery() throws Exception {
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState checkpoint;
    try (var h = harness()) {
      h.processElement(tx("finalized", 100_000), 100_000);
      h.processWatermark(100_000);
      checkpoint = h.snapshot(1, 1);
    }
    var op =
        new CoBroadcastWithKeyedOperator<String, String, String, String>(
            new CustomerRiskProcessor(AppConfig.from(Map.of())),
            List.of(CustomerRiskProcessor.RULES));
    try (var h =
        new KeyedBroadcastOperatorTestHarness<String, String, String, String>(
            op, s -> Json.read(s).path("customer_id").asText(), Types.STRING, 128, 1, 0)) {
      h.initializeState(checkpoint);
      h.open();
      h.getTwoInputOperator()
          .processWatermarkStatus2(
              org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus.IDLE);
      h.processElement(tx("retroactive", 99_999), 99_999);
      h.processElement(tx("equal", 100_000), 100_000);
      assertEquals(2, h.getSideOutput(CustomerRiskProcessor.LATE).size());
      h.processElement(tx("new", 100_001), 100_001);
      h.processWatermark(100_001);
      assertEquals(1, h.extractOutputValues().size());
    }
  }

  @Test
  void pendingSnapshotAndBroadcastStateSurviveRestore() throws Exception {
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState checkpoint;
    try (var h = harness()) {
      var update = (ObjectNode) Rules.defaults().get(0);
      update.put("version", 2).put("threshold", 0).put("score", 70);
      h.processBroadcastElement(Json.write(update), 0);
      h.processElement(tx("a", 100_000), 100_000);
      update.put("version", 3).put("enabled", false);
      h.processBroadcastElement(Json.write(update), 0);
      update.put("version", 2).put("enabled", true);
      h.processBroadcastElement(Json.write(update), 0);
      checkpoint = h.snapshot(1, 1);
    }
    var op =
        new CoBroadcastWithKeyedOperator<String, String, String, String>(
            new CustomerRiskProcessor(AppConfig.from(Map.of())),
            List.of(CustomerRiskProcessor.RULES));
    try (var h =
        new KeyedBroadcastOperatorTestHarness<String, String, String, String>(
            op, s -> Json.read(s).path("customer_id").asText(), Types.STRING, 128, 1, 0)) {
      h.initializeState(checkpoint);
      h.open();
      h.getTwoInputOperator()
          .processWatermarkStatus2(
              org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus.IDLE);
      h.processWatermark(new Watermark(100_000));
      assertEquals("REJECT", Json.read(h.extractOutputValues().get(0)).path("decision").asText());
      h.processElement(tx("b", 100_001), 100_001);
      h.processWatermark(new Watermark(100_001));
      assertEquals(0, Json.read(h.extractOutputValues().get(1)).path("risk_score").asInt());
    }
  }

  @Test
  void dedupIsGlobalRestoredAndExpiresWithoutRefresh() throws Exception {
    var op = new KeyedProcessOperator<String, String, String>(new Deduplicate(1000));
    try (var h =
        new KeyedOneInputStreamOperatorTestHarness<String, String, String>(
            op, s -> Json.read(s).path("event_id").asText(), Types.STRING)) {
      h.open();
      h.setStateTtlProcessingTime(0);
      h.processElement(new StreamRecord<>(tx("same", 1000)));
      h.setStateTtlProcessingTime(900);
      h.processElement(new StreamRecord<>(tx("same", 1000)));
      assertEquals(1, h.extractOutputValues().size());
      h.setStateTtlProcessingTime(1001);
      h.processElement(new StreamRecord<>(tx("same", 1000)));
      assertEquals(2, h.extractOutputValues().size());
    }
  }

  @Test
  void expiryTimersStayBoundedAndAllIdleCustomerStateIsRemoved() throws Exception {
    try (var h = harness()) {
      for (int i = 0; i < 100; i++) h.processElement(tx("e" + i, 100_000 + i), 100_000 + i);
      h.processWatermark(100_100);
      assertTrue(
          h.numEventTimeTimers() <= 401,
          "Three distinct rule windows, history and one active device expiry");
      h.processWatermark(4_000_000);
      assertEquals(1, h.numEventTimeTimers(), "Repeated device use retains only its latest expiry");
      h.processWatermark(3_000_000_000L);
      assertEquals(0, h.numEventTimeTimers());
      var backend =
          (org.apache.flink.runtime.state.heap.HeapKeyedStateBackend<?>)
              h.getOperator().getKeyedStateBackend();
      assertEquals(
          0,
          backend.numKeyValueStateEntries(),
          "Idle customers leave no zero counters or empty maps");
    }
  }

  @Test
  void dedupSnapshotSuppressesReplayEvenWithAnotherCustomer() throws Exception {
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState snapshot;
    try (var h =
        new KeyedOneInputStreamOperatorTestHarness<String, String, String>(
            new KeyedProcessOperator<>(new Deduplicate(10000)),
            s -> Json.read(s).path("event_id").asText(),
            Types.STRING)) {
      h.open();
      h.setStateTtlProcessingTime(100);
      h.processElement(new StreamRecord<>(tx("same", 1000)));
      snapshot = h.snapshot(1, 1);
    }
    try (var h =
        new KeyedOneInputStreamOperatorTestHarness<String, String, String>(
            new KeyedProcessOperator<>(new Deduplicate(10000)),
            s -> Json.read(s).path("event_id").asText(),
            Types.STRING)) {
      h.initializeState(snapshot);
      h.open();
      h.setStateTtlProcessingTime(200);
      var replay =
          RiskEngineTest.tx("same", 1000, 100, "d", "APPROVED").put("customer_id", "another");
      assertThrows(
          IllegalStateException.class,
          () -> h.processElement(new StreamRecord<>(Json.write(replay))));
      assertTrue(h.extractOutputValues().isEmpty());
    }
  }

  @Test
  void watchdogNeverFinalizesPaymentsAndStopsWhenQueueDrains() throws Exception {
    try (var h = harness(AppConfig.from(Map.of("PENDING_ALERT_MS", "1000")))) {
      h.processElement(tx("pending", 100_000), 100_000);
      h.setProcessingTime(5000);
      assertTrue(h.extractOutputValues().isEmpty());
      assertEquals(1, h.numProcessingTimeTimers());
      h.processWatermark(100_000);
      assertEquals(1, h.extractOutputValues().size());
      assertEquals(0, h.numProcessingTimeTimers());
    }
  }

  @Test
  void frozenPolicyIgnoresBroadcastInterleaving() throws Exception {
    String catalog =
        java.nio.file.Files.readString(java.nio.file.Path.of("config/policy-default.json"));
    var config = AppConfig.from(Map.of("POLICY_CATALOG_JSON", catalog));
    try (var before = harness(config);
        var after = harness(config)) {
      var rule = (ObjectNode) Rules.defaults().get(0);
      rule.put("version", 2).put("threshold", 0).put("score", 100);
      before.processBroadcastElement(Json.write(rule), 0);
      before.processElement(tx("event", 100_000), 100_000);
      after.processElement(tx("event", 100_000), 100_000);
      after.processBroadcastElement(Json.write(rule), 0);
      before.processWatermark(100_000);
      after.processWatermark(100_000);
      var a = (ObjectNode) Json.read(before.extractOutputValues().get(0));
      var b = (ObjectNode) Json.read(after.extractOutputValues().get(0));
      a.remove("processed_at");
      b.remove("processed_at");
      assertEquals(a, b);
      assertEquals("payment-risk-eur-demo", a.path("policy_id").asText());
      assertEquals(0, a.path("risk_score").asInt());
    }
  }

  @Test
  void anotherEventForSameTransactionCannotInflateCustomerFeatures() throws Exception {
    try (var h =
        new KeyedOneInputStreamOperatorTestHarness<String, String, String>(
            new KeyedProcessOperator<>(new Deduplicate(10000, "TRANSACTION")),
            v -> Json.read(v).path("transaction_id").asText(),
            Types.STRING)) {
      h.open();
      var first = RiskEngineTest.tx("event-a", 1000, 100, "d", "APPROVED");
      h.processElement(new StreamRecord<>(Json.write(first)));
      first.put("event_id", "event-b");
      h.processElement(new StreamRecord<>(Json.write(first)));
      assertEquals(1, h.extractOutputValues().size());
      first.put("amount_minor", 999);
      var error =
          assertThrows(
              IllegalStateException.class,
              () -> h.processElement(new StreamRecord<>(Json.write(first))));
      assertTrue(error.getMessage().contains("TRANSACTION_IDENTITY_CONFLICT"));
      assertEquals(1, h.extractOutputValues().size());
    }
  }

  @Test
  void restoredPendingPaymentKeepsItsPolicyThresholdsDuringCatalogUpgrade() throws Exception {
    var policy =
        (ObjectNode)
            Json.read(
                java.nio.file.Files.readString(
                    java.nio.file.Path.of("config/policy-default.json")));
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState checkpoint;
    try (var h = harness(AppConfig.from(Map.of("POLICY_CATALOG_JSON", Json.write(policy))))) {
      h.processElement(
          Json.write(RiskEngineTest.tx("old-policy", 100_000, 90_000, "d1", "APPROVED")), 100_000);
      checkpoint = h.snapshot(1, 1);
    }
    policy.put("version", 2).put("review_threshold", 70).put("reject_threshold", 90);
    var op =
        new CoBroadcastWithKeyedOperator<String, String, String, String>(
            new CustomerRiskProcessor(
                AppConfig.from(Map.of("POLICY_CATALOG_JSON", Json.write(policy)))),
            List.of(CustomerRiskProcessor.RULES));
    try (var h =
        new KeyedBroadcastOperatorTestHarness<String, String, String, String>(
            op, v -> Json.read(v).path("customer_id").asText(), Types.STRING, 128, 1, 0)) {
      h.initializeState(checkpoint);
      h.open();
      h.getTwoInputOperator()
          .processWatermarkStatus2(
              org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus.IDLE);
      h.processWatermark(100_000);
      h.processElement(
          Json.write(RiskEngineTest.tx("new-policy", 100_001, 90_000, "d2", "APPROVED")), 100_001);
      h.processWatermark(100_001);
      var old = Json.read(h.extractOutputValues().get(0));
      var next = Json.read(h.extractOutputValues().get(1));
      assertEquals(30, old.path("risk_score").asInt());
      assertEquals(30, next.path("risk_score").asInt());
      assertEquals("REVIEW", old.path("decision").asText());
      assertEquals("APPROVE", next.path("decision").asText());
      assertEquals(1, old.path("policy_version").asLong());
      assertEquals(2, next.path("policy_version").asLong());
    }
  }
}
