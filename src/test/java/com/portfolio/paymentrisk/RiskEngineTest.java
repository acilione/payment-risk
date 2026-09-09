package com.portfolio.paymentrisk;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.domain.*;
import com.portfolio.paymentrisk.processor.Validate;
import java.util.*;
import org.junit.jupiter.api.Test;

class RiskEngineTest {
  static ObjectNode tx(String id, long time, long amount, String device, String status) {
    return Json.object()
        .put("event_id", id)
        .put("transaction_id", "txn-" + id)
        .put("customer_id", "C1")
        .put("merchant_id", "M1")
        .put("amount_minor", amount)
        .put("currency", "EUR")
        .put("country", "IT")
        .put("device_id", device)
        .put("status", status)
        .put("event_time", time)
        .put("producer_time", time);
  }

  @Test
  void sixthTransactionAndAllFiveRules() {
    List<JsonNode> history = new ArrayList<>();
    Map<String, Long> devices = new HashMap<>();
    long base = 1_000_000;
    ObjectNode decision = null;
    for (int i = 0; i < 6; i++) {
      var t = tx("e" + i, base + i * 20_000, 90_000, "d" + i, i < 4 ? "DECLINED" : "APPROVED");
      decision = RiskEngine.evaluate(t, history, devices, Rules.defaults(), 30, 70, 2_000_000);
      history.add(t);
      devices.put("d" + i, t.path("event_time").asLong());
    }
    assertEquals(100, decision.path("risk_score").asInt());
    assertEquals("REJECT", decision.path("decision").asText());
    assertEquals(5, decision.path("matched_rules").size());
  }

  @Test
  void exactBoundaryExcludesOldPaymentAndIncludesCurrent() {
    var rule = Rules.defaults().get(0).deepCopy();
    ((ObjectNode) rule).put("threshold", 1);
    var result =
        RiskEngine.evaluate(
            tx("b", 200_000, 100, "d", "APPROVED"),
            List.of(tx("a", 80_000, 100, "d", "APPROVED")),
            Map.of(),
            List.of(rule),
            30,
            70,
            300_000);
    assertEquals(0, result.path("risk_score").asInt());
    result =
        RiskEngine.evaluate(
            tx("b", 199_999, 100, "d", "APPROVED"),
            List.of(tx("a", 80_000, 100, "d", "APPROVED")),
            Map.of(),
            List.of(rule),
            30,
            70,
            300_000);
    assertEquals(25, result.path("risk_score").asInt());
  }

  @Test
  void newDeviceUsesPriorHistoryAndScoreThresholds() {
    var t = tx("e", 1_000_000, 80_000, "A", "APPROVED");
    assertEquals(
        "REVIEW",
        RiskEngine.evaluate(t, List.of(), Map.of(), Rules.defaults(), 30, 70, 2_000_000)
            .path("decision")
            .asText());
    assertEquals(
        "APPROVE",
        RiskEngine.evaluate(
                t, List.of(), Map.of("A", 999_000L), Rules.defaults(), 30, 70, 2_000_000)
            .path("decision")
            .asText());
  }

  @Test
  void fingerprintsIncludeParametersAndAreOrderIndependent() {
    var rules = Rules.defaults();
    var reversed = new ArrayList<>(rules);
    Collections.reverse(reversed);
    assertEquals(Rules.fingerprint(rules, 30, 70), Rules.fingerprint(reversed, 30, 70));
    ((ObjectNode) reversed.get(0)).put("threshold", 999);
    assertNotEquals(
        Rules.fingerprint(Rules.defaults(), 30, 70), Rules.fingerprint(reversed, 30, 70));
  }

  @Test
  void invalidRulesAndTransactionsRejected() {
    var c = AppConfig.from(Map.of());
    for (var r : Rules.defaults()) assertDoesNotThrow(() -> Rules.validate(r, c));
    var r = (ObjectNode) Rules.defaults().get(0);
    r.put("window_seconds", 3601);
    assertThrows(IllegalArgumentException.class, () -> Rules.validate(r, c));
    var t = tx("e", 1_000, 100, "d", "APPROVED");
    t.put("currency", "USD");
    assertThrows(IllegalArgumentException.class, () -> Validate.transaction(t, 2_000));
    t.put("currency", "EUR").put("event_time", 999_999);
    assertThrows(IllegalArgumentException.class, () -> Validate.transaction(t, 2_000));
  }

  @Test
  void disabledRuleAndNoCrossCurrencySum() {
    var r = (ObjectNode) Rules.defaults().get(1);
    r.put("enabled", false);
    var t = tx("e", 1_000_000, 500_000, "d", "APPROVED");
    assertEquals(
        0,
        RiskEngine.evaluate(t, List.of(), Map.of(), List.of(r), 30, 70, 2_000_000)
            .path("risk_score")
            .asInt());
    r.put("enabled", true);
    t.put("amount_minor", 100);
    var other = tx("x", 999_000, 999_999, "d", "APPROVED").put("currency", "USD");
    assertEquals(
        0,
        RiskEngine.evaluate(t, List.of(other), Map.of(), List.of(r), 30, 70, 2_000_000)
            .path("risk_score")
            .asInt());
  }
}
