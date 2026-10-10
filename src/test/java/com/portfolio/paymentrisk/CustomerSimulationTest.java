package com.portfolio.paymentrisk;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.processor.CustomerRiskProcessor;
import com.portfolio.paymentrisk.processor.Validate;
import com.portfolio.paymentrisk.tools.CustomerScenarios;
import java.util.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.junit.jupiter.api.Test;

class CustomerSimulationTest {
  @Test
  void customerStoriesExposeFalseAlarmsAndMissedSuspiciousPayments() throws Exception {
    long now = 1_800_000_000_000L;
    var samples = CustomerScenarios.samples("test", now);
    assertEquals(samples, CustomerScenarios.samples("test", now), "Fixture must be reproducible");
    var operator =
        new CoBroadcastWithKeyedOperator<String, String, String, String>(
            new CustomerRiskProcessor(AppConfig.from(Map.of())),
            List.of(CustomerRiskProcessor.RULES));
    try (var harness =
        new KeyedBroadcastOperatorTestHarness<String, String, String, String>(
            operator, s -> Json.read(s).path("customer_id").asText(), Types.STRING, 128, 1, 0)) {
      harness.open();
      harness.getTwoInputOperator().processWatermarkStatus2(WatermarkStatus.IDLE);
      long previous = -1;
      for (var sample : samples) {
        var payment = sample.payment();
        Validate.transaction(payment, now);
        assertFalse(payment.has("label"));
        assertFalse(payment.has("expected_score"));
        long time = payment.path("event_time").asLong();
        if (time != previous && previous >= 0) harness.processWatermark(previous);
        harness.processElement(Json.write(payment), time);
        previous = time;
      }
      harness.processWatermark(now);
      List<JsonNode> decisions = harness.extractOutputValues().stream().map(Json::read).toList();
      var report = CustomerScenarios.report(samples, decisions);
      assertEquals(9, report.path("labeled_targets").asInt());
      assertEquals(3, report.path("true_positives").asInt());
      assertEquals(1, report.path("false_positives").asInt());
      assertEquals(3, report.path("true_negatives").asInt());
      assertEquals(2, report.path("false_negatives").asInt());
      assertEquals(0.75, report.path("precision").asDouble());
      assertEquals(0.6, report.path("recall").asDouble());
      assertEquals(0.25, report.path("false_positive_rate").asDouble());
      var duplicates = new ArrayList<>(decisions);
      duplicates.add(decisions.get(0));
      assertThrows(AssertionError.class, () -> CustomerScenarios.report(samples, duplicates));
      assertThrows(
          AssertionError.class,
          () -> CustomerScenarios.report(samples, decisions.subList(1, decisions.size())));
      var altered = new ArrayList<>(decisions);
      var targetId =
          samples.stream()
              .filter(CustomerScenarios.Sample::target)
              .findFirst()
              .orElseThrow()
              .payment()
              .path("event_id")
              .asText();
      for (int i = 0; i < altered.size(); i++) {
        if (altered.get(i).path("event_id").asText().equals(targetId)) {
          var incorrect =
              (com.fasterxml.jackson.databind.node.ObjectNode) altered.get(i).deepCopy();
          incorrect.put("risk_score", 99);
          altered.set(i, incorrect);
        }
      }
      assertThrows(AssertionError.class, () -> CustomerScenarios.report(samples, altered));
      assertTrue(
          harness.getSideOutput(CustomerRiskProcessor.LATE) == null
              || harness.getSideOutput(CustomerRiskProcessor.LATE).isEmpty());
    }
  }
}
