package com.portfolio.paymentrisk.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.portfolio.paymentrisk.Json;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** Synthetic labels stay in the evaluation fixture, never in the payment sent to the engine. */
public final class CustomerScenarios {
  private CustomerScenarios() {}

  public record Sample(JsonNode scenario, ObjectNode payment, boolean target) {}

  public static List<Sample> samples(String run, long endTime) throws IOException {
    JsonNode dataset;
    try (var in = CustomerScenarios.class.getResourceAsStream("/customer-scenarios.json")) {
      if (in == null) throw new IOException("Missing customer scenarios");
      dataset = Json.read(new String(in.readAllBytes(), StandardCharsets.UTF_8));
    }
    var samples = new ArrayList<Sample>();
    for (var scenario : dataset.path("cases")) {
      String customer = run + "-" + scenario.path("id").asText();
      var payments = scenario.path("payments");
      for (int i = 0; i < payments.size(); i++) {
        var p = payments.get(i);
        var payment =
            IntegrationScenario.transaction(
                customer + "-" + i,
                customer,
                endTime + p.path("seconds").asLong() * 1000,
                p.path("amount_minor").asLong(),
                customer + "-" + p.path("device").asText(),
                p.path("status").asText());
        payment.put("merchant_id", "synthetic-" + p.path("merchant").asText());
        // A reproducible fixture timestamp; it is not a measured publishing latency.
        payment.put("producer_time", endTime);
        samples.add(new Sample(scenario, payment, i == payments.size() - 1));
      }
    }
    samples.sort(
        Comparator.comparingLong((Sample s) -> s.payment().path("event_time").asLong())
            .thenComparing(s -> s.payment().path("event_id").asText()));
    return List.copyOf(samples);
  }

  public static ObjectNode report(List<Sample> samples, Collection<JsonNode> decisions) {
    var byId = new HashMap<String, JsonNode>();
    for (var decision : decisions) {
      String id = decision.path("event_id").asText();
      if (byId.putIfAbsent(id, decision) != null)
        throw new AssertionError("Duplicate decision: " + id);
    }
    var expected = new HashSet<String>();
    samples.forEach(s -> expected.add(s.payment().path("event_id").asText()));
    if (expected.size() != samples.size() || !byId.keySet().equals(expected))
      throw new AssertionError("Scenario input/output identities do not reconcile");
    var results = Json.MAPPER.createArrayNode();
    int tp = 0, fp = 0, tn = 0, fn = 0;
    for (var sample : samples) {
      if (!sample.target()) continue;
      var scenario = sample.scenario();
      var decision = byId.get(sample.payment().path("event_id").asText());
      int expectedScore = scenario.path("expected_score").asInt();
      String expectedDecision =
          expectedScore >= 70 ? "REJECT" : expectedScore >= 30 ? "REVIEW" : "APPROVE";
      if (decision.path("risk_score").asInt(-1) != expectedScore
          || !decision.path("decision").asText().equals(expectedDecision)
          || !decision.path("matched_rules").equals(scenario.path("expected_rules")))
        throw new AssertionError(
            "Unexpected result for " + scenario.path("id").asText() + ": " + decision);
      String label = scenario.path("label").asText();
      if (!Set.of("SUSPICIOUS", "LEGITIMATE").contains(label))
        throw new AssertionError("Unknown label");
      boolean positive = label.equals("SUSPICIOUS");
      boolean alerted = !decision.path("decision").asText().equals("APPROVE");
      String outcome;
      if (positive && alerted) {
        tp++;
        outcome = "TRUE_POSITIVE";
      } else if (!positive && alerted) {
        fp++;
        outcome = "FALSE_POSITIVE";
      } else if (positive) {
        fn++;
        outcome = "FALSE_NEGATIVE";
      } else {
        tn++;
        outcome = "TRUE_NEGATIVE";
      }
      var result =
          Json.object()
              .put("scenario", scenario.path("id").asText())
              .put("story", scenario.path("story").asText())
              .put("label", label)
              .put("outcome", outcome);
      result.set("target_payment", sample.payment());
      result.set("decision", decision);
      var history = Json.MAPPER.createArrayNode();
      samples.stream()
          .filter(s -> s.payment().path("customer_id").equals(sample.payment().path("customer_id")))
          .forEach(
              s -> {
                var entry = Json.object();
                entry.set("payment", s.payment());
                entry.set("decision", byId.get(s.payment().path("event_id").asText()));
                history.add(entry);
              });
      result.set("timeline", history);
      results.add(result);
    }
    var report =
        Json.object()
            .put("result", "PASS")
            .put("dataset_version", 1)
            .put(
                "scope",
                "One labeled final payment per authored story; synthetic coverage, not real fraud accuracy")
            .put("input_payments", samples.size())
            .put("decisions", byId.size())
            .put("labeled_targets", results.size())
            .put("true_positives", tp)
            .put("false_positives", fp)
            .put("true_negatives", tn)
            .put("false_negatives", fn);
    ratio(report, "precision", tp, tp + fp);
    ratio(report, "recall", tp, tp + fn);
    ratio(report, "false_positive_rate", fp, fp + tn);
    report.set("cases", results);
    return report;
  }

  private static void ratio(ObjectNode report, String name, int numerator, int denominator) {
    if (denominator == 0) report.putNull(name);
    else report.put(name, (double) numerator / denominator);
  }
}
