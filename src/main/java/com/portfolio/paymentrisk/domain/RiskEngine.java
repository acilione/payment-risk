package com.portfolio.paymentrisk.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.portfolio.paymentrisk.Json;
import java.util.*;

/** Pure evaluation. History excludes current event. Windows are (t-window,t]. */
public final class RiskEngine {
  private RiskEngine() {}

  public static ObjectNode evaluate(
      JsonNode tx,
      List<JsonNode> history,
      Map<String, Long> devices,
      List<JsonNode> rules,
      int review,
      int reject,
      long processedAt) {
    return evaluate(
        tx,
        history,
        devices.get(tx.path("device_id").asText()),
        rules,
        review,
        reject,
        processedAt);
  }

  public static ObjectNode evaluate(
      JsonNode tx,
      Iterable<JsonNode> history,
      Long deviceLastSeen,
      List<JsonNode> rules,
      int review,
      int reject,
      long processedAt) {
    var matched = new ArrayList<String>();
    var reasons = new ArrayList<String>();
    var evidence = Json.MAPPER.createArrayNode();
    int score = 0;
    long time = tx.path("event_time").asLong();
    var ordered =
        rules.stream().sorted(Comparator.comparing(r -> r.path("rule_id").asText())).toList();
    var observations = RiskFeatures.observe(tx, history, deviceLastSeen, ordered);
    for (var r : ordered) {
      long threshold = r.path("threshold").asLong();
      var observation = observations.get(r.path("rule_id").asText());
      boolean enabled = r.path("enabled").asBoolean();
      boolean eligible = observation.eligible(), hit = observation.matched();
      long observed = observation.value();
      hit = hit && eligible && enabled;
      int contribution = hit ? r.path("score").asInt() : 0;
      evidence.add(
          Json.object()
              .put("rule_id", r.path("rule_id").asText())
              .put("rule_version", r.path("version").asLong())
              .put("type", r.path("type").asText())
              .put("enabled", enabled)
              .put("window_seconds", r.path("window_seconds").asLong())
              .put("threshold", threshold)
              .put("observed", observed)
              .put("eligible", eligible)
              .put("matched", hit)
              .put("score_contribution", contribution));
      if (hit) {
        score += contribution;
        matched.add(r.path("rule_id").asText());
        reasons.add(r.path("type").asText());
      }
    }
    score = Math.min(100, score);
    String fingerprint = Rules.fingerprint(ordered, review, reject);
    var snapshot = Json.object().put("review_threshold", review).put("reject_threshold", reject);
    snapshot.set("rules", Json.MAPPER.valueToTree(ordered));
    var result =
        Json.object()
            .put("decision_id", "risk_" + tx.path("transaction_id").asText())
            .put(
                "evaluation_id",
                Audit.evaluationId(
                    tx.path("event_id").asText(), Audit.ENGINE_VERSION, "dynamic", 0))
            .put("engine_version", Audit.ENGINE_VERSION)
            .put("policy_snapshot", Audit.canonical(snapshot))
            .put("event_id", tx.path("event_id").asText())
            .put("transaction_id", tx.path("transaction_id").asText())
            .put("customer_id", tx.path("customer_id").asText())
            .put("amount_minor", tx.path("amount_minor").asLong())
            .put("currency", tx.path("currency").asText())
            .put("risk_score", score)
            .put("decision", score >= reject ? "REJECT" : score >= review ? "REVIEW" : "APPROVE")
            .put("rules_fingerprint", fingerprint)
            .put("event_time", time)
            .put("processed_at", processedAt);
    result.set("matched_rules", Json.MAPPER.valueToTree(matched));
    result.set("reason_codes", Json.MAPPER.valueToTree(reasons));
    result.set("rule_evidence", evidence);
    Audit.attachPolicy(result, tx, "dynamic", 0);
    return result;
  }
}
