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
    var matched = new ArrayList<String>();
    var reasons = new ArrayList<String>();
    var evidence = Json.MAPPER.createArrayNode();
    int score = 0;
    long time = tx.path("event_time").asLong();
    var ordered =
        rules.stream().sorted(Comparator.comparing(r -> r.path("rule_id").asText())).toList();
    for (var r : ordered) {
      long boundary = time - r.path("window_seconds").asLong() * 1000;
      long threshold = r.path("threshold").asLong();
      var recent =
          history.stream()
              .filter(
                  h ->
                      h.path("event_time").asLong() > boundary
                          && h.path("event_time").asLong() <= time)
              .toList();
      boolean enabled = r.path("enabled").asBoolean(), eligible = true, hit;
      long observed;
      switch (Rules.Type.valueOf(r.path("type").asText())) {
        case TX_COUNT_VELOCITY -> {
          observed = recent.size() + 1;
          hit = observed > threshold;
        }
        case AMOUNT_VELOCITY -> {
          observed = tx.path("amount_minor").asLong();
          for (var h : recent)
            if (h.path("currency").asText().equals(tx.path("currency").asText()))
              observed = Math.addExact(observed, h.path("amount_minor").asLong());
          eligible = tx.path("currency").asText().equals(r.path("currency").asText());
          hit = observed > threshold;
        }
        case UNIQUE_DEVICES -> {
          var unique = new HashSet<String>();
          recent.forEach(h -> unique.add(h.path("device_id").asText()));
          unique.add(tx.path("device_id").asText());
          observed = unique.size();
          hit = observed >= threshold;
        }
        case NEW_DEVICE_HIGH_AMOUNT -> {
          observed = tx.path("amount_minor").asLong();
          eligible =
              devices.getOrDefault(tx.path("device_id").asText(), Long.MIN_VALUE) <= boundary;
          hit = observed >= threshold;
        }
        case DECLINE_THEN_APPROVAL -> {
          observed =
              recent.stream().filter(h -> h.path("status").asText().equals("DECLINED")).count();
          eligible = tx.path("status").asText().equals("APPROVED");
          hit = observed >= threshold;
        }
        default -> throw new IllegalArgumentException("Unsupported rule");
      }
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
