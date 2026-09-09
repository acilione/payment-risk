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
    int score = 0;
    long time = tx.path("event_time").asLong();
    for (var r :
        rules.stream().sorted(Comparator.comparing(x -> x.path("rule_id").asText())).toList()) {
      if (!r.path("enabled").asBoolean()) continue;
      long boundary = time - r.path("window_seconds").asLong() * 1000,
          threshold = r.path("threshold").asLong();
      var recent =
          history.stream()
              .filter(
                  h ->
                      h.path("event_time").asLong() > boundary
                          && h.path("event_time").asLong() <= time)
              .toList();
      boolean hit =
          switch (Rules.Type.valueOf(r.path("type").asText())) {
            case TX_COUNT_VELOCITY -> recent.size() + 1 > threshold;
            case AMOUNT_VELOCITY -> {
              long amount = tx.path("amount_minor").asLong();
              for (var h : recent)
                if (h.path("currency").asText().equals(tx.path("currency").asText()))
                  amount = Math.addExact(amount, h.path("amount_minor").asLong());
              yield amount > threshold
                  && tx.path("currency").asText().equals(r.path("currency").asText());
            }
            case UNIQUE_DEVICES -> {
              var unique = new HashSet<String>();
              recent.forEach(h -> unique.add(h.path("device_id").asText()));
              unique.add(tx.path("device_id").asText());
              yield unique.size() >= threshold;
            }
            case NEW_DEVICE_HIGH_AMOUNT ->
                devices.getOrDefault(tx.path("device_id").asText(), Long.MIN_VALUE) <= boundary
                    && tx.path("amount_minor").asLong() >= threshold;
            case DECLINE_THEN_APPROVAL ->
                tx.path("status").asText().equals("APPROVED")
                    && recent.stream()
                            .filter(h -> h.path("status").asText().equals("DECLINED"))
                            .count()
                        >= threshold;
          };
      if (hit) {
        score += r.path("score").asInt();
        matched.add(r.path("rule_id").asText());
        reasons.add(r.path("type").asText());
      }
    }
    score = Math.min(100, score);
    var result =
        Json.object()
            .put("decision_id", "risk_" + tx.path("transaction_id").asText())
            .put("event_id", tx.path("event_id").asText())
            .put("transaction_id", tx.path("transaction_id").asText())
            .put("customer_id", tx.path("customer_id").asText())
            .put("amount_minor", tx.path("amount_minor").asLong())
            .put("currency", tx.path("currency").asText())
            .put("risk_score", score)
            .put("decision", score >= reject ? "REJECT" : score >= review ? "REVIEW" : "APPROVE")
            .put("rules_fingerprint", Rules.fingerprint(rules, review, reject))
            .put("event_time", time)
            .put("processed_at", processedAt);
    result.set("matched_rules", Json.MAPPER.valueToTree(matched));
    result.set("reason_codes", Json.MAPPER.valueToTree(reasons));
    return result;
  }
}
