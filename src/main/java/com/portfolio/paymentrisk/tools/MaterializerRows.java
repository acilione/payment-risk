package com.portfolio.paymentrisk.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.portfolio.paymentrisk.Json;
import com.portfolio.paymentrisk.domain.Audit;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public final class MaterializerRows {
  private MaterializerRows() {}

  private static final List<String> LEGACY =
      List.of(
          "decision_id",
          "event_id",
          "transaction_id",
          "customer_id",
          "amount_minor",
          "currency",
          "risk_score",
          "decision",
          "matched_rules",
          "reason_codes",
          "rules_fingerprint",
          "event_time",
          "processed_at");

  public static ObjectNode convert(ObjectNode decision, ConsumerRecord<String, byte[]> source) {
    for (String field : List.of("decision_id", "event_id", "transaction_id", "customer_id"))
      if (decision.path(field).asText().isBlank() || decision.path(field).asText().length() > 256)
        throw new IllegalArgumentException("INVALID_DECISION");
    if (decision.path("risk_score").asInt(-1) < 0
        || decision.path("risk_score").asInt() > 100
        || !List.of("APPROVE", "REVIEW", "REJECT").contains(decision.path("decision").asText())
        || !decision.path("currency").asText().equals("EUR")
        || decision.path("amount_minor").asLong() < 1
        || decision.path("amount_minor").asLong() > 1_000_000_000_000L
        || decision.path("policy_version").asLong() < 0
        || decision.path("event_time").asLong() <= 0
        || decision.path("processed_at").asLong() <= 0)
      throw new IllegalArgumentException("INVALID_DECISION");
    String id = decision.path("evaluation_id").asText();
    if (id.isEmpty()) id = Audit.legacyEvaluationId(decision);
    else if (!id.equals(
        Audit.evaluationId(
            decision.path("event_id").asText(),
            decision.path("engine_version").asText(),
            decision.path("policy_id").asText(),
            decision.path("policy_version").asLong())))
      throw new IllegalArgumentException("INVALID_EVALUATION_ID");
    if (!id.matches("eval_[0-9a-f]{64}"))
      throw new IllegalArgumentException("INVALID_EVALUATION_ID");
    var row = Json.object();
    for (String field : LEGACY) row.set(field, decision.get(field));
    row.put("source_partition", source.partition()).put("source_offset", source.offset());
    return row.put("evaluation_id", id)
        .put("engine_version", decision.path("engine_version").asText("legacy"))
        .put("policy_id", decision.path("policy_id").asText("dynamic"))
        .put("policy_version", decision.path("policy_version").asLong())
        .put("policy_snapshot", decision.path("policy_snapshot").asText())
        .put("rule_evidence", Json.write(decision.path("rule_evidence")))
        .put("input_sha256", decision.path("input_sha256").asText())
        .put("source_topic", source.topic());
  }

  public static ObjectNode rejection(ConsumerRecord<String, byte[]> record, String code) {
    byte[] payload = record.value() == null ? new byte[0] : record.value();
    return Json.object()
        .put("source_topic", record.topic())
        .put("source_partition", record.partition())
        .put("source_offset", record.offset())
        .put("source_timestamp", record.timestamp())
        .put("error_code", code)
        .put("payload_sha256", Audit.sha256(payload))
        .put("payload_bytes", payload.length)
        .put("observed_at", System.currentTimeMillis());
  }
}
