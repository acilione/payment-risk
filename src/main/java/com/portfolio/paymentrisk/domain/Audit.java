package com.portfolio.paymentrisk.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.portfolio.paymentrisk.Json;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

/** Stable business-result identities; processing timestamps and Kafka offsets are not revisions. */
public final class Audit {
  public static final String ENGINE_VERSION = "payment-risk/0.1.0-audit-v1";

  private Audit() {}

  public static String sha256(String value) {
    return sha256(value.getBytes(StandardCharsets.UTF_8));
  }

  public static String sha256(byte[] value) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
    } catch (java.security.NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  public static String canonical(JsonNode value) {
    if (value.isObject()) {
      var fields = new TreeMap<String, JsonNode>();
      value
          .fields()
          .forEachRemaining(e -> fields.put(e.getKey(), Json.read(canonical(e.getValue()))));
      return Json.write(fields);
    }
    if (value.isArray()) {
      var items = new ArrayList<JsonNode>();
      value.forEach(v -> items.add(Json.read(canonical(v))));
      return Json.write(items);
    }
    return Json.write(value);
  }

  // One evaluation per event, engine release and explicitly named policy release.
  // Results and rule fingerprints are deliberately not part of identity: divergence is a conflict.
  public static String evaluationId(String eventId, String engine, String policyId, long version) {
    return "eval_"
        + sha256(canonical(Json.MAPPER.valueToTree(List.of(eventId, engine, policyId, version))));
  }

  public static String inputHash(JsonNode transaction) {
    var facts = (com.fasterxml.jackson.databind.node.ObjectNode) transaction.deepCopy();
    facts.remove("producer_time");
    return sha256(canonical(facts));
  }

  public static void attachPolicy(
      com.fasterxml.jackson.databind.node.ObjectNode result,
      JsonNode transaction,
      String id,
      long version) {
    result.put("policy_id", id).put("policy_version", version);
    result.put("input_sha256", inputHash(transaction));
    result.put(
        "evaluation_id",
        evaluationId(transaction.path("event_id").asText(), ENGINE_VERSION, id, version));
  }

  public static String legacyEvaluationId(JsonNode decision) {
    return evaluationId(decision.path("event_id").asText(), "legacy", "dynamic", 0);
  }
}
