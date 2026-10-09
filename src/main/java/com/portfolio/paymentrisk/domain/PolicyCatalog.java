package com.portfolio.paymentrisk.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.portfolio.paymentrisk.Json;
import com.portfolio.paymentrisk.config.AppConfig;
import java.util.*;

/** One immutable catalog per job deployment. No ordering assumptions between Kafka inputs. */
public record PolicyCatalog(String id, long version, List<JsonNode> rules, int review, int reject) {
  public PolicyCatalog {
    rules = List.copyOf(rules);
  }

  public static PolicyCatalog from(AppConfig c) {
    var p = Json.read(c.policyJson());
    if (!p.path("policy_id").asText().matches("[a-zA-Z0-9._-]{1,128}")
        || p.path("version").asLong() < 1
        || !p.path("rules").isArray()
        || p.path("rules").size() != 5
        || !p.path("review_threshold").canConvertToInt()
        || !p.path("reject_threshold").canConvertToInt()
        || p.path("review_threshold").asInt() < 1
        || p.path("reject_threshold").asInt() > 100
        || p.path("reject_threshold").asInt() <= p.path("review_threshold").asInt())
      throw new IllegalArgumentException("Invalid immutable policy catalog");
    var ids = new HashSet<String>();
    var rules = new ArrayList<JsonNode>();
    for (var r : p.path("rules")) {
      Rules.validate(r, c);
      if (!ids.add(r.path("rule_id").asText()))
        throw new IllegalArgumentException("Duplicate catalog rule");
      rules.add(r.deepCopy());
    }
    rules.sort(Comparator.comparing(r -> r.path("rule_id").asText()));
    return new PolicyCatalog(
        p.path("policy_id").asText(),
        p.path("version").asLong(),
        rules,
        p.path("review_threshold").asInt(),
        p.path("reject_threshold").asInt());
  }
}
