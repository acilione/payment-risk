package com.portfolio.paymentrisk;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.portfolio.paymentrisk.domain.RiskEngine;
import com.portfolio.paymentrisk.domain.Rules;
import com.portfolio.paymentrisk.tools.CustomerScenarios;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class InvestigationFixtureTest {
  @Test
  void websiteEvidenceMatchesTheEngineAndAuthoredScenarios() throws Exception {
    long end = 1791540000000L;
    var samples = CustomerScenarios.samples("demo", end);
    var histories = new HashMap<String, List<JsonNode>>();
    var devices = new HashMap<String, Long>();
    var decisions = new ArrayList<JsonNode>();
    for (var sample : samples) {
      var tx = sample.payment();
      var history =
          histories.computeIfAbsent(tx.path("customer_id").asText(), key -> new ArrayList<>());
      decisions.add(
          RiskEngine.evaluate(tx, history, devices, Rules.defaults(), 30, 70, end + 20000));
      history.add(tx);
      devices.put(tx.path("device_id").asText(), tx.path("event_time").asLong());
    }
    var report = CustomerScenarios.report(samples, decisions);
    var path = Path.of("web/src/investigation-fixture.json");
    if (Boolean.getBoolean("risk.fixture.write")) {
      Files.writeString(
          path, Json.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(report) + "\n");
    }
    assertEquals(
        Json.read(Json.write(report)),
        Json.read(Files.readString(path)),
        "Regenerate with -Drisk.fixture.write=true after reviewing scenario changes");
  }
}
