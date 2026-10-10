package com.portfolio.paymentrisk;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.portfolio.paymentrisk.tools.generation.*;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class GeneratedPaymentsTest {
  private ObjectNode config() throws Exception {
    return (ObjectNode) Json.read(Files.readString(Path.of("config/showcase.json")));
  }

  @Test
  void seedReproducesHistoriesAndRetryChoicesInChronologicalOrder() throws Exception {
    var config = GeneratorConfig.parse(config());
    var first = new GeneratedPayments(config, "show-test", 1800000000000L);
    assertEquals(
        6,
        first.customers().stream().map(c -> c.profile()).distinct().count(),
        "Portfolio defaults should include every behavior");
    var second = new GeneratedPayments(config, "show-test", 1800000000000L).iterator();
    var ids = new HashSet<String>();
    var targets = new HashSet<String>();
    long previous = 0;
    int retries = 0;
    for (var item : first) {
      assertEquals(item, second.next());
      assertTrue(item.value().path("event_time").asLong() >= previous);
      previous = item.value().path("event_time").asLong();
      assertTrue(ids.add(item.value().path("event_id").asText()));
      assertFalse(item.value().has("label"));
      assertFalse(item.value().has("risk_score"));
      if (item.target()) assertTrue(targets.add(item.value().path("customer_id").asText()));
      if (item.retry()) retries++;
    }
    assertFalse(second.hasNext());
    assertEquals(first.size(), ids.size());
    assertEquals(config.customers(), targets.size());
    assertTrue(retries > 0);
    assertNotEquals(
        first.customers(),
        new GeneratedPayments(
                GeneratorConfig.parse(config().put("seed", 43)), "show-test", 1800000000000L)
            .customers());
  }

  @Test
  void weightsCanSelectOneProfileAndRetriesCanBeDisabled() throws Exception {
    var raw = config().put("retry_percent", 0);
    var weights = (ObjectNode) raw.path("profiles");
    for (var profile : GeneratorConfig.PROFILES)
      weights.put(profile, profile.equals("new_device") ? 100 : 0);
    var generator =
        new GeneratedPayments(GeneratorConfig.parse(raw), "show-profile", 1800000000000L);
    assertTrue(generator.customers().stream().allMatch(c -> c.profile().equals("new_device")));
    for (var item : generator) {
      assertFalse(item.retry());
      if (item.target()) {
        assertTrue(item.value().path("device_id").asText().endsWith("-replacement"));
        assertTrue(item.value().path("amount_minor").asLong() >= 80000);
      }
    }
  }

  @Test
  void invalidOrUnboundedConfigurationFailsBeforeGeneration() throws Exception {
    var invalid = config().put("customers", 2001);
    assertThrows(IllegalArgumentException.class, () -> GeneratorConfig.parse(invalid));
    var oversized = config().put("customers", 2000).put("max_payments", 100);
    assertThrows(IllegalArgumentException.class, () -> GeneratorConfig.parse(oversized));
    var weights = config();
    ((ObjectNode) weights.path("profiles")).put("normal", 54);
    assertThrows(IllegalArgumentException.class, () -> GeneratorConfig.parse(weights));
    var unknown = config().put("publish_rates", 20);
    assertThrows(IllegalArgumentException.class, () -> GeneratorConfig.parse(unknown));
    var range = config().put("ordinary_amount_max", 1);
    assertThrows(IllegalArgumentException.class, () -> GeneratorConfig.parse(range));
  }
}
