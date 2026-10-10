package com.portfolio.paymentrisk;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.processor.CustomerRiskProcessor;
import com.portfolio.paymentrisk.processor.Validate;
import com.portfolio.paymentrisk.tools.generation.*;
import java.nio.file.*;
import java.util.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
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
        GeneratorConfig.PROFILES.size(),
        first.customers().stream().map(c -> c.profile()).distinct().count(),
        "Showcase defaults should include every behavior");
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
  void customerHabitsAndAuthorizationRetriesRemainConsistent() throws Exception {
    for (int history : List.of(5, 1440)) {
      for (String profile : GeneratorConfig.PROFILES) {
        var raw = config().put("history_minutes", history).put("customers", 3);
        var weights = (ObjectNode) raw.path("profiles");
        for (String name : GeneratorConfig.PROFILES)
          weights.put(name, name.equals(profile) ? 100 : 0);
        var generated =
            new GeneratedPayments(GeneratorConfig.parse(raw), "show-habits", 1800000000000L);
        var customers = new HashMap<String, List<ObjectNode>>();
        long previous = 0;
        for (var item : generated) {
          var tx = item.value();
          Validate.transaction(tx, 1800000000000L);
          assertTrue(tx.path("event_time").asLong() >= previous);
          previous = tx.path("event_time").asLong();
          customers
              .computeIfAbsent(tx.path("customer_id").asText(), ignored -> new ArrayList<>())
              .add(tx);
        }
        for (var payments : customers.values()) {
          assertEquals(
              1, payments.stream().map(p -> p.path("country").asText()).distinct().count());
          long devices =
              payments.stream().map(p -> p.path("device_id").asText()).distinct().count();
          assertTrue(devices <= 2, "Profiles do not imply a different device for every payment");
          var last = payments.get(payments.size() - 1);
          if (profile.equals("known_devices")) assertEquals(2, devices);
          if (profile.equals("takeover")) {
            var attempts = payments.subList(payments.size() - 3, payments.size());
            assertEquals(1, attempts.stream().map(p -> p.path("device_id")).distinct().count());
            assertNotEquals(payments.get(0).path("device_id"), last.path("device_id"));
            assertTrue(attempts.stream().allMatch(p -> p.path("amount_minor").asLong() >= 80000));
          }
          if (profile.equals("checkout_retry")) {
            var declined = payments.get(payments.size() - 2);
            for (String field : List.of("merchant_id", "amount_minor", "device_id"))
              assertEquals(declined.path(field), last.path(field));
            assertEquals("DECLINED", declined.path("status").asText());
            assertEquals("APPROVED", last.path("status").asText());
            assertNotEquals(declined.path("transaction_id"), last.path("transaction_id"));
            assertTrue(last.path("event_time").asLong() > declined.path("event_time").asLong());
          }
        }
      }
    }
  }

  @Test
  void generatedCasesExerciseActualRulesWithoutTreatingTwoDevicesAsFraud() throws Exception {
    var generated =
        new GeneratedPayments(GeneratorConfig.parse(config()), "show-rules", 1800000000000L);
    var profiles = new HashMap<String, String>();
    generated.customers().forEach(c -> profiles.put(c.id(), c.profile()));
    var operator =
        new CoBroadcastWithKeyedOperator<String, String, String, String>(
            new CustomerRiskProcessor(AppConfig.from(Map.of())),
            List.of(CustomerRiskProcessor.RULES));
    try (var harness =
        new KeyedBroadcastOperatorTestHarness<String, String, String, String>(
            operator, s -> Json.read(s).path("customer_id").asText(), Types.STRING, 128, 1, 0)) {
      harness.open();
      harness.getTwoInputOperator().processWatermarkStatus2(WatermarkStatus.IDLE);
      var targets = new HashSet<String>();
      long previous = -1;
      for (var item : generated) {
        long time = item.value().path("event_time").asLong();
        if (previous >= 0 && time != previous) harness.processWatermark(previous);
        harness.processElement(Json.write(item.value()), time);
        if (item.target()) targets.add(item.value().path("event_id").asText());
        previous = time;
      }
      harness.processWatermark(1800000000000L);
      List<JsonNode> decisions = harness.extractOutputValues().stream().map(Json::read).toList();
      assertEquals(generated.size(), decisions.size());
      for (var decision : decisions) {
        String profile = profiles.get(decision.path("customer_id").asText());
        assertFalse(decision.path("matched_rules").toString().contains("R003"));
        if (Set.of("normal", "known_devices", "checkout_retry", "trusted_device").contains(profile))
          assertEquals("APPROVE", decision.path("decision").asText(), profile);
        if (targets.contains(decision.path("event_id").asText())) {
          if (profile.equals("new_device"))
            assertEquals("REVIEW", decision.path("decision").asText());
          if (profile.equals("card_testing")) assertTrue(decision.path("risk_score").asInt() >= 40);
        }
      }
      assertTrue(
          harness.getSideOutput(CustomerRiskProcessor.LATE) == null
              || harness.getSideOutput(CustomerRiskProcessor.LATE).isEmpty());
    }
  }

  @Test
  void oldConfigurationsCanOmitNewProfiles() throws Exception {
    var raw = config();
    var weights = (ObjectNode) raw.path("profiles");
    weights.remove("known_devices");
    weights.remove("checkout_retry");
    weights.put("normal", 65);
    assertEquals(0, GeneratorConfig.parse(raw).profiles().get("known_devices"));
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
