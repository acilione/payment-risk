package com.portfolio.paymentrisk;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.domain.RiskEngine;
import com.portfolio.paymentrisk.domain.Rules;
import com.portfolio.paymentrisk.processor.CustomerRiskProcessor;
import java.lang.management.ManagementFactory;
import java.nio.file.*;
import java.util.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Opt-in real RocksDB/operator load test. Excludes Kafka, network and checkpoint commit latency.
 */
class StateLoadTest {
  record Profile(String name, int events, int keys, boolean sameTimestamp, double p99BudgetMs) {}

  private KeyedBroadcastOperatorTestHarness<String, String, String, String> harness()
      throws Exception {
    var op =
        new CoBroadcastWithKeyedOperator<String, String, String, String>(
            new CustomerRiskProcessor(AppConfig.from(Map.of())),
            List.of(CustomerRiskProcessor.RULES));
    var h =
        new KeyedBroadcastOperatorTestHarness<String, String, String, String>(
            op, s -> Json.read(s).path("customer_id").asText(), Types.STRING, 128, 1, 0);
    h.setStateBackend(new EmbeddedRocksDBStateBackend(true));
    return h;
  }

  @Test
  @EnabledIfSystemProperty(named = "risk.load", matches = "true")
  void measureStateDensityAndRecovery() throws Exception {
    var profiles =
        List.of(
            new Profile("typical", 5000, 1000, false, 20),
            new Profile("active", 1000, 1, false, 20),
            new Profile("hot-key", 5000, 1, false, 50),
            new Profile("high-cardinality", 5000, 5000, false, 20),
            new Profile("same-timestamp", 1000, 1, true, 50));
    var results = Json.MAPPER.createArrayNode();
    // Separate warmup keys and state, never included in a measured profile.
    run(new Profile("warmup", 300, 300, false, 20));
    for (var profile : profiles) results.add(run(profile));
    var report =
        Json.object()
            .put(
                "scope",
                "single-subtask RocksDB operator; admission plus synchronous watermark finalization; no Kafka/HTTP or committed-output latency")
            .put("java", System.getProperty("java.version"))
            .put("os", System.getProperty("os.name"))
            .put("cpus", Runtime.getRuntime().availableProcessors())
            .put("max_heap_bytes", Runtime.getRuntime().maxMemory())
            .put("history_ms", 3600000)
            .put("parallelism", 1)
            .put("input_spacing_ms", 100)
            .put("typical_target_decisions_per_second", 1000)
            .put(
                "source_commit",
                new String(
                        new ProcessBuilder("git", "rev-parse", "HEAD")
                            .start()
                            .getInputStream()
                            .readAllBytes())
                    .trim())
            .put(
                "working_tree_dirty",
                !new String(
                        new ProcessBuilder("git", "status", "--porcelain")
                            .start()
                            .getInputStream()
                            .readAllBytes())
                    .isBlank())
            .put(
                "qualification",
                "exploratory portfolio budgets declared in source before execution; not a production SLA");
    report.set("profiles", results);
    Files.createDirectories(Path.of("artifacts"));
    Files.writeString(
        Path.of("artifacts/state-load.json"),
        Json.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(report) + "\n");
  }

  private JsonNode run(Profile p) throws Exception {
    var histories = new HashMap<String, List<JsonNode>>();
    var devices = new HashMap<String, Long>();
    var expected = new HashMap<String, JsonNode>();
    var samples = new ArrayList<String>();
    long base = 1800000000000L;
    for (int i = 0; i < p.events; i++) {
      String customer = "c-" + (i % p.keys);
      long time = base + (p.sameTimestamp ? 0 : i * 100L);
      var tx =
          transaction(
              String.format("load-%08d", i),
              customer,
              time,
              1000,
              customer + "-d" + (i % 4),
              i % 7 == 0 ? "DECLINED" : "APPROVED");
      var history = histories.computeIfAbsent(customer, k -> new ArrayList<>());
      // Independent in-memory oracle is generated before the timed processing.
      var decision = RiskEngine.evaluate(tx, history, devices, Rules.defaults(), 30, 70, time);
      expected.put(tx.path("event_id").asText(), Json.read(Json.write(decision)));
      history.add(tx);
      devices.put(tx.path("device_id").asText(), time);
      samples.add(Json.write(tx));
    }
    long[] durations = new long[p.events];
    long maxFinalizationNanos = 0;
    long gcBefore = gcMillis(), started = System.nanoTime();
    long checkpointBytes, checkpointMs, restoreMs;
    int outputs;
    try (var h = harness()) {
      h.open();
      h.getTwoInputOperator().processWatermarkStatus2(WatermarkStatus.IDLE);
      started = System.nanoTime();
      for (int i = 0; i < samples.size(); i++) {
        long tick = System.nanoTime(), time = base + (p.sameTimestamp ? 0 : i * 100L);
        h.processElement(samples.get(i), time);
        if (!p.sameTimestamp || i == samples.size() - 1) {
          long finalizationStart = System.nanoTime();
          h.processWatermark(time);
          maxFinalizationNanos =
              Math.max(maxFinalizationNanos, System.nanoTime() - finalizationStart);
        }
        durations[i] = System.nanoTime() - tick;
      }
      long elapsed = System.nanoTime() - started;
      var values = h.extractOutputValues();
      outputs = values.size();
      assertEquals(p.events, outputs);
      var seen = new HashSet<String>();
      for (var value : values) {
        var actual = Json.read(value);
        String id = actual.path("event_id").asText();
        assertTrue(seen.add(id));
        var oracle = expected.get(id);
        assertNotNull(oracle);
        assertEquals(oracle.path("rule_evidence"), actual.path("rule_evidence"));
        assertEquals(oracle.path("risk_score"), actual.path("risk_score"));
        assertEquals(oracle.path("decision"), actual.path("decision"));
      }
      long checkpointStart = System.nanoTime();
      var snapshot = h.snapshot(1, base);
      checkpointMs = (System.nanoTime() - checkpointStart) / 1000000;
      checkpointBytes = snapshot.getStateSize();
      try (var restored = harness()) {
        long restoreStart = System.nanoTime();
        restored.initializeState(snapshot);
        restored.open();
        restored.getTwoInputOperator().processWatermarkStatus2(WatermarkStatus.IDLE);
        restoreMs = (System.nanoTime() - restoreStart) / 1000000;
        var next =
            transaction(
                "after-restore", "c-0", base + p.events * 100L + 1, 1000, "c-0-d0", "APPROVED");
        var oracle =
            RiskEngine.evaluate(
                next, histories.get("c-0"), devices, Rules.defaults(), 30, 70, base);
        restored.processElement(Json.write(next), next.path("event_time").asLong());
        restored.processWatermark(next.path("event_time").asLong());
        assertEquals(
            Json.read(Json.write(oracle)).path("rule_evidence"),
            Json.read(restored.extractOutputValues().get(0)).path("rule_evidence"));
      } finally {
        snapshot.discardState();
      }
      long max = Arrays.stream(durations).max().orElseThrow();
      Arrays.sort(durations);
      double p99 = durations[(int) Math.ceil(durations.length * .99) - 1] / 1e6;
      return Json.object()
          .put("profile", p.name)
          .put("events", p.events)
          .put("keys", p.keys)
          .put("events_per_key", p.events / p.keys)
          .put("decisions", outputs)
          .put("elapsed_ms", elapsed / 1e6)
          .put("decisions_per_second", p.events * 1e9 / elapsed)
          .put("operation_p50_ms", durations[durations.length / 2] / 1e6)
          .put("operation_p95_ms", durations[(int) (durations.length * .95)] / 1e6)
          .put("operation_p99_ms", p99)
          .put("max_operation_ms", max / 1e6)
          .put("max_watermark_finalization_ms", maxFinalizationNanos / 1e6)
          .put("p99_budget_ms", p.p99BudgetMs)
          .put(
              "within_budget",
              p99 <= p.p99BudgetMs
                  && (!p.sameTimestamp || maxFinalizationNanos / 1e6 <= p.p99BudgetMs)
                  && (!p.name.equals("typical") || p.events * 1e9 / elapsed >= 1000))
          .put("checkpoint_bytes", checkpointBytes)
          .put("checkpoint_ms", checkpointMs)
          .put("restore_ms", restoreMs)
          .put("gc_ms_including_reconciliation_restore", gcMillis() - gcBefore)
          .put(
              "correctness",
              "exact IDs, scores, all rule evidence and first post-restore decision match oracle");
    }
  }

  private com.fasterxml.jackson.databind.node.ObjectNode transaction(
      String id, String customer, long time, long amount, String device, String status) {
    return RiskEngineTest.tx(id, time, amount, device, status).put("customer_id", customer);
  }

  private long gcMillis() {
    return ManagementFactory.getGarbageCollectorMXBeans().stream()
        .mapToLong(b -> Math.max(0, b.getCollectionTime()))
        .sum();
  }
}
