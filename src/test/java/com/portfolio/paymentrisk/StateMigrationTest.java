package com.portfolio.paymentrisk;

import static org.junit.jupiter.api.Assertions.*;

import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.domain.*;
import com.portfolio.paymentrisk.processor.CustomerRiskProcessor;
import java.util.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

class StateMigrationTest {
  private KeyedBroadcastOperatorTestHarness<String, String, String, String> harness(
      KeyedBroadcastProcessFunction<String, String, String, String> function) throws Exception {
    return new KeyedBroadcastOperatorTestHarness<>(
        new CoBroadcastWithKeyedOperator<>(function, List.of(CustomerRiskProcessor.RULES)),
        v -> Json.read(v).path("customer_id").asText(),
        Types.STRING,
        128,
        1,
        0);
  }

  /** Write the persisted v1 contract, without any of the newly introduced state descriptors. */
  static class LegacyState extends KeyedBroadcastProcessFunction<String, String, String, String> {
    public void processBroadcastElement(String value, Context ctx, Collector<String> out) {}

    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out)
        throws Exception {
      var runtime = getRuntimeContext();
      var history =
          runtime.getMapState(
              new MapStateDescriptor<>("history-json-v1", Types.LONG, Types.STRING));
      var pending =
          runtime.getMapState(
              new MapStateDescriptor<>("pending-json-v1", Types.LONG, Types.STRING));
      var devices =
          runtime.getMapState(new MapStateDescriptor<>("devices-v1", Types.STRING, Types.LONG));
      var tx = Json.read(value);
      history.put(100_000L, Json.write(List.of(tx)));
      devices.put("d", 100_000L);
      var queued = Json.object();
      queued.set("transaction", RiskEngineTest.tx("pending-v1", 100_001, 100, "d", "APPROVED"));
      queued.set("rules", Json.MAPPER.valueToTree(Rules.defaults()));
      queued.put("queued_at", 0);
      pending.put(100_001L, Json.write(List.of(queued)));
      for (String name : List.of("history-count-v1", "device-count-v1", "pending-count-v1"))
        runtime.getState(new ValueStateDescriptor<>(name, Types.INT)).update(1);
      runtime
          .getState(new ValueStateDescriptor<>("finalized-through-v1", Types.LONG))
          .update(100_000L);
      runtime.getState(new ValueStateDescriptor<>("cleanup-at-v1", Types.LONG)).update(3_700_000L);
      ctx.timerService().registerEventTimeTimer(100_001);
      ctx.timerService().registerEventTimeTimer(3_700_000);
    }
  }

  @Test
  void restoresLegacyHistoryDevicesAndPendingBucketThenExpiresAllState() throws Exception {
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState snapshot;
    try (var old = harness(new LegacyState())) {
      old.open();
      old.processElement(
          Json.write(RiskEngineTest.tx("history-v1", 100_000, 100, "d", "APPROVED")), 100_000);
      snapshot = old.snapshot(1, 1);
    }
    try (var restored = harness(new CustomerRiskProcessor(AppConfig.from(Map.of())))) {
      restored.initializeState(snapshot);
      restored.open();
      restored.getTwoInputOperator().processWatermarkStatus2(WatermarkStatus.IDLE);
      // A new queue entry and a legacy bucket share one timestamp after restore.
      restored.processElement(
          Json.write(RiskEngineTest.tx("z-pending-v2", 100_001, 100, "d", "APPROVED")), 100_001);
      restored.processWatermark(100_001);
      var results = restored.extractOutputValues();
      assertEquals(2, results.size());
      assertEquals("pending-v1", Json.read(results.get(0)).path("event_id").asText());
      assertEquals(
          2, Json.read(results.get(0)).path("rule_evidence").get(0).path("observed").asInt());
      assertEquals(
          3, Json.read(results.get(1)).path("rule_evidence").get(0).path("observed").asInt());
      assertFalse(
          Json.read(results.get(1)).path("rule_evidence").get(3).path("eligible").asBoolean());
      restored.processWatermark(3_000_000_000L);
      assertEquals(0, restored.numEventTimeTimers());
      var backend =
          (org.apache.flink.runtime.state.heap.HeapKeyedStateBackend<?>)
              restored.getOperator().getKeyedStateBackend();
      assertEquals(0, backend.numKeyValueStateEntries());
    }
  }
}
