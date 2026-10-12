package com.portfolio.paymentrisk;

import static org.junit.jupiter.api.Assertions.*;

import com.portfolio.paymentrisk.domain.Audit;
import com.portfolio.paymentrisk.tools.ProcessingOutcomeRows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class ProcessingOutcomeRowsTest {
  @Test
  void invalidInputPreservesOriginalCoordinatesAndHash() {
    var value =
        Json.object()
            .put("source_topic", "payments")
            .put("source_partition", 2)
            .put("source_offset", 17)
            .put("payload_sha256", Audit.sha256("bad bytes"))
            .put("error_code", "DESERIALIZATION_ERROR")
            .put("observed_at", 123);
    var row =
        ProcessingOutcomeRows.convert(
            value, new ConsumerRecord<>("dlq", 0, 9, "", new byte[] {1}), true);
    assertEquals("payments", row.path("input_topic").asText());
    assertEquals(17, row.path("input_offset").asLong());
    assertEquals(9, row.path("source_offset").asLong());
    assertEquals(value.path("payload_sha256"), row.path("payload_sha256"));
    value.put("payload_sha256", "");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ProcessingOutcomeRows.convert(
                value, new ConsumerRecord<>("dlq", 0, 9, "", new byte[] {1}), true));
  }

  @Test
  void lateEvidenceUsesBusinessHashAndChecksCutoff() {
    var tx =
        Json.object()
            .put("event_id", "e1")
            .put("customer_id", "c1")
            .put("event_time", 100)
            .put("producer_time", 1);
    var value =
        Json.object()
            .put("transaction", Json.write(tx))
            .put("customer_id", "c1")
            .put("watermark", 110)
            .put("lateness_ms", 10)
            .put("reason", "EVENT_TIME_CUTOFF")
            .put("observed_at", 123);
    var delivery = new ConsumerRecord<String, byte[]>("late", 1, 20, "c1", new byte[] {1});
    var row = ProcessingOutcomeRows.convert(value, delivery, false);
    assertEquals(Audit.inputHash(tx), row.path("input_sha256").asText());
    assertEquals(-1, row.path("input_offset").asLong());
    value.put("lateness_ms", 9);
    assertThrows(
        IllegalArgumentException.class,
        () -> ProcessingOutcomeRows.convert(value, delivery, false));
  }
}
