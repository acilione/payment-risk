package com.portfolio.paymentrisk.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.portfolio.paymentrisk.Json;
import com.portfolio.paymentrisk.domain.Audit;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Durable side-output evidence. Invalid inputs use source coordinates; late inputs use business
 * identity.
 */
public final class ProcessingOutcomeRows {
  private ProcessingOutcomeRows() {}

  public static ObjectNode convert(
      JsonNode value, ConsumerRecord<String, byte[]> delivery, boolean invalid) {
    var row =
        Json.object()
            .put("source_topic", delivery.topic())
            .put("source_partition", delivery.partition())
            .put("source_offset", delivery.offset())
            .put("outcome", invalid ? "INVALID" : "LATE")
            .put("observed_at", value.path("observed_at").asLong());
    if (row.path("observed_at").asLong() <= 0)
      throw new IllegalArgumentException("INVALID_OUTCOME");
    if (invalid) {
      String topic = value.path("source_topic").asText();
      long partition = value.path("source_partition").asLong(-1),
          offset = value.path("source_offset").asLong(-1);
      String hash = value.path("payload_sha256").asText();
      if (topic.isBlank()
          || partition < 0
          || offset < 0
          || !hash.matches("[0-9a-f]{64}")
          || value.path("error_code").asText().isBlank())
        throw new IllegalArgumentException("INVALID_OUTCOME");
      row.put("input_topic", topic)
          .put("input_partition", partition)
          .put("input_offset", offset)
          .put("payload_sha256", hash)
          .put("event_id", "")
          .put("input_sha256", "")
          .put("reason", value.path("error_code").asText())
          .put("watermark", 0)
          .put("lateness_ms", 0);
    } else {
      var transaction = Json.read(value.path("transaction").asText());
      String event = transaction.path("event_id").asText();
      if (event.isBlank()
          || !transaction.path("customer_id").asText().equals(value.path("customer_id").asText())
          || value.path("watermark").asLong() < transaction.path("event_time").asLong()
          || value.path("lateness_ms").asLong(-1)
              != value.path("watermark").asLong() - transaction.path("event_time").asLong())
        throw new IllegalArgumentException("INVALID_OUTCOME");
      row.put("input_topic", "")
          .put("input_partition", -1)
          .put("input_offset", -1)
          .put("payload_sha256", "")
          .put("event_id", event)
          .put("input_sha256", Audit.inputHash(transaction))
          .put("reason", value.path("reason").asText())
          .put("watermark", value.path("watermark").asLong())
          .put("lateness_ms", value.path("lateness_ms").asLong());
    }
    return row;
  }
}
