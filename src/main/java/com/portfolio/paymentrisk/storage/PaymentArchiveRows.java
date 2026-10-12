package com.portfolio.paymentrisk.storage;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.portfolio.paymentrisk.Json;
import com.portfolio.paymentrisk.domain.Audit;
import java.util.Base64;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/** Wire bytes are archived before decoding; registry and risk failures cannot discard an input. */
public final class PaymentArchiveRows {
  private PaymentArchiveRows() {}

  public static ObjectNode convert(ConsumerRecord<byte[], byte[]> record) {
    var row =
        Json.object()
            .put("source_topic", record.topic())
            .put("source_partition", record.partition())
            .put("source_offset", record.offset())
            .put("source_timestamp", record.timestamp())
            .put("timestamp_type", record.timestampType().name())
            .put("key_is_null", record.key() == null)
            .put("value_is_null", record.value() == null)
            .put("key_base64", encoded(record.key()))
            .put("payload_base64", encoded(record.value()))
            .put("payload_bytes", record.value() == null ? 0 : record.value().length)
            .put(
                "payload_sha256",
                Audit.sha256(record.value() == null ? new byte[0] : record.value()));
    var headers = Json.MAPPER.createArrayNode();
    record
        .headers()
        .forEach(
            header ->
                headers.add(
                    Json.object()
                        .put("key", header.key())
                        .put("value_is_null", header.value() == null)
                        .put("value_base64", encoded(header.value()))));
    row.put("headers_json", Json.write(headers));
    return row;
  }

  private static String encoded(byte[] bytes) {
    return bytes == null ? "" : Base64.getEncoder().encodeToString(bytes);
  }
}
