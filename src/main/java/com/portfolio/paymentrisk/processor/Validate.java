package com.portfolio.paymentrisk.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.portfolio.paymentrisk.*;
import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.domain.Rules;
import com.portfolio.paymentrisk.serialization.AvroCodec;
import com.portfolio.paymentrisk.source.RawRecord;
import java.util.*;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.*;

public class Validate extends ProcessFunction<RawRecord, String> {
  private static final Set<String> COUNTRIES = Set.of(Locale.getISOCountries());
  public static final OutputTag<String> DLQ =
      new OutputTag<>("dead-letters", org.apache.flink.api.common.typeinfo.Types.STRING);
  private final AppConfig config;
  private final boolean rules;
  private transient AvroCodec codec;
  private transient Counter valid, invalid;

  public Validate(AppConfig config, boolean rules) {
    this.config = config;
    this.rules = rules;
  }

  public void open(OpenContext c) {
    codec = new AvroCodec(config.registry());
    valid =
        getRuntimeContext()
            .getMetricGroup()
            .counter(rules ? "risk_rules_valid_total" : "risk_transactions_valid_total");
    invalid =
        getRuntimeContext()
            .getMetricGroup()
            .counter(
                rules ? "risk_rule_updates_rejected_total" : "risk_transactions_invalid_total");
  }

  public static void transaction(JsonNode t, long observed) {
    for (String f :
        List.of("event_id", "transaction_id", "customer_id", "merchant_id", "device_id"))
      if (t.path(f).asText().isBlank() || t.path(f).asText().length() > 128)
        throw new IllegalArgumentException("MISSING_REQUIRED_FIELD: " + f);
    if (t.path("amount_minor").asLong() <= 0
        || t.path("amount_minor").asLong() > 1_000_000_000_000L)
      throw new IllegalArgumentException("INVALID_AMOUNT");
    if (!t.path("currency").asText().equals("EUR"))
      throw new IllegalArgumentException("INVALID_CURRENCY: EUR-only deployment");
    if (!COUNTRIES.contains(t.path("country").asText()))
      throw new IllegalArgumentException("INVALID_COUNTRY");
    if (!Set.of("APPROVED", "DECLINED").contains(t.path("status").asText()))
      throw new IllegalArgumentException("INVALID_STATUS");
    if (t.path("event_time").asLong() <= 0 || t.path("event_time").asLong() > observed + 300_000)
      throw new IllegalArgumentException("INVALID_EVENT_TIME");
  }

  public void processElement(RawRecord r, Context ctx, Collector<String> out) throws Exception {
    String decoded;
    try {
      if (r.validationError != null) throw new IllegalArgumentException(r.validationError);
      decoded = r.decoded != null ? r.decoded : inspect(r, codec, config, rules);
    } catch (IllegalArgumentException
        | org.apache.avro.AvroRuntimeException
        | java.io.EOFException e) {
      invalid.inc();
      var error =
          Json.object()
              .put("error_id", r.topic + ":" + r.partition + ":" + r.offset)
              .put("source_topic", r.topic)
              .put("source_partition", r.partition)
              .put("source_offset", r.offset)
              .put("source_timestamp", r.timestamp)
              .put(
                  "schema_id",
                  r.payload != null && r.payload.length >= 5 && r.payload[0] == 0
                      ? java.nio.ByteBuffer.wrap(r.payload, 1, 4).getInt()
                      : -1)
              .put("observed_at", System.currentTimeMillis())
              .put("error_code", errorCode(e))
              .put("error_message", "Rejected " + (rules ? "rule" : "payment") + " record")
              .put(
                  "raw_payload",
                  Base64.getEncoder()
                      .encodeToString(
                          r.payload == null
                              ? new byte[0]
                              : Arrays.copyOf(r.payload, Math.min(4096, r.payload.length))))
              .put("payload_truncated", r.payload != null && r.payload.length > 4096);
      ctx.output(DLQ, Json.write(error));
      return;
    }
    valid.inc();
    out.collect(decoded);
  }

  public static String inspect(RawRecord r, AvroCodec codec, AppConfig config, boolean rules)
      throws java.io.IOException {
    if (r.payload != null && r.payload.length > 1_048_576)
      throw new IllegalArgumentException("PAYLOAD_TOO_LARGE");
    String decoded = codec.decode(r.payload, rules ? "risk-rule" : "transaction");
    if (rules) Rules.validate(Json.read(decoded), config);
    else transaction(Json.read(decoded), System.currentTimeMillis());
    return decoded;
  }

  public static String errorCode(Exception e) {
    String code = e.getMessage() == null ? "" : e.getMessage().split(":")[0];
    return Set.of(
                "DESERIALIZATION_ERROR",
                "SCHEMA_ERROR",
                "PAYLOAD_TOO_LARGE",
                "MISSING_REQUIRED_FIELD",
                "INVALID_AMOUNT",
                "INVALID_CURRENCY",
                "INVALID_COUNTRY",
                "INVALID_STATUS",
                "INVALID_EVENT_TIME",
                "UNKNOWN_RULE_TYPE",
                "INVALID_RULE_CONFIGURATION")
            .contains(code)
        ? code
        : "DESERIALIZATION_ERROR";
  }
}
