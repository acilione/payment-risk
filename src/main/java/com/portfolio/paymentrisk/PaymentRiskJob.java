package com.portfolio.paymentrisk;

import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.processor.*;
import com.portfolio.paymentrisk.sink.AvroKafkaSerializer;
import com.portfolio.paymentrisk.source.*;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public final class PaymentRiskJob {
  private PaymentRiskJob() {}

  public static void main(String[] args) throws Exception {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    if (!env.getCheckpointConfig().isCheckpointingEnabled()) env.enableCheckpointing(30_000);
    var settings = new org.apache.flink.configuration.Configuration();
    settings.set(org.apache.flink.configuration.PipelineOptions.GENERIC_TYPES, false);
    env.configure(settings);
    env.setMaxParallelism(128);
    build(env, AppConfig.fromEnv());
    env.execute("payment-risk-v1");
  }

  public static void build(StreamExecutionEnvironment env, AppConfig c) {
    var payments =
        source(env, c, c.topic("payments.raw"), false)
            .process(new Validate(c, false))
            .uid("transaction-validation-v1")
            .name("validate-payments");
    var rules =
        source(env, c, c.topic("risk.rules"), true)
            .process(new Validate(c, true))
            .uid("rule-validation-v1")
            .name("validate-rules");
    // A control input has no business-time frontier. Keep it idle rather than emitting
    // MAX_VALUE, which would finalize everything when the payment input becomes idle.
    var ruleWatermarks =
        rules
            .assignTimestampsAndWatermarks(
                new WatermarkStrategy<String>() {
                  public WatermarkGenerator<String> createWatermarkGenerator(
                      WatermarkGeneratorSupplier.Context ctx) {
                    return new WatermarkGenerator<>() {
                      public void onEvent(String event, long timestamp, WatermarkOutput out) {
                        out.markIdle();
                      }

                      public void onPeriodicEmit(WatermarkOutput out) {
                        out.markIdle();
                      }
                    };
                  }
                })
            .uid("rule-watermarks-v1");
    var unique =
        payments
            .keyBy(s -> Json.read(s).path("event_id").asText())
            .process(new Deduplicate(c.dedupMs()))
            .uid("transaction-dedup-v1")
            .name("deduplicate");
    var decisions =
        unique
            .keyBy(s -> Json.read(s).path("customer_id").asText())
            .connect(ruleWatermarks.broadcast(CustomerRiskProcessor.RULES))
            .process(new CustomerRiskProcessor(c))
            .uid("customer-risk-state-v1")
            .name("customer-risk");
    sink(
        decisions,
        c,
        c.topic("risk.decisions"),
        "risk-decision",
        "customer_id",
        "risk-decision-sink-v1");
    sink(
        payments.getSideOutput(Validate.DLQ).union(rules.getSideOutput(Validate.DLQ)),
        c,
        c.topic("payments.dlq"),
        "dead-letter",
        "error_id",
        "dlq-sink-v1");
    sink(
        decisions.getSideOutput(CustomerRiskProcessor.LATE),
        c,
        c.topic("payments.late"),
        "late-event",
        "customer_id",
        "late-sink-v1");
  }

  private static DataStream<RawRecord> source(
      StreamExecutionEnvironment env, AppConfig c, String topic, boolean rules) {
    var p = c.kafkaProperties();
    p.setProperty("isolation.level", "read_committed");
    var source =
        KafkaSource.<RawRecord>builder()
            .setBootstrapServers(c.bootstrap())
            .setTopics(topic)
            .setGroupId(c.group() + (rules ? "-rules" : "-payments"))
            .setProperties(p)
            .setStartingOffsets(
                rules || c.offsets().equals("earliest")
                    ? OffsetsInitializer.earliest()
                    : OffsetsInitializer.committedOffsets(OffsetResetStrategy.NONE))
            .setDeserializer(new RawDeserializer(c, rules))
            .build();
    WatermarkStrategy<RawRecord> watermarks =
        rules
            ? WatermarkStrategy.noWatermarks()
            : WatermarkStrategy.<RawRecord>forBoundedOutOfOrderness(
                    Duration.ofMillis(c.disorderMs()))
                .withTimestampAssigner((event, t) -> event.eventTime)
                .withIdleness(Duration.ofMillis(c.idleMs()));
    return env.fromSource(source, watermarks, topic)
        .uid(rules ? "rule-source-v1" : "transaction-source-v1");
  }

  private static void sink(
      DataStream<String> stream, AppConfig c, String topic, String schema, String key, String uid) {
    var p = c.kafkaProperties();
    p.setProperty("transaction.timeout.ms", "900000");
    stream
        .sinkTo(
            KafkaSink.<String>builder()
                .setBootstrapServers(c.bootstrap())
                .setKafkaProducerConfig(p)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(c.prefix() + "-" + topic + "-")
                .setRecordSerializer(new AvroKafkaSerializer(c.registry(), topic, schema, key))
                .build())
        .uid(uid)
        .name(topic);
  }
}
