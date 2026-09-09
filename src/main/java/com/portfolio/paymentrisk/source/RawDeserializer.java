package com.portfolio.paymentrisk.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class RawDeserializer implements KafkaRecordDeserializationSchema<RawRecord> {
  private final com.portfolio.paymentrisk.config.AppConfig config;
  private final boolean rules;
  private transient com.portfolio.paymentrisk.serialization.AvroCodec codec;

  public RawDeserializer(com.portfolio.paymentrisk.config.AppConfig config, boolean rules) {
    this.config = config;
    this.rules = rules;
  }

  public void deserialize(ConsumerRecord<byte[], byte[]> r, Collector<RawRecord> out)
      throws java.io.IOException {
    if (codec == null)
      codec = new com.portfolio.paymentrisk.serialization.AvroCodec(config.registry());
    var record = new RawRecord(r.topic(), r.partition(), r.offset(), r.timestamp(), r.value());
    try {
      record.decoded =
          com.portfolio.paymentrisk.processor.Validate.inspect(record, codec, config, rules);
      if (!rules)
        record.eventTime =
            com.portfolio.paymentrisk.Json.read(record.decoded).path("event_time").asLong();
    } catch (IllegalArgumentException | org.apache.avro.AvroRuntimeException e) {
      record.validationError = com.portfolio.paymentrisk.processor.Validate.errorCode(e);
    }
    out.collect(record);
  }

  public TypeInformation<RawRecord> getProducedType() {
    return TypeInformation.of(RawRecord.class);
  }
}
