package com.portfolio.paymentrisk;

import static org.junit.jupiter.api.Assertions.*;

import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.serialization.AvroCodec;
import com.portfolio.paymentrisk.source.*;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.util.*;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class SourceValidationTest {
  @Test
  void validationBeforeSourceWatermarksPreservesMetadataAndPropagatesOutage() throws Exception {
    var server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    var schema = AvroCodec.schema("transaction");
    server.createContext(
        "/schemas/ids/1",
        exchange -> {
          byte[] body =
              Json.write(Map.of("schema", schema.toString()))
                  .getBytes(java.nio.charset.StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    server.createContext(
        "/schemas/ids/2",
        exchange -> {
          exchange.sendResponseHeaders(503, -1);
          exchange.close();
        });
    server.start();
    try {
      var c =
          AppConfig.from(
              Map.of("SCHEMA_REGISTRY_URL", "http://localhost:" + server.getAddress().getPort()));
      var deserializer = new RawDeserializer(c, false);
      var output = new ArrayList<RawRecord>();
      Collector<RawRecord> collector =
          new Collector<>() {
            public void collect(RawRecord r) {
              output.add(r);
            }

            public void close() {}
          };
      var tx = RiskEngineTest.tx("e", System.currentTimeMillis(), 100, "d", "APPROVED");
      deserializer.deserialize(
          new ConsumerRecord<>("payments.raw", 2, 42, null, AvroCodec.encode(schema, 1, tx)),
          collector);
      assertEquals(tx.path("event_time").asLong(), output.get(0).eventTime);
      assertEquals(42, output.get(0).offset);
      assertEquals(2, output.get(0).partition);
      deserializer.deserialize(
          new ConsumerRecord<>("payments.raw", 2, 43, null, new byte[] {99}), collector);
      assertEquals(Long.MIN_VALUE, output.get(1).eventTime);
      assertEquals("DESERIALIZATION_ERROR", output.get(1).validationError);
      tx.put("event_time", System.currentTimeMillis() + 600_000);
      deserializer.deserialize(
          new ConsumerRecord<>("payments.raw", 2, 44, null, AvroCodec.encode(schema, 1, tx)),
          collector);
      assertEquals("INVALID_EVENT_TIME", output.get(2).validationError);
      assertEquals(Long.MIN_VALUE, output.get(2).eventTime);
      assertThrows(
          java.io.IOException.class,
          () ->
              deserializer.deserialize(
                  new ConsumerRecord<>(
                      "payments.raw", 2, 45, null, AvroCodec.encode(schema, 2, tx)),
                  collector));
      assertEquals(3, output.size(), "Infrastructure outage cannot become DLQ business data");
    } finally {
      server.stop(0);
    }
  }
}
