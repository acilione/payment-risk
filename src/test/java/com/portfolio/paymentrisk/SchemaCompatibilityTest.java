package com.portfolio.paymentrisk;

import static org.junit.jupiter.api.Assertions.*;

import com.portfolio.paymentrisk.serialization.AvroCodec;
import java.nio.file.*;
import org.apache.avro.*;
import org.junit.jupiter.api.Test;

class SchemaCompatibilityTest {
  @Test
  void allHistoricalWritersRemainReadable() throws Exception {
    try (var files = Files.walk(Path.of("schemas/history"))) {
      for (var file : files.filter(p -> p.toString().endsWith(".avsc")).toList()) {
        var writer = new Schema.Parser().parse(file.toFile());
        var reader =
            new Schema.Parser().parse(Path.of("schemas", file.getFileName().toString()).toFile());
        assertEquals(
            SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE,
            SchemaCompatibility.checkReaderWriterCompatibility(reader, writer).getType(),
            file.toString());
      }
    }
  }

  @Test
  void avroWireRoundtripAndUnknownEnumRejection() throws Exception {
    var s = AvroCodec.schema("transaction");
    var tx = RiskEngineTest.tx("e", 1_000_000, 123, "d", "APPROVED");
    var bytes = AvroCodec.encode(s, 42, tx);
    assertEquals(Json.read(Json.write(tx)), Json.read(AvroCodec.decode(bytes, s, s)));
    tx.put("status", "UNKNOWN");
    assertThrows(AvroRuntimeException.class, () -> AvroCodec.encode(s, 42, tx));
  }
}
