package com.portfolio.paymentrisk.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.portfolio.paymentrisk.Json;
import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;

/** Confluent-compatible wire framing; writer schema lookup and explicit reader resolution. */
public final class AvroCodec {
  private static final Map<String, Schema> READERS = new java.util.concurrent.ConcurrentHashMap<>();
  private final String registry;
  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
  private final Map<Integer, Schema> writers =
      new LinkedHashMap<>() {
        protected boolean removeEldestEntry(Map.Entry<Integer, Schema> e) {
          return size() > 256;
        }
      };
  private final Map<String, Integer> ids = new HashMap<>();

  public AvroCodec(String registry) {
    this.registry = registry;
  }

  public static Schema schema(String name) {
    return READERS.computeIfAbsent(name, AvroCodec::loadSchema);
  }

  private static Schema loadSchema(String name) {
    try (var in = AvroCodec.class.getResourceAsStream("/schemas/" + name + ".avsc")) {
      if (in == null) throw new IllegalArgumentException("Unknown schema " + name);
      return new Schema.Parser().parse(in);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private JsonNode request(String path, String body) throws IOException {
    var b = HttpRequest.newBuilder(URI.create(registry + path)).timeout(Duration.ofSeconds(15));
    String token = System.getenv("REGISTRY_BEARER_TOKEN");
    if (token != null) b.header("Authorization", "Bearer " + token);
    if (body != null)
      b.header("Content-Type", "application/vnd.schemaregistry.v1+json")
          .POST(HttpRequest.BodyPublishers.ofString(body));
    try {
      var response = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() == 404)
        throw new IllegalArgumentException("SCHEMA_ERROR: unknown registry schema");
      if (response.statusCode() / 100 != 2)
        throw new IOException("Registry HTTP " + response.statusCode());
      return Json.read(response.body());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
  }

  public int register(String topic, String name) throws IOException {
    return request(
            "/subjects/" + topic + "-value/versions",
            Json.write(Map.of("schemaType", "AVRO", "schema", schema(name).toString())))
        .path("id")
        .asInt();
  }

  public byte[] encode(String topic, String name, String json) throws IOException {
    if (!ids.containsKey(topic)) {
      ids.put(
          topic,
          request(
                  "/subjects/" + topic + "-value",
                  Json.write(Map.of("schemaType", "AVRO", "schema", schema(name).toString())))
              .path("id")
              .asInt());
    }
    return encode(schema(name), ids.get(topic), Json.read(json));
  }

  public static byte[] encode(Schema schema, int id, JsonNode json) throws IOException {
    GenericRecord record = new GenericData.Record(schema);
    for (var f : schema.getFields())
      record.put(
          f.name(),
          json.get(f.name()) == null && f.hasDefaultValue()
              ? GenericData.get().getDefaultValue(f)
              : datum(f.schema(), json.get(f.name())));
    var out = new ByteArrayOutputStream();
    out.write(ByteBuffer.allocate(5).put((byte) 0).putInt(id).array());
    var encoder = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
    encoder.flush();
    return out.toByteArray();
  }

  private static Object datum(Schema s, JsonNode n) {
    if (n == null || n.isNull()) return null;
    return switch (s.getType()) {
      case STRING -> n.asText();
      case LONG -> n.longValue();
      case INT -> n.intValue();
      case BOOLEAN -> n.booleanValue();
      case ENUM -> new GenericData.EnumSymbol(s, n.asText());
      case ARRAY -> {
        var a = new ArrayList<Object>();
        n.forEach(x -> a.add(datum(s.getElementType(), x)));
        yield a;
      }
      case UNION ->
          datum(
              s.getTypes().stream()
                  .filter(x -> x.getType() != Schema.Type.NULL)
                  .findFirst()
                  .orElseThrow(),
              n);
      default -> throw new IllegalArgumentException("Unsupported schema field " + s.getType());
    };
  }

  public String decode(byte[] bytes, String name) throws IOException {
    if (bytes == null || bytes.length < 5 || bytes[0] != 0)
      throw new IllegalArgumentException("DESERIALIZATION_ERROR: invalid framing");
    int id = ByteBuffer.wrap(bytes, 1, 4).getInt();
    if (!writers.containsKey(id))
      writers.put(
          id,
          new Schema.Parser().parse(request("/schemas/ids/" + id, null).path("schema").asText()));
    try {
      return decode(bytes, writers.get(id), schema(name));
    } catch (IOException e) {
      throw new IllegalArgumentException("DESERIALIZATION_ERROR: invalid Avro body", e);
    }
  }

  public static String decode(byte[] bytes, Schema writer, Schema reader) throws IOException {
    var decoder = DecoderFactory.get().binaryDecoder(bytes, 5, bytes.length - 5, null);
    var result = new GenericDatumReader<GenericRecord>(writer, reader).read(null, decoder);
    if (!decoder.isEnd())
      throw new IllegalArgumentException("DESERIALIZATION_ERROR: trailing bytes");
    return result.toString();
  }
}
