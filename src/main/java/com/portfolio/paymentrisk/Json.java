package com.portfolio.paymentrisk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public final class Json {
  public static final ObjectMapper MAPPER = new ObjectMapper();

  private Json() {}

  public static JsonNode read(String value) {
    try {
      return MAPPER.readTree(value);
    } catch (java.io.IOException e) {
      throw new IllegalArgumentException("Invalid JSON", e);
    }
  }

  public static String write(Object value) {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (java.io.IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public static ObjectNode object() {
    return MAPPER.createObjectNode();
  }
}
