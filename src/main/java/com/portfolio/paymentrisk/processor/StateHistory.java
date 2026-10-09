package com.portfolio.paymentrisk.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.portfolio.paymentrisk.Json;
import java.io.UncheckedIOException;
import java.util.*;
import org.apache.flink.api.common.state.MapState;

/** Streams persisted JSON arrays without collecting the customer's history on the heap. */
final class StateHistory {
  private StateHistory() {}

  static Iterable<JsonNode> rows(String json) {
    return () -> {
      try {
        return Json.MAPPER.readerFor(JsonNode.class).readValues(json);
      } catch (java.io.IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

  static Iterable<JsonNode> read(
      MapState<Long, String> history, long currentTime, List<JsonNode> current) {
    return () -> {
      try {
        var buckets = history.entries().iterator();
        return new Iterator<JsonNode>() {
          private MappingIterator<JsonNode> rows;
          private Iterator<JsonNode> tail;

          public boolean hasNext() {
            try {
              while (rows == null || !rows.hasNextValue()) {
                if (rows != null) {
                  rows.close();
                  rows = null;
                }
                if (!buckets.hasNext()) {
                  if (tail == null) tail = current.iterator();
                  return tail.hasNext();
                }
                var entry = buckets.next();
                if (entry.getKey() == currentTime) continue;
                rows = Json.MAPPER.readerFor(JsonNode.class).readValues(entry.getValue());
              }
              return true;
            } catch (java.io.IOException e) {
              throw new UncheckedIOException(e);
            }
          }

          public JsonNode next() {
            if (!hasNext()) throw new NoSuchElementException();
            try {
              return rows == null ? tail.next() : rows.nextValue();
            } catch (java.io.IOException e) {
              throw new UncheckedIOException(e);
            }
          }
        };
      } catch (Exception e) {
        throw new IllegalStateException("Cannot read customer state", e);
      }
    };
  }
}
