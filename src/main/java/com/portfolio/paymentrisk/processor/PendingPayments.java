package com.portfolio.paymentrisk.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.portfolio.paymentrisk.Json;
import java.util.*;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;

/** Append one record per admission; sort only the timestamp being finalized. */
final class PendingPayments {
  private final MapState<Long, String> legacy;
  private final MapState<Long, String> heads;
  private final MapState<String, String> records;

  PendingPayments(RuntimeContext context, MapState<Long, String> legacy) {
    this.legacy = legacy;
    heads =
        context.getMapState(new MapStateDescriptor<>("pending-heads-v2", Types.LONG, Types.STRING));
    records =
        context.getMapState(
            new MapStateDescriptor<>("pending-records-v2", Types.STRING, Types.STRING));
  }

  void add(long time, JsonNode item) throws Exception {
    String id = item.path("transaction").path("event_id").asText();
    if (records.contains(id)) throw new IllegalStateException("Duplicate pending event identity");
    var entry = Json.object();
    entry.set("item", item);
    String previous = heads.get(time);
    if (previous != null) entry.put("next", previous);
    records.put(id, Json.write(entry));
    heads.put(time, id);
  }

  List<JsonNode> drain(long time) throws Exception {
    var result = new ArrayList<JsonNode>();
    String old = legacy.get(time);
    if (old != null) StateHistory.rows(old).forEach(result::add);
    if (old != null) legacy.remove(time);
    String id = heads.get(time);
    if (id == null) return result;
    while (id != null) {
      String saved = records.get(id);
      if (saved == null) throw new IllegalStateException("Missing pending payment record");
      var entry = Json.read(saved);
      result.add(entry.path("item"));
      records.remove(id);
      id = entry.has("next") ? entry.path("next").asText() : null;
    }
    heads.remove(time);
    return result;
  }

  boolean contains(long time) throws Exception {
    return heads.contains(time) || legacy.contains(time);
  }

  long oldest(long fallback) throws Exception {
    long oldest = fallback;
    for (String value : legacy.values())
      for (var item : StateHistory.rows(value))
        oldest = Math.min(oldest, item.path("queued_at").asLong(fallback));
    for (String value : records.values())
      oldest = Math.min(oldest, Json.read(value).path("item").path("queued_at").asLong(fallback));
    return oldest;
  }
}
