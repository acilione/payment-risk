package com.portfolio.paymentrisk.processor;

import com.portfolio.paymentrisk.Json;
import com.portfolio.paymentrisk.config.AppConfig;
import java.util.*;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimerService;

/** Exact expiration lookups. Existing savepoints are indexed once per customer on first timer. */
final class StateExpiryIndex {
  private final AppConfig config;
  private final MapState<Long, String> deviceBuckets;
  private final ValueState<Boolean> initialized;

  StateExpiryIndex(RuntimeContext context, AppConfig config) {
    this.config = config;
    deviceBuckets =
        context.getMapState(
            new MapStateDescriptor<>("device-expiry-index-v1", Types.LONG, Types.STRING));
    initialized =
        context.getState(new ValueStateDescriptor<>("expiry-index-ready-v1", Types.BOOLEAN));
  }

  int[] expire(
      long time,
      MapState<Long, String> history,
      MapState<String, Long> devices,
      TimerService timers)
      throws Exception {
    int removedHistory = 0, removedDevices = 0;
    if (initialized.value() == null) {
      // One migration scan, retaining the existing descriptors and serializers.
      var oldHistory = new ArrayList<Long>();
      for (var entry : history.entries()) {
        long expires = entry.getKey() + config.historyMs();
        if (expires <= time) {
          oldHistory.add(entry.getKey());
          for (var ignored : StateHistory.rows(entry.getValue())) removedHistory++;
        } else timers.registerEventTimeTimer(expires);
      }
      for (long key : oldHistory) history.remove(key);
      var oldDevices = new ArrayList<String>();
      for (var entry : devices.entries()) {
        if (entry.getValue() + config.deviceMs() <= time) oldDevices.add(entry.getKey());
        else deviceSeen(entry.getKey(), null, entry.getValue(), timers, ignored -> true);
      }
      for (String key : oldDevices) devices.remove(key);
      removedDevices = oldDevices.size();
      initialized.update(true);
    }
    String expired = history.get(time - config.historyMs());
    if (expired != null) {
      for (var ignored : StateHistory.rows(expired)) removedHistory++;
      history.remove(time - config.historyMs());
    }
    for (String device : bucket(time)) {
      Long seen = devices.get(device);
      if (seen != null && seen + config.deviceMs() <= time) {
        devices.remove(device);
        removedDevices++;
      }
    }
    deviceBuckets.remove(time);
    return new int[] {removedHistory, removedDevices};
  }

  @FunctionalInterface
  interface TimerInUse {
    boolean test(long time) throws Exception;
  }

  void clear() throws Exception {
    initialized.clear();
    deviceBuckets.clear();
  }

  void deviceSeen(String device, Long previous, long time, TimerService timers, TimerInUse inUse)
      throws Exception {
    if (previous != null && previous == time) return;
    if (previous != null) {
      long old = previous + config.deviceMs();
      var entries = bucket(old);
      entries.remove(device);
      if (entries.isEmpty()) {
        deviceBuckets.remove(old);
        if (!inUse.test(old)) timers.deleteEventTimeTimer(old);
      } else deviceBuckets.put(old, Json.write(entries));
      // A shared timer is removed only after checking pending work and other expiry owners.
    }
    long expires = time + config.deviceMs();
    var entries = bucket(expires);
    entries.add(device);
    deviceBuckets.put(expires, Json.write(entries));
    timers.registerEventTimeTimer(expires);
  }

  private Set<String> bucket(long time) throws Exception {
    var entries = new LinkedHashSet<String>();
    String saved = deviceBuckets.get(time);
    if (saved != null) Json.read(saved).forEach(v -> entries.add(v.asText()));
    return entries;
  }
}
