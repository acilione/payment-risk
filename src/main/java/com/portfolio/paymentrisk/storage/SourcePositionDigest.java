package com.portfolio.paymentrisk.storage;

import com.portfolio.paymentrisk.Json;
import com.portfolio.paymentrisk.domain.Audit;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;

/** Order-independent across partitions, ordered within each partition; memory is O(partitions). */
public final class SourcePositionDigest {
  private final Map<String, MessageDigest> hashes = new TreeMap<>();
  private final Map<String, Long> last = new HashMap<>();

  public void add(String topic, int partition, long offset) {
    String key = topic + ":" + partition;
    if (offset < 0 || offset <= last.getOrDefault(key, -1L))
      throw new IllegalArgumentException("Non-increasing replay offset");
    last.put(key, offset);
    hashes
        .computeIfAbsent(
            key,
            ignored -> {
              try {
                return MessageDigest.getInstance("SHA-256");
              } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e);
              }
            })
        .update((offset + "\n").getBytes(StandardCharsets.UTF_8));
  }

  public String finish() {
    var result = new TreeMap<String, String>();
    hashes.forEach((key, hash) -> result.put(key, HexFormat.of().formatHex(hash.digest())));
    return Audit.sha256(Json.write(result));
  }
}
