package com.portfolio.paymentrisk.tools;

import com.portfolio.paymentrisk.Json;
import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.storage.*;
import java.time.Duration;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/** Independent durable input capture: no dependency on Flink evaluation or the schema registry. */
public final class PaymentArchiver {
  private PaymentArchiver() {}

  public static void main(String[] args) throws Exception {
    var config = AppConfig.fromEnv();
    var properties =
        KafkaIngestion.properties(
            config,
            System.getenv().getOrDefault("ARCHIVER_GROUP", "risk-payment-archive-v1"),
            true);
    try (var metrics =
            new MaterializerMetrics(
                Integer.parseInt(System.getenv().getOrDefault("ARCHIVER_METRICS_PORT", "9406")));
        var consumer = new KafkaConsumer<byte[], byte[]>(properties)) {
      var writer = KafkaIngestion.writer(config, metrics);
      metrics.unresolved.set(0);
      KafkaIngestion.subscribe(
          consumer, config.topic("payments.raw"), config.environment().equals("local"));
      long lastProbe = 0;
      while (!Thread.currentThread().isInterrupted()) {
        var records = consumer.poll(Duration.ofSeconds(1));
        metrics.lastPoll.set(System.currentTimeMillis());
        if (System.currentTimeMillis() - lastProbe > 15000) {
          writer.ping();
          lastProbe = System.currentTimeMillis();
        }
        var batch =
            new DurableKafkaBatch(
                writer, consumer::commitSync, metrics, DurableKafkaBatch.DEFAULT_BYTES);
        long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
        for (var record : records) {
          if (System.nanoTime() > deadline)
            throw new java.io.IOException("Archive batch time budget exceeded");
          batch.add("risk.payment_ingress", Json.write(PaymentArchiveRows.convert(record)), record);
        }
        batch.flush();
        long lag = 0;
        for (var partition : consumer.assignment())
          lag = Math.max(lag, consumer.currentLag(partition).orElse(0));
        metrics.lag.set(lag);
      }
    }
  }
}
