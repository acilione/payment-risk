package com.portfolio.paymentrisk;

import com.portfolio.paymentrisk.config.AppConfig;
import com.portfolio.paymentrisk.tools.IntegrationScenario;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

/** Optional IDE/host integration mode with real Compose Kafka and Schema Registry. */
@EnabledIfEnvironmentVariable(named = "RUN_KAFKA_IT", matches = "true")
class KafkaMiniClusterTest {
  @TempDir Path checkpoints;

  @Test
  void realKafkaWithEmbeddedFlink() throws Exception {
    var settings = new Configuration();
    settings.setString("execution.checkpointing.dir", checkpoints.toUri().toString());
    settings.setString("execution.checkpointing.storage", "filesystem");
    settings.setString("restart-strategy.type", "fixed-delay");
    settings.setString("restart-strategy.fixed-delay.attempts", "1");
    settings.setString("taskmanager.numberOfTaskSlots", "2");
    var env = StreamExecutionEnvironment.createLocalEnvironment(2, settings);
    env.enableCheckpointing(2000);
    env.setMaxParallelism(128);
    PaymentRiskJob.build(env, AppConfig.fromEnv());
    var job = env.executeAsync("payment-risk-minicluster-it");
    try {
      IntegrationScenario.main(new String[0]);
    } finally {
      job.cancel().get(30, TimeUnit.SECONDS);
    }
  }
}
