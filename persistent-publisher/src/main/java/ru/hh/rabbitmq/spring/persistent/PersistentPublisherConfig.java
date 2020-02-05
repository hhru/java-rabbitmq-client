package ru.hh.rabbitmq.spring.persistent;

import java.util.Optional;
import org.hibernate.SessionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.hh.nab.metrics.StatsDSender;
import ru.hh.nab.common.properties.FileSettings;
import static java.util.Optional.ofNullable;

@Configuration
public class PersistentPublisherConfig {

  @Bean
  DatabaseQueueDao databaseQueueDao(SessionFactory sessionFactory) {
    return new DatabaseQueueDao(sessionFactory);
  }

  @Bean
  PersistentPublisherRegistry persistentPublisherRegistry() {
    return new PersistentPublisherRegistry();
  }

  @Bean
  DatabaseQueueService databaseQueueService(DatabaseQueueDao databaseQueueDao, PersistentPublisherRegistry persistentPublisherRegistry,
                                            StatsDSender statsDSender, FileSettings settings) {
    int batchSizeHistogramSize = ofNullable(settings.getInteger(String.join(".", DatabaseQueueService.CONFIG_KEY, "batchSizeHistogramSize")))
      .orElse(1000);
    int stageHistogramSize = ofNullable(settings.getInteger(String.join(".", DatabaseQueueService.CONFIG_KEY, "stageHistogramSize")))
      .orElse(batchSizeHistogramSize * DatabaseQueueService.SendingStage.values().length);
    long statsSendIntervalMs = ofNullable(settings.getLong(String.join(".", DatabaseQueueService.CONFIG_KEY, "statsSendIntervalMs")))
      .orElse(30L);
    return new DatabaseQueueService(databaseQueueDao, persistentPublisherRegistry, statsDSender, batchSizeHistogramSize, stageHistogramSize,
      statsSendIntervalMs
    );
  }

  @Bean
  PersistentPublisherBuilderFactory persistentPublisherBuilder(DatabaseQueueService databaseQueueService,
      PersistentPublisherRegistry persistentPublisherRegistry, FileSettings settings,
      String serviceName, Optional<StatsDSender> statsDSender) {

    return new PersistentPublisherBuilderFactory(databaseQueueService, persistentPublisherRegistry, settings,
      statsDSender.orElse(null), serviceName);
  }
}
