package ru.hh.rabbitmq.spring.persistent;

import java.util.Optional;
import org.hibernate.SessionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.hh.nab.metrics.StatsDSender;
import ru.hh.nab.common.properties.FileSettings;

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
  DatabaseQueueService databaseQueueService(DatabaseQueueDao databaseQueueDao, PersistentPublisherRegistry persistentPublisherRegistry) {
    return new DatabaseQueueService(databaseQueueDao, persistentPublisherRegistry);
  }

  @Bean
  PersistentPublisherBuilderFactory persistentPublisherBuilder(DatabaseQueueService databaseQueueService,
      PersistentPublisherRegistry persistentPublisherRegistry, FileSettings settings,
      String serviceName, Optional<StatsDSender> statsDSender) {

    return new PersistentPublisherBuilderFactory(databaseQueueService, persistentPublisherRegistry, settings,
      statsDSender.orElse(null), serviceName);
  }
}
