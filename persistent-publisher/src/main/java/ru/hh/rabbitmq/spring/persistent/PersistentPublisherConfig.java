package ru.hh.rabbitmq.spring.persistent;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.hibernate.SessionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.hh.hhinvoker.client.InvokerClient;
import ru.hh.metrics.StatsDSender;
import ru.hh.nab.common.properties.FileSettings;

@Configuration
public class PersistentPublisherConfig {

  @Bean
  DatabaseQueueDao databaseQueueDao(SessionFactory sessionFactory) {
    return new DatabaseQueueDao(sessionFactory);
  }

  @Bean
  DatabaseQueueService databaseQueueService(DatabaseQueueDao databaseQueueDao, PersistentPublisherRegistry persistentPublisherRegistry) {
    return new DatabaseQueueService(databaseQueueDao, persistentPublisherRegistry);
  }

  @Bean
  PersistentPublisherRegistry persistentPublisherRegistry() {
    return new PersistentPublisherRegistry(new ConcurrentHashMap<>());
  }

  @Bean
  PersistentPublisherResource persistentPublisherResource(InvokerClient invokerClient, DatabaseQueueService databaseQueueService) {
    return new PersistentPublisherResource(invokerClient, databaseQueueService);
  }

  @Bean
  PersistentPublisherBuilderFactory persistentPublisherBuilder(DatabaseQueueDao databaseQueueDao, DatabaseQueueService databaseQueueService,
      PersistentPublisherRegistry persistentPublisherRegistry, FileSettings settings, String serviceName, Optional<StatsDSender> statsDSender) {

    return new PersistentPublisherBuilderFactory(databaseQueueDao, databaseQueueService, persistentPublisherRegistry, settings,
      statsDSender.orElse(null), serviceName);
  }
}
