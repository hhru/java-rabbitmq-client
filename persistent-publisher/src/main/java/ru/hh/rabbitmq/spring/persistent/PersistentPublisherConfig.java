package ru.hh.rabbitmq.spring.persistent;

import java.util.Collection;
import java.util.Optional;
import org.hibernate.SessionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.hh.metrics.StatsDSender;
import ru.hh.nab.common.properties.FileSettings;

@Configuration
public class PersistentPublisherConfig {

  @Bean
  DatabaseQueueDao databaseQueueDao(SessionFactory sessionFactory) {
    return new DatabaseQueueDao(sessionFactory);
  }

  @Bean
  JacksonDbQueueConverter jacksonDbQueueConverter() {
    return JacksonDbQueueConverter.INSTANCE;
  }

  @Bean
  PersistentPublisherRegistry persistentPublisherRegistry(Collection<DbQueueConverter> converters) {
    PersistentPublisherRegistry persistentPublisherRegistry = new PersistentPublisherRegistry();
    converters.forEach(persistentPublisherRegistry::registerConverter);
    return persistentPublisherRegistry;
  }

  @Bean
  DatabaseQueueService databaseQueueService(DatabaseQueueDao databaseQueueDao, PersistentPublisherRegistry persistentPublisherRegistry) {
    return new DatabaseQueueService(databaseQueueDao, persistentPublisherRegistry);
  }

  @Bean
  PersistentPublisherBuilderFactory persistentPublisherBuilder(DatabaseQueueDao databaseQueueDao, DatabaseQueueService databaseQueueService,
      PersistentPublisherRegistry persistentPublisherRegistry, FileSettings settings, String serviceName, Optional<StatsDSender> statsDSender) {

    return new PersistentPublisherBuilderFactory(databaseQueueService, persistentPublisherRegistry, settings,
      statsDSender.orElse(null), serviceName);
  }
}
