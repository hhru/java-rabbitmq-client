package ru.hh.rabbitmq.spring.persistent;

import ru.hh.metrics.StatsDSender;
import ru.hh.nab.common.properties.FileSettings;
import static ru.hh.rabbitmq.spring.persistent.PersistentPublisherConfigKeys.PERSISTENT_PUBLISHER_PREFIX;

public class PersistentPublisherBuilderFactory {

  private final DatabaseQueueService databaseQueueService;
  private final PersistentPublisherRegistry persistentPublisherRegistry;
  private final FileSettings persistenceFileSettings;
  private final StatsDSender statsDSender;
  private final String serviceName;

  PersistentPublisherBuilderFactory(DatabaseQueueService databaseQueueService,
      PersistentPublisherRegistry persistentPublisherRegistry, FileSettings fileSettings, StatsDSender statsDSender, String serviceName) {
    this.databaseQueueService = databaseQueueService;
    this.persistentPublisherRegistry = persistentPublisherRegistry;
    persistenceFileSettings = fileSettings.getSubSettings(PERSISTENT_PUBLISHER_PREFIX);
    this.statsDSender = statsDSender;
    this.serviceName = serviceName;
  }

  public PersistentPublisherBuilder createPublisherBuilder(String publisherKey) {
    return new PersistentPublisherBuilder(databaseQueueService, persistentPublisherRegistry,
      serviceName, publisherKey, persistenceFileSettings.getSubSettings(publisherKey), statsDSender);
  }
}
