package ru.hh.rabbitmq.spring.persistent;

import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import ru.hh.metrics.StatsDSender;
import ru.hh.nab.common.properties.FileSettings;
import static ru.hh.rabbitmq.spring.persistent.PersistentPublisherConfigKeys.JERSEY_BASE_PATH_PROPERTY;
import static ru.hh.rabbitmq.spring.persistent.PersistentPublisherConfigKeys.PERSISTENT_PUBLISHER_PREFIX;
import static ru.hh.rabbitmq.spring.persistent.PersistentPublisherConfigKeys.UPSTREAM_PROPERTY;

public class PersistentPublisherBuilderFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(PersistentPublisherBuilderFactory.class);

  private final DatabaseQueueDao databaseQueueDao;
  private final DatabaseQueueService databaseQueueService;
  private final PersistentPublisherRegistry persistentPublisherRegistry;
  private final FileSettings persistenceFileSettings;
  private final String jerseyBasePath;
  private final String upstreamName;
  private final StatsDSender statsDSender;
  private final String serviceName;

  PersistentPublisherBuilderFactory(DatabaseQueueDao databaseQueueDao, DatabaseQueueService databaseQueueService,
      PersistentPublisherRegistry persistentPublisherRegistry, FileSettings fileSettings, StatsDSender statsDSender, String serviceName) {
    this.databaseQueueDao = databaseQueueDao;
    this.databaseQueueService = databaseQueueService;
    this.persistentPublisherRegistry = persistentPublisherRegistry;
    persistenceFileSettings = fileSettings.getSubSettings(PERSISTENT_PUBLISHER_PREFIX);
    jerseyBasePath = Objects.requireNonNull(persistenceFileSettings.getString(JERSEY_BASE_PATH_PROPERTY),
      JERSEY_BASE_PATH_PROPERTY + " must be set in service config with prefix " + PERSISTENT_PUBLISHER_PREFIX);
    upstreamName = Objects.requireNonNull(persistenceFileSettings.getString(UPSTREAM_PROPERTY),
      UPSTREAM_PROPERTY + " must be set in service config with prefix " + PERSISTENT_PUBLISHER_PREFIX);
    this.statsDSender = statsDSender;
    this.serviceName = serviceName;
  }

  public PersistentPublisherBuilder createPublisherBuilder(String publisherKey) {
    return new PersistentPublisherBuilder(databaseQueueService, databaseQueueDao, persistentPublisherRegistry,
      serviceName, upstreamName + jerseyBasePath, publisherKey,
      persistenceFileSettings.getSubSettings(publisherKey), statsDSender);
  }

  @EventListener(ContextRefreshedEvent.class)
  public void start() {
    LOGGER.info("Got {}", ContextRefreshedEvent.class.getSimpleName());
    persistentPublisherRegistry.getSenders().forEach(DatabaseQueueSender::start);
  }
}
