package ru.hh.rabbitmq.spring.persistent;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import ru.hh.metrics.StatsDSender;
import ru.hh.nab.common.properties.FileSettings;
import ru.hh.rabbitmq.spring.ConnectionsFactory;
import ru.hh.rabbitmq.spring.send.Destination;
import ru.hh.rabbitmq.spring.send.MessageSender;
import ru.hh.rabbitmq.spring.send.RabbitTemplateFactory;
import static ru.hh.rabbitmq.spring.ConfigKeys.HOST;
import static ru.hh.rabbitmq.spring.ConfigKeys.HOSTS;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_HOSTS;
import static ru.hh.rabbitmq.spring.persistent.JacksonMessageConverter.JACKSON_CONVERTER_KEY;
import static ru.hh.rabbitmq.spring.persistent.PersistentPublisherConfigKeys.DB_QUEUE_NAME_PROPERTY;
import static ru.hh.rabbitmq.spring.persistent.PersistentPublisherConfigKeys.POLLING_INTERVAL_SEC_PROPERTY;
import static ru.hh.rabbitmq.spring.persistent.PersistentPublisherConfigKeys.RETRY_DELAY_SEC_PROPERTY;

public class PersistentPublisherBuilder {

  private final DatabaseQueueService databaseQueueService;
  private final DatabaseQueueDao databaseQueueDao;
  private final PersistentPublisherRegistry persistentPublisherRegistry;
    private final String jerseyBasePath;
  private final String upstreamName;
  private final String publisherKey;
  private final FileSettings publisherFileSettings;

  private final String databaseQueueName;
  private final Duration pollingInterval;

  private final StatsDSender statsDSender;
  private final String serviceName;

  private final RabbitTemplate rabbitTemplate;

  PersistentPublisherBuilder(DatabaseQueueService databaseQueueService, DatabaseQueueDao databaseQueueDao,
      PersistentPublisherRegistry persistentPublisherRegistry,
      String jerseyBasePath, String upstreamName, String publisherKey, FileSettings publisherFileSettings, StatsDSender statsDSender,
      String serviceName) {
    this.databaseQueueService = databaseQueueService;
    this.databaseQueueDao = databaseQueueDao;
    this.persistentPublisherRegistry = persistentPublisherRegistry;
    this.jerseyBasePath = jerseyBasePath;
    this.upstreamName = upstreamName;
    this.publisherKey = publisherKey;
    this.publisherFileSettings = publisherFileSettings;
    databaseQueueName = Objects.requireNonNull(publisherFileSettings.getString(DB_QUEUE_NAME_PROPERTY),
      DB_QUEUE_NAME_PROPERTY + " must be set");
    pollingInterval = Duration.ofSeconds(Objects.requireNonNull(publisherFileSettings.getLong(POLLING_INTERVAL_SEC_PROPERTY),
      POLLING_INTERVAL_SEC_PROPERTY + " must be set"));
    this.serviceName = serviceName;
    this.statsDSender = statsDSender;
    rabbitTemplate = createRabbitTemplate();
  }

  public PersistentPublisher build() {
    MessageSender messageSender = new MessageSender(rabbitTemplate, serviceName, statsDSender);
    PersistentPublisher persistentPublisher = new PersistentPublisher(databaseQueueService, databaseQueueName, upstreamName, jerseyBasePath,
      pollingInterval, publisherKey, JACKSON_CONVERTER_KEY, messageSender) {

      private final Logger sendLogger = LoggerFactory.getLogger(PersistentPublisher.class + "." + publisherKey);
      private final String errorQueueName = databaseQueueName + ":error";
      @Override
      public void start() {
        super.start();
        databaseQueueService.registerQueueIfPossible(errorQueueName);
      }

      @Override
      public void onAmpqException(Exception e, long eventId, long batchId, Destination type, Object data) {
        Duration duration = Duration.ofSeconds(Objects.requireNonNull(publisherFileSettings.getLong(RETRY_DELAY_SEC_PROPERTY),
          RETRY_DELAY_SEC_PROPERTY + " must be set"));
        sendLogger.info("Got exception={} on sending event [id={}, destination={}, data={}], retrying on {}",
          e.getMessage(), eventId, type, data, duration);
        databaseQueueService.retryEvent(eventId, batchId, duration);
      }

      @Nullable
      @Override
      public <T> T onConvertationException(Exception e, long eventId, String type, String data) {
        sendLogger.warn("Failed to convert event: [id={}, type={}, data={}], moving to {}", eventId, type, data, errorQueueName, e);
        databaseQueueDao.publish(errorQueueName, type, data);
        return null;
      }
    };
    persistentPublisherRegistry.registerSender(publisherKey, persistentPublisher);
    return persistentPublisher;
  }

  protected PersistentPublisherBuilder withConfirmCallback(RabbitTemplate.ConfirmCallback callback) {
    rabbitTemplate.setConfirmCallback(callback);
    return this;
  }

  public PersistentPublisherBuilder withReturnCallback(RabbitTemplate.ReturnCallback callback) {
    rabbitTemplate.setReturnCallback(callback);
    return this;
  }

  private RabbitTemplate createRabbitTemplate() {
    ConnectionsFactory connectionsFactory = new ConnectionsFactory(publisherFileSettings.getProperties());
    List<ConnectionFactory> connectionFactories = connectionsFactory.createConnectionFactories(true, PUBLISHER_HOSTS, HOSTS, HOST);
    RabbitTemplateFactory rabbitTemplateFactory = new RabbitTemplateFactory(publisherFileSettings.getProperties());
    return rabbitTemplateFactory.createTemplate(connectionFactories.get(0));
  }
}
