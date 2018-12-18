package ru.hh.rabbitmq.spring.persistent;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import ru.hh.metrics.StatsDSender;
import ru.hh.nab.common.properties.FileSettings;
import ru.hh.rabbitmq.spring.ConnectionsFactory;
import ru.hh.rabbitmq.spring.send.MessageSender;
import ru.hh.rabbitmq.spring.send.RabbitTemplateFactory;
import static ru.hh.rabbitmq.spring.ConfigKeys.HOST;
import static ru.hh.rabbitmq.spring.ConfigKeys.HOSTS;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_HOSTS;
import static ru.hh.rabbitmq.spring.persistent.PersistentPublisherConfigKeys.DB_QUEUE_CONSUMER_NAME_PROPERTY;
import static ru.hh.rabbitmq.spring.persistent.PersistentPublisherConfigKeys.DB_QUEUE_NAME_PROPERTY;
import static ru.hh.rabbitmq.spring.persistent.PersistentPublisherConfigKeys.RABBIT_ERROR_QUEUE_PROPERTY;
import static ru.hh.rabbitmq.spring.persistent.PersistentPublisherConfigKeys.RETRY_DELAY_SEC_PROPERTY;

public class PersistentPublisherBuilder {

  private final DatabaseQueueService databaseQueueService;
  private final PersistentPublisherRegistry persistentPublisherRegistry;
  private final String publisherKey;
  private final FileSettings publisherFileSettings;

  private final StatsDSender statsDSender;
  private final String serviceName;

  private final RabbitTemplate rabbitTemplate;

  private DbQueueProcessor mainDbQueueProcessor;
  private DbQueueProcessor[] additionalQueueConverters = new DbQueueProcessor[0];

  PersistentPublisherBuilder(DatabaseQueueService databaseQueueService, PersistentPublisherRegistry persistentPublisherRegistry,
      String serviceName, String publisherKey, FileSettings publisherFileSettings, StatsDSender statsDSender) {
    this.databaseQueueService = databaseQueueService;
    this.persistentPublisherRegistry = persistentPublisherRegistry;
    this.serviceName = serviceName;
    this.publisherKey = publisherKey;
    this.publisherFileSettings = publisherFileSettings;
    this.statsDSender = statsDSender;
    rabbitTemplate = createRabbitTemplate();
  }

  public PersistentPublisher build() {
    MessageSender messageSender = new MessageSender(rabbitTemplate, serviceName, statsDSender);
    String databaseQueueName = Objects.requireNonNull(publisherFileSettings.getString(DB_QUEUE_NAME_PROPERTY),
      DB_QUEUE_NAME_PROPERTY + " must be set");
    Duration retryDelay = Duration.ofSeconds(Objects.requireNonNull(publisherFileSettings.getLong(RETRY_DELAY_SEC_PROPERTY),
      RETRY_DELAY_SEC_PROPERTY + " must be set"));
    String errorTableName = publisherFileSettings.getString(RABBIT_ERROR_QUEUE_PROPERTY);
    String databaseQueueConsumerName = publisherFileSettings.getString(DB_QUEUE_CONSUMER_NAME_PROPERTY);
    PersistentPublisher persistentPublisher = new PersistentPublisher(databaseQueueService,
      databaseQueueName, databaseQueueConsumerName, errorTableName,
      publisherKey, retryDelay, messageSender, mainDbQueueProcessor, additionalQueueConverters);
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

  public PersistentPublisherBuilder withJsonMessageConverter() {
    Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter();
    return withMessageConverter(converter);
  }

  public PersistentPublisherBuilder withMessageConverter(MessageConverter messageConverter) {
    rabbitTemplate.setMessageConverter(messageConverter);
    return this;
  }

  public PersistentPublisherBuilder withMainDbQueueProcessor(DbQueueProcessor dbQueueProcessor) {
    mainDbQueueProcessor = dbQueueProcessor;
    return this;
  }

  public PersistentPublisherBuilder withAdditionalDbQueueProcessors(DbQueueProcessor... dbQueueProcessors) {
    additionalQueueConverters = dbQueueProcessors;
    return this;
  }

  private RabbitTemplate createRabbitTemplate() {
    ConnectionsFactory connectionsFactory = new ConnectionsFactory(publisherFileSettings.getProperties());
    List<ConnectionFactory> connectionFactories = connectionsFactory.createConnectionFactories(true, PUBLISHER_HOSTS, HOSTS, HOST);
    RabbitTemplateFactory rabbitTemplateFactory = new RabbitTemplateFactory(publisherFileSettings.getProperties());
    return rabbitTemplateFactory.createTemplate(connectionFactories.get(0));
  }
}
