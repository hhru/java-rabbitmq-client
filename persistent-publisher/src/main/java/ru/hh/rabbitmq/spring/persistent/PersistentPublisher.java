package ru.hh.rabbitmq.spring.persistent;

import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.support.CorrelationData;
import ru.hh.rabbitmq.spring.persistent.dto.TargetedDestination;
import ru.hh.rabbitmq.spring.send.CorrelatedMessage;
import ru.hh.rabbitmq.spring.send.Destination;
import ru.hh.rabbitmq.spring.send.MessageSender;

public abstract class PersistentPublisher implements DatabaseQueueSender {

  private static final Logger LOGGER = LoggerFactory.getLogger(PersistentPublisher.class);

  private final DatabaseQueueService databaseQueueService;
  private final String taskBaseUrl;

  private final String publisherKey;
  private final String converterKey;
  private final String databaseQueueName;

  private final String serviceName;
  private final Duration pollingInterval;
  private final MessageSender messageSender;

  protected PersistentPublisher(
      DatabaseQueueService databaseQueueService,
      String serviceName, String taskBaseUrl,
      String databaseQueueName, String publisherKey, String converterKey,
      Duration pollingInterval,
    MessageSender messageSender) {
    this.databaseQueueService = databaseQueueService;
    this.taskBaseUrl = taskBaseUrl;
    this.publisherKey = publisherKey;
    this.converterKey = converterKey;
    this.databaseQueueName = databaseQueueName;
    this.serviceName = serviceName;
    this.pollingInterval = pollingInterval;
    this.messageSender = messageSender;
  }

  public void send(Object message) {
    send(message, null);
  }

  public void send(Object message, Destination destination) {
    CorrelationData correlationData = null;
    if (message instanceof CorrelatedMessage) {
      CorrelatedMessage correlatedMessage = (CorrelatedMessage) message;
      message = correlatedMessage.getMessage();
      correlationData = correlatedMessage.getCorrelationData();
    }
    databaseQueueService.publish(databaseQueueName, message,
      TargetedDestination.build(destination, message, correlationData, converterKey, publisherKey)
    );
  }

  @Override
  public void start() {
    if (!databaseQueueService.isAllDbQueueToolsRegistered(databaseQueueName, getConsumerName())) {
      databaseQueueService.registerConsumerIfPossible(databaseQueueName, getConsumerName());
      LOGGER.info("Registered consumer {} for queue {}, publisherKey {}", getConsumerName(), databaseQueueName, publisherKey);
    }
    if (!databaseQueueService.isTaskRegistered(getConsumerName())) {
      databaseQueueService.registerHhInvokerJob(publisherKey, getConsumerName(), taskBaseUrl, pollingInterval);
      LOGGER.info("Registered hh-invoker job for publisherKey {}", publisherKey);
    }
  }

  @Override
  public MessageSender getMessageSender() {
    return messageSender;
  }

  @Override
  public String getDatabaseQueueName() {
    return databaseQueueName;
  }

  @Override
  public String getConsumerName() {
    return databaseQueueName + ':' + serviceName;
  }
}
