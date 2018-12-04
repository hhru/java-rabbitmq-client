package ru.hh.rabbitmq.spring.persistent;

import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import ru.hh.rabbitmq.spring.persistent.dto.TargetedDestination;
import ru.hh.rabbitmq.spring.send.Destination;
import ru.hh.rabbitmq.spring.send.MessageSender;

public abstract class PersistentPublisher implements DatabaseQueueSender, SmartLifecycle {

  private static final Logger LOGGER = LoggerFactory.getLogger(PersistentPublisher.class);

  private final DatabaseQueueService databaseQueueService;
  private final String upstreamName;
  private final String jerseyBasePath;
  private final Duration pollingInterval;
  private final String senderKey;

  private final String converterKey;
  private final String databaseQueueName;
  private final MessageSender messageSender;

  protected PersistentPublisher(DatabaseQueueService databaseQueueService, String databaseQueueName, String upstreamName,
      String jerseyBasePath, Duration pollingInterval, String senderKey, String converterKey, MessageSender messageSender) {
    this.databaseQueueService = databaseQueueService;
    this.databaseQueueName = databaseQueueName;
    this.upstreamName = upstreamName;
    this.jerseyBasePath = jerseyBasePath;
    this.pollingInterval = pollingInterval;
    this.senderKey = senderKey;
    this.converterKey = converterKey;
    this.messageSender = messageSender;
  }

  public void send(Object message) {
    databaseQueueService.publish(databaseQueueName, message, TargetedDestination.build(null, message, converterKey, senderKey));
  }

  public void send(Object message, Destination destination) {
    databaseQueueService.publish(databaseQueueName, message, TargetedDestination.build(destination, message, converterKey, senderKey));
  }

  @Override
  public void start() {
    databaseQueueService.registerConsumerIfPossible(databaseQueueName, databaseQueueName);
    LOGGER.info("Registered consumer for queue {}, senderKey {}", databaseQueueName, senderKey);
  }

  @Override
  public void stop() {
    LOGGER.info("Stopping consumer for queue {}, senderKey {}", databaseQueueName, senderKey);
  }

  @Override
  public boolean isRunning() {
    databaseQueueService.registerHhInvokerJob(databaseQueueName, upstreamName, jerseyBasePath, pollingInterval);
    LOGGER.info("Registered hh-invoker job for senderKey {}", senderKey);
    return databaseQueueService.isAllRegistered(databaseQueueName, databaseQueueName);
  }

  @Override
  public boolean isAutoStartup() {
    return true;
  }

  @Override
  public void stop(Runnable callback) {
    stop();
    callback.run();
  }

  @Override
  public int getPhase() {
    return Integer.MAX_VALUE;
  }

  @Override
  public MessageSender getMessageSender() {
    return messageSender;
  }

  @Override
  public String getDatabaseQueueName() {
    return databaseQueueName;
  }
}
