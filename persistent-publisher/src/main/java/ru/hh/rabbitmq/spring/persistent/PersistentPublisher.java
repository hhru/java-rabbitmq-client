package ru.hh.rabbitmq.spring.persistent;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.support.CorrelationData;
import ru.hh.rabbitmq.spring.persistent.dto.TargetedDestination;
import ru.hh.rabbitmq.spring.send.CorrelatedMessage;
import ru.hh.rabbitmq.spring.send.Destination;
import ru.hh.rabbitmq.spring.send.MessageSender;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class PersistentPublisher implements DatabaseQueueSender {

  private static final Logger LOGGER = LoggerFactory.getLogger(PersistentPublisher.class);

  private final DatabaseQueueService databaseQueueService;

  private final String publisherKey;
  private final DbQueueProcessor mainDbQueueProcessor;
  private final String databaseQueueName;
  private final String databaseQueueConsumerName;

  private final Optional<String> errorTableName;
  private final Duration retryDelay;
  private final MessageSender messageSender;
  private final Map<String, DbQueueProcessor> converters;

  protected PersistentPublisher(DatabaseQueueService databaseQueueService,
      String databaseQueueName, String databaseQueueConsumerName, @Nullable String errorTableName,
      String publisherKey, Duration retryDelay, MessageSender messageSender, DbQueueProcessor mainDbQueueProcessor,
      DbQueueProcessor... additionalConverters) {
    this.databaseQueueService = databaseQueueService;
    this.databaseQueueName = databaseQueueName;
    this.databaseQueueConsumerName = databaseQueueConsumerName;
    this.errorTableName = Optional.ofNullable(errorTableName);
    this.publisherKey = publisherKey;
    this.mainDbQueueProcessor = mainDbQueueProcessor;
    this.retryDelay = retryDelay;
    this.messageSender = messageSender;
    converters = Stream.concat(Stream.of(mainDbQueueProcessor), Stream.of(additionalConverters))
      .collect(toMap(DbQueueProcessor::getKey, identity()));
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
      TargetedDestination.build(destination, message, correlationData, mainDbQueueProcessor.getKey(), publisherKey)
    );
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
  public Optional<String> getErrorTableName() {
    return errorTableName;
  }

  @Override
  public String getConsumerName() {
    return databaseQueueConsumerName;
  }

  @Override
  public DbQueueProcessor getConverter(String converterKey) {
    return Optional.ofNullable(converters.get(converterKey)).orElseThrow(() -> new RuntimeException("No converter with key " + converterKey));
  }

  @Override
  public void onAmpqException(Exception e, long eventId, long batchId, TargetedDestination destination, Object message) {
    LOGGER.info("Got exception={} on sending event [id={}, destination={}, message={}], going to retry after {}",
      e.getMessage(), eventId, destination, message, retryDelay);
    databaseQueueService.retryEvent(this, eventId, batchId, retryDelay, destination, message);
  }

  @Nullable
  @Override
  public <T> T onConvertationException(Exception e, long eventId, String destinationContent, String messageContent) {
    databaseQueueService.logErrorIfErrorTablePresent(this, eventId, destinationContent, messageContent);
    return null;
  }

  @Override
  public Duration getRetryDuration() {
    return retryDelay;
  }
}
