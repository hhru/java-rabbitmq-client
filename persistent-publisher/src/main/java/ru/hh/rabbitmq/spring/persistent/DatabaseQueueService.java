package ru.hh.rabbitmq.spring.persistent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;
import ru.hh.rabbitmq.spring.persistent.dto.TargetedDestination;
import ru.hh.rabbitmq.spring.send.CorrelatedMessage;
import ru.hh.rabbitmq.spring.send.MessageSender;
import static java.lang.String.join;
import static java.util.stream.Collectors.toList;
import static ru.hh.rabbitmq.spring.persistent.PersistentPublisherResource.DATABASE_QUEUE_RABBIT_PUBLISH;
import static ru.hh.rabbitmq.spring.persistent.PersistentPublisherResource.SENDER_KEY;

public class DatabaseQueueService {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseQueueService.class);
  private static final ObjectMapper DESTINATION_CONVERTER = new ObjectMapper();
  private static final int NEW_REGISTRATION = 1;

  private final DatabaseQueueDao databaseQueueDao;
  private final PersistentPublisherRegistry persistentPublisherRegistry;

  public DatabaseQueueService(DatabaseQueueDao databaseQueueDao, PersistentPublisherRegistry persistentPublisherRegistry) {
    this.databaseQueueDao = databaseQueueDao;
    this.persistentPublisherRegistry = persistentPublisherRegistry;
  }

  //TODO http API to register job
  @Transactional
  public void registerHhInvokerJob(String queueName, String upstreamName, String jerseyBasePath, Duration pollingInterval) {
    databaseQueueDao.registerOrUpdateHhInvokerJob(queueName + ":rabbit publication job",
      upstreamName + jerseyBasePath + DATABASE_QUEUE_RABBIT_PUBLISH + '?' + join("=", SENDER_KEY, queueName), pollingInterval);
  }

  @Transactional
  public Long publish(String queueName, Object message, TargetedDestination destination) {
    return databaseQueueDao.publish(queueName, toDb(destination),
      persistentPublisherRegistry.getMessageConverter(destination.getConverterKey()).convertToDb(message));
  }

  private static String toDb(TargetedDestination destination) {
    try {
      return DESTINATION_CONVERTER.writeValueAsString(destination);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Transactional
  public void sendBatch(String senderKey) {
    DatabaseQueueSender sender = persistentPublisherRegistry.getSender(senderKey);
    if (sender == null) {
      LOGGER.error("Trying to send batch for {}, but no publisher found for the key", senderKey);
      return;
    }
    String queueName = sender.getDatabaseQueueName();
    Optional<Long> batchId = databaseQueueDao.getNextBatchId(queueName, queueName);
    if (!batchId.isPresent()) {
      LOGGER.info("No batchId is available. Just wait for next iteration");
      return;
    }
    List<MessageEventContainer> events = getNextBatchEvents(queueName, batchId.get(), sender);
      events.forEach(messageEventContainer -> {
        TargetedDestination destination = messageEventContainer.getDestination();
        try {
          Object message = Optional.ofNullable(destination.getCorrelationData())
            .map(correlationData -> (Object) new CorrelatedMessage(correlationData, messageEventContainer.getMessage()))
            .orElseGet(messageEventContainer::getMessage);
          MessageSender messageSender = persistentPublisherRegistry.getSender(destination.getSenderKey()).getMessageSender();
          messageSender.publishMessage(message, destination);
        } catch (Exception e) {
          sender.onAmpqException(e, messageEventContainer.getId(), batchId.get(), destination, messageEventContainer.getMessage());
        }
      });
    databaseQueueDao.finishBatch(batchId.get());
  }

  @Transactional
  public boolean registerConsumerIfPossible(String queueName, String consumerName) {
    registerQueueIfPossible(queueName);
    LOGGER.debug("Registering same name PGQ consumer for queue: {}", consumerName);
    Integer result = databaseQueueDao.registerConsumer(queueName, consumerName);
    boolean newRegistration = NEW_REGISTRATION == result;
    if (newRegistration) {
      LOGGER.info("PGQ consumer {} for queue {} registered successfully for the first time", consumerName, queueName);
    } else {
      LOGGER.info("PGQ consumer {} for queue {} was already registered", consumerName, queueName);
    }
    return newRegistration;
  }

  @Transactional
  public boolean registerQueueIfPossible(String queueName) {
    LOGGER.debug("Registering queue: {}", queueName);
    Integer result = databaseQueueDao.registerQueue(queueName);
    boolean newRegistration = NEW_REGISTRATION == result;
    if (newRegistration) {
      LOGGER.info("PGQ queue {} registered successfully for the first time", queueName);
    } else {
      LOGGER.info("PGQ queue {} was already registered", queueName);
    }
    return newRegistration;
  }

  @Transactional
  public boolean isQueueRegistered(String queueName) {
    return databaseQueueDao.getQueueInfo(queueName).isPresent();
  }

  @Transactional
  public boolean isConsumerRegistered(String consumerName) {
    return databaseQueueDao.getConsumerInfo(consumerName).isPresent();
  }

  @Transactional
  public boolean isAllRegistered(String queueName, String consumerName) {
    return isQueueRegistered(queueName) && isConsumerRegistered(consumerName);
  }

  public void retryEvent(long eventId, long batchId, Duration retryEventDelay) {
    int result = databaseQueueDao.retryEvent(eventId, batchId, Math.toIntExact(retryEventDelay.getSeconds()));
    if (NEW_REGISTRATION != result) {
      LOGGER.error("Error scheduling event {} for retry", eventId);
    }
  }

  private List<MessageEventContainer> getNextBatchEvents(String queueName, long batchId, DatabaseQueueSender sender) {
    LOGGER.debug("Getting next events batch for ID {}", batchId);
    return databaseQueueDao.getNextBatchEvents(batchId).stream().map(row -> {
      long id = row.get(0, Long.class);
      String data = row.get(1, String.class);
      String type = row.get(2, String.class);
      try {
        TargetedDestination destination = DESTINATION_CONVERTER.readValue(type, TargetedDestination.class);
        MessageConverter messageConverter = persistentPublisherRegistry.getMessageConverter(destination.getConverterKey());
        return new MessageEventContainer(id, destination, messageConverter, data);
      } catch (Exception e) {
        return sender.onConvertationException(e, id, data, type);
      }
    }).filter(Objects::nonNull).collect(toList());
  }

  private static final class MessageEventContainer {
    private final long id;
    private final Object message;
    private final TargetedDestination destination;

    private MessageEventContainer(long id, TargetedDestination destination, MessageConverter messageConverter, String data) {
      this.id = id;
      this.destination = destination;
      message = messageConverter.convertFromDb(data, destination.getMsgClass());
    }

    @Override
    public String toString() {
      return "MessageContainer{" + id + "}: message=" + message + ", destination=" + destination;
    }

    public long getId() {
      return id;
    }

    public Object getMessage() {
      return message;
    }

    public TargetedDestination getDestination() {
      return destination;
    }
  }
}
