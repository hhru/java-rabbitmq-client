package ru.hh.rabbitmq.spring.persistent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;
import ru.hh.rabbitmq.spring.persistent.dto.TargetedDestination;
import ru.hh.rabbitmq.spring.send.CorrelatedMessage;
import ru.hh.rabbitmq.spring.send.MessageSender;
import static java.util.stream.Collectors.toList;
import static ru.hh.rabbitmq.spring.persistent.PersistentPublisherConfigKeys.DB_QUEUE_CONSUMER_NAME_PROPERTY;

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

  @Transactional
  public Long publish(String queueName, Object message, TargetedDestination destination) {
    return databaseQueueDao.publish(queueName, toDb(destination), JacksonDbQueueConverter.INSTANCE.convertToDb(message));
  }

  @Transactional
  public void sendBatch(String senderKey) {
    DatabaseQueueSender sender = persistentPublisherRegistry.getSender(senderKey);
    if (sender == null) {
      throw new RuntimeException("Trying to send batch for " + senderKey + "but no publisher found for the key");
    }
    if (sender.getConsumerName() == null) {
      throw new RuntimeException(DB_QUEUE_CONSUMER_NAME_PROPERTY + " is not configured to get events from DB for sender " + senderKey);
    }
    String queueName = sender.getDatabaseQueueName();
    Optional<Long> batchId = databaseQueueDao.getNextBatchId(queueName, sender.getConsumerName());
    if (!batchId.isPresent()) {
      LOGGER.info("No batchId is available. Just wait for next iteration");
      return;
    }
    List<MessageEventContainer> events = getNextBatchEvents(batchId.get(), sender);
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
    LOGGER.debug("Batch {} finished", batchId);
  }

  @Transactional(readOnly = true)
  public boolean isQueueRegistered(String queueName) {
    return databaseQueueDao.getQueueInfo(queueName).isPresent();
  }

  @Transactional(readOnly = true)
  public boolean isConsumerRegistered(String consumerName) {
    return databaseQueueDao.getConsumerInfo(consumerName).isPresent();
  }

  @Transactional(readOnly = true)
  public boolean isAllDbQueueToolsRegistered(String queueName, String consumerName) {
    return isQueueRegistered(queueName) && isConsumerRegistered(consumerName);
  }

  @Transactional
  public void retryEvent(DatabaseQueueSender databaseQueueSender, long eventId, long batchId, Duration retryEventDelay,
      TargetedDestination destination, Object message) {
    int result = databaseQueueDao.retryEvent(eventId, batchId, retryEventDelay.getSeconds());
    if (NEW_REGISTRATION != result) {
      LOGGER.error("Error scheduling event {} for retry", eventId);
      logErrorIfErrorTablePresent(databaseQueueSender, eventId, toDb(destination),
        JacksonDbQueueConverter.INSTANCE.convertToDb(message)
      );
    }
  }

  @Transactional
  public void logErrorIfErrorTablePresent(DatabaseQueueSender databaseQueueSender, long eventId, String destination, String message) {
    if (!databaseQueueSender.getErrorTableName().isPresent()) {
      LOGGER.warn("Error processing event {} by consumer {} in queue {}. " +
        "Table for saving error data is not set, so dropping event", eventId, databaseQueueSender.getConsumerName(),
        databaseQueueSender.getDatabaseQueueName()
      );
      return;
    }
    String errorTableName = databaseQueueSender.getErrorTableName().get();
    LOGGER.error("Saving event {} in error table {}", eventId, errorTableName);
    databaseQueueDao.saveError(errorTableName, LocalDateTime.now(), eventId,
      databaseQueueSender.getDatabaseQueueName(),
      databaseQueueSender.getConsumerName(),
      destination, message);
  }

  private List<MessageEventContainer> getNextBatchEvents(long batchId, DatabaseQueueSender sender) {
    LOGGER.debug("Getting next events batch for ID {}", batchId);
    return databaseQueueDao.getNextBatchEvents(batchId).stream().map(row -> {
      long id = row.get(0, Number.class).longValue();
      String data = row.get(1, String.class);
      String type = row.get(2, String.class);
      try {
        TargetedDestination destination = DESTINATION_CONVERTER.readValue(type, TargetedDestination.class);
        return new MessageEventContainer(id, destination, JacksonDbQueueConverter.INSTANCE, data);
      } catch (Exception e) {
        return sender.onConvertationException(e, id, data, type);
      }
    }).filter(Objects::nonNull).collect(toList());
  }

  private static String toDb(TargetedDestination destination) {
    try {
      return DESTINATION_CONVERTER.writeValueAsString(destination);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private static final class MessageEventContainer {
    private final long id;
    private final Object message;
    private final TargetedDestination destination;

    private MessageEventContainer(long id, TargetedDestination destination, DbQueueConverter dbQueueConverter, String data) {
      this.id = id;
      this.destination = destination;
      message = dbQueueConverter.convertFromDb(data, destination.getMsgClass());
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
