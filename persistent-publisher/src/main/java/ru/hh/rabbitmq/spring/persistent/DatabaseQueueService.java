package ru.hh.rabbitmq.spring.persistent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;
import ru.hh.hhinvoker.api.dto.TaskDto;
import ru.hh.hhinvoker.api.enums.ScheduleType;
import ru.hh.hhinvoker.client.InvokerClient;
import ru.hh.rabbitmq.spring.persistent.dto.TargetedDestination;
import ru.hh.rabbitmq.spring.persistent.http.EmptyHttpContext;
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
  private final InvokerClient invokerClient;
  private final EmptyHttpContext emptyHttpContext;

  public DatabaseQueueService(DatabaseQueueDao databaseQueueDao, PersistentPublisherRegistry persistentPublisherRegistry,
      InvokerClient invokerClient, EmptyHttpContext emptyHttpContext) {
    this.databaseQueueDao = databaseQueueDao;
    this.persistentPublisherRegistry = persistentPublisherRegistry;
    this.invokerClient = invokerClient;
    this.emptyHttpContext = emptyHttpContext;
  }

  @Transactional
  public void registerHhInvokerJob(String senderKey, String consumerKey, String taskBaseUrl, Duration pollingInterval) {
    String targetUrl = taskBaseUrl + DATABASE_QUEUE_RABBIT_PUBLISH + '?' + join("=", SENDER_KEY, senderKey);
    TaskDto taskDto = new TaskDto(consumerKey, "job to publish rabbit messages from pgq", true,
      ScheduleType.COUNTER_STARTS_AFTER_TASK_FINISH, pollingInterval.getSeconds(), targetUrl, false,
      pollingInterval.getSeconds() / 2, TimeUnit.MINUTES.toSeconds(pollingInterval.getSeconds()));
    try {
      emptyHttpContext.executeAsServerRequest(() -> invokerClient.createOrUpdate(taskDto).get());
      LOGGER.info("Regitered job for targetUrl={}", targetUrl);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("Thread was interrupted while trying to register hh-invoker job for consumer {}", consumerKey, e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Transactional
  public Long publish(String queueName, Object message, TargetedDestination destination) {
    return databaseQueueDao.publish(queueName, toDb(destination),
      persistentPublisherRegistry.getMessageConverter(destination.getConverterKey()).convertToDb(message));
  }

  @Transactional
  public void sendBatch(String senderKey) {
    DatabaseQueueSender sender = persistentPublisherRegistry.getSender(senderKey);
    if (sender == null) {
      LOGGER.error("Trying to send batch for {}, but no publisher found for the key", senderKey);
      return;
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

  @Transactional(readOnly = true)
  public boolean isQueueRegistered(String queueName) {
    return databaseQueueDao.getQueueInfo(queueName).isPresent();
  }

  @Transactional(readOnly = true)
  public boolean isConsumerRegistered(String consumerName) {
    return databaseQueueDao.getConsumerInfo(consumerName).isPresent();
  }

  public boolean isTaskRegistered(String consumerName) {
    try {
      return emptyHttpContext.executeAsServerRequest(() -> invokerClient.getTaskInfo(consumerName).get().isSuccess());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (ExecutionException e) {
      LOGGER.warn("Failed to check if all is registered", e);
      return false;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Transactional(readOnly = true)
  public boolean isAllDbQueueToolsRegistered(String queueName, String consumerName) {
    return isQueueRegistered(queueName) && isConsumerRegistered(consumerName);
  }

  public void retryEvent(long eventId, long batchId, Duration retryEventDelay) {
    int result = databaseQueueDao.retryEvent(eventId, batchId, Math.toIntExact(retryEventDelay.getSeconds()));
    if (NEW_REGISTRATION != result) {
      LOGGER.error("Error scheduling event {} for retry", eventId);
    }
  }

  private List<MessageEventContainer> getNextBatchEvents(long batchId, DatabaseQueueSender sender) {
    LOGGER.debug("Getting next events batch for ID {}", batchId);
    return databaseQueueDao.getNextBatchEvents(batchId).stream().map(row -> {
      long id = row.get(0, Number.class).longValue();
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
