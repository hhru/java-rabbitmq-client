package ru.hh.rabbitmq.spring.persistent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.PostConstruct;
import javax.persistence.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;
import ru.hh.metrics.timinglogger.StageTimings;
import ru.hh.nab.metrics.AdjustingHistograms;
import ru.hh.nab.metrics.Histograms;
import ru.hh.nab.metrics.StatsDSender;
import ru.hh.nab.metrics.Tag;
import ru.hh.rabbitmq.spring.persistent.dto.TargetedDestination;
import ru.hh.rabbitmq.spring.send.CorrelatedMessage;
import ru.hh.rabbitmq.spring.send.MessageSender;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static ru.hh.rabbitmq.spring.persistent.PersistentPublisherConfigKeys.DB_QUEUE_CONSUMER_NAME_PROPERTY;

public class DatabaseQueueService {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseQueueService.class);
  private static final ObjectMapper DESTINATION_CONVERTER = new ObjectMapper();
  private static final int NEW_REGISTRATION = 1;
  public static final String CONFIG_KEY = "database.queue.service";

  private final DatabaseQueueDao databaseQueueDao;
  private final PersistentPublisherRegistry persistentPublisherRegistry;
  private StageTimings<SendingStage> stageTimings;
  private final StatsDSender statsDSender;

  private final Histograms batchSizeHistogram;
  private final int stageHistogramsSize;
  private final int statsSendIntervalMs;

  public DatabaseQueueService(DatabaseQueueDao databaseQueueDao, PersistentPublisherRegistry persistentPublisherRegistry, StatsDSender statsDSender,
                              int batchSizeHistogramSize, int batchSizeHistogramNumLimit,
                              int stageHistogramsSize,
                              long statsSendIntervalMs) {
    this.databaseQueueDao = databaseQueueDao;
    this.persistentPublisherRegistry = persistentPublisherRegistry;
    this.statsDSender = statsDSender;
    this.batchSizeHistogram = new AdjustingHistograms(batchSizeHistogramSize, batchSizeHistogramNumLimit,
      persistentPublisherRegistry::numberOfSenders);
    this.stageHistogramsSize = stageHistogramsSize;
    this.statsSendIntervalMs = Math.toIntExact(statsSendIntervalMs);
  }

  @PostConstruct
  public void init() {
    stageTimings = new StageTimings.Builder<>("databaseQueueTimings", SendingStage.class)
      .withTagName("stages")
      .withPercentiles(StatsDSender.DEFAULT_PERCENTILES)
      .withMaxHistogramSize(stageHistogramsSize).startOn(statsDSender, statsSendIntervalMs);
    statsDSender.sendPeriodically(() -> statsDSender.sendHistograms("databaseQueueBatchSize", batchSizeHistogram, StatsDSender.DEFAULT_PERCENTILES),
      statsSendIntervalMs
    );
  }

  @Transactional
  public Long publish(String queueName, Object message, TargetedDestination destination) {
    return databaseQueueDao.publish(queueName, toDb(destination), JacksonDbQueueProcessor.INSTANCE.convertToDb(message));
  }

  @Transactional
  public void sendBatch(String senderKey, int maxEventsPerBatchToProcess, int maxBatchesToProcessInTx, boolean multiBatchOnlyForEmptyBatches) {
    long sendStartMs = System.currentTimeMillis();
    DatabaseQueueSender sender = persistentPublisherRegistry.getSender(senderKey);
    if (sender == null) {
      throw new RuntimeException("Trying to send batch for " + senderKey + "but no publisher found for the key");
    }
    if (sender.getConsumerName() == null) {
      throw new RuntimeException(DB_QUEUE_CONSUMER_NAME_PROPERTY + " is not configured to get events from DB for sender " + senderKey);
    }
    String queueName = sender.getDatabaseQueueName();
    Optional<Long> batchId;
    int i = 0;
    while(i < maxBatchesToProcessInTx) {
      stageTimings.start();
      batchId = databaseQueueDao.getNextBatchId(queueName, sender.getConsumerName());
      if (batchId.isEmpty()) {
        return;
      }
      long batchIdValue = batchId.get();
      Map<SendHandle, ? extends Collection<MessageEventContainer>> events = getNextBatchEvents(batchIdValue, sender, maxEventsPerBatchToProcess);
      long eventsSize = events.values().stream().mapToLong(Collection::size).sum();
      LOGGER.debug("Prepared to send {} messages for batch {}", eventsSize, batchIdValue);
      events.forEach((sendHandle, messages) -> {
        var messageSender = persistentPublisherRegistry.getSender(sendHandle.destination.getSenderKey()).getMessageSender();
        var messageExtractor = Optional.ofNullable(sendHandle.destination.getCorrelationData()).map(
          correlationData -> (Function<MessageEventContainer, Object>) msg -> (Object) new CorrelatedMessage(correlationData, msg.getMessage())
        ).orElseGet(() -> MessageEventContainer::getMessage);
        LOGGER.trace("Starting to send batch for handle {}", sendHandle);
        sendMessages(sender, batchIdValue, sendHandle, messages, messageSender, messageExtractor);
        LOGGER.trace("Finished sending batch for {}", sendHandle);
      });
      stageTimings.markStage(SendingStage.SEND_BATCH_EVENTS_TO_RABBIT);
      databaseQueueDao.finishBatch(batchIdValue);
      stageTimings.markStage(SendingStage.FINISH_BATCH);
      batchSizeHistogram.save((int) eventsSize, new Tag("queueName", queueName));
      LOGGER.debug("Batch {}, containing {} events finished", batchIdValue, eventsSize);
      if (multiBatchOnlyForEmptyBatches && !events.isEmpty()) {
        return;
      }
      i++;
    }
    LOGGER.info("Processed {} batches in {}ms", i, System.currentTimeMillis() - sendStartMs);
  }

  private void sendMessages(
    DatabaseQueueSender sender,
    long batchId,
    SendHandle sendHandle,
    Collection<MessageEventContainer> messages,
    MessageSender messageSender,
    Function<MessageEventContainer, Object> messageExtractor
  ) {
    int msgCount = 0;
    for (MessageEventContainer message : messages) {
      try {
        messageSender.publishMessage(messageExtractor.apply(message), sendHandle.destination);
      } catch (RuntimeException e) {
        sender.onAmpqException(e, message.getId(), batchId, sendHandle.destination, message.getMessage());
      } finally {
        if (msgCount % 1000 == 0) {
          LOGGER.trace("Sent {} messages for handle {} in batch ID {}", msgCount, sendHandle, batchId);
        }
        msgCount++;
      }
    }
  }

  @Transactional
  public void retryEvent(DatabaseQueueSender databaseQueueSender, long eventId, long batchId, Duration retryEventDelay,
      TargetedDestination destination, Object message) {
    int result = databaseQueueDao.retryEvent(eventId, batchId, retryEventDelay.getSeconds());
    if (NEW_REGISTRATION != result) {
      LOGGER.error("Error scheduling event {} for retry", eventId);
      logErrorIfErrorTablePresent(databaseQueueSender, eventId, toDb(destination),
        JacksonDbQueueProcessor.INSTANCE.convertToDb(message)
      );
    }
  }

  @Transactional
  public void logErrorIfErrorTablePresent(DatabaseQueueSender databaseQueueSender, long eventId, String destination, String message) {
    if (databaseQueueSender.getErrorTableName().isEmpty()) {
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

  private Map<SendHandle, ? extends Collection<MessageEventContainer>> getNextBatchEvents(
    long batchId,
    DatabaseQueueSender sender,
    int maxEventsToProcess
  ) {
    LOGGER.trace("Getting next events batch for ID {}", batchId);
    List<Tuple> currentBatch = databaseQueueDao.getNextBatchEvents(batchId);
    stageTimings.markStage(SendingStage.GET_BATCH_EVENTS);
    LOGGER.trace("Got events from db for batch ID {}", batchId);
    Map<SendHandle, List<Tuple>> eventDataBySendHandle = currentBatch.stream().limit(maxEventsToProcess).collect(groupingBy(event -> {
      String type = event.get(2, String.class);
      try {
        TargetedDestination destination = DESTINATION_CONVERTER.readValue(type, TargetedDestination.class);
        return new SendHandle(sender.getConverter(destination.getConverterKey()), destination);
      } catch (RuntimeException | IOException e) {
        long eventId = event.get(0, Number.class).longValue();
        String data = event.get(1, String.class);
        sender.onConvertationException(e, eventId, data, type);
        return SendHandle.FAILED;
      }
    }, toList()));
    stageTimings.markStage(SendingStage.GROUP_BATCH_EVENTS);
    LOGGER.trace("Grouped events by sendHandle for batch ID {}", batchId);

    int size = currentBatch.size();
    if (size > maxEventsToProcess) {
      LOGGER.warn("In batch {} we have {} events and limit={}, the rest will be retried", batchId, size, maxEventsToProcess);
      currentBatch.subList(maxEventsToProcess, size).forEach(event -> {
        long eventId = event.get(0, Number.class).longValue();
        databaseQueueDao.retryEvent(eventId, batchId, sender.getRetryDuration().getSeconds());
      });
    }
    stageTimings.markStage(SendingStage.RETRY_OUT_OF_PROCESSING_LIMIT_EVENTS);
    Map<SendHandle, List<MessageEventContainer>> convertedEvents = eventDataBySendHandle.entrySet().stream()
      .filter(eventBatchBySendHandle -> !SendHandle.FAILED.equals(eventBatchBySendHandle.getKey()))
      .collect(toMap(Map.Entry::getKey, eventBatchBySendHandle -> convertMessagesForHandle(batchId, eventBatchBySendHandle)));
    stageTimings.markStage(SendingStage.CONVERT_EVENTS);
    return convertedEvents;
  }

  private List<MessageEventContainer> convertMessagesForHandle(long batchId, Map.Entry<SendHandle, List<Tuple>> eventBatchBySendHandle) {
    var sendHandle = eventBatchBySendHandle.getKey();
    LOGGER.trace("Starting to convert {} messages for sendHandle {} in batch ID {}",
      eventBatchBySendHandle.getValue().size(), sendHandle, batchId
    );
    var dataById = eventBatchBySendHandle.getValue().stream()
      .collect(toMap(tuple -> tuple.get(0, Number.class).longValue(), tuple -> tuple.get(1, String.class)));
    List<MessageEventContainer> result = sendHandle.converter.batchConvertFromDb(dataById, sendHandle.destination.getMsgClass()).entrySet()
      .stream()
      .map(entityById -> new MessageEventContainer(entityById.getKey(), entityById.getValue()))
      .collect(toList());
    LOGGER.trace("Converted {} messages for sendHandle {} in batch ID {}",
      eventBatchBySendHandle.getValue().size(), sendHandle, batchId
    );
    return result;
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

    private MessageEventContainer(long id, Object message) {
      this.id = id;
      this.message = message;
    }

    @Override
    public String toString() {
      return "MessageContainer{" + id + "}: message=" + message;
    }

    public long getId() {
      return id;
    }

    public Object getMessage() {
      return message;
    }
  }

  private static final class SendHandle {
    private static final SendHandle FAILED = new SendHandle(null, null);
    private final TargetedDestination destination;
    private final DbQueueProcessor converter;

    SendHandle(DbQueueProcessor converter, TargetedDestination destination) {
      this.destination = destination;
      this.converter = converter;
    }

    public DbQueueProcessor getConverter() {
      return converter;
    }

    public TargetedDestination getDestination() {
      return destination;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SendHandle that = (SendHandle) o;
      return Objects.equals(destination, that.destination);
    }

    @Override
    public int hashCode() {
      return Objects.hash(destination);
    }

    @Override
    public String toString() {
      return "SendHandle{" +
        "destination=" + destination +
        ", converter=" + converter +
        '}';
    }
  }

  enum SendingStage {
    GET_BATCH_EVENTS,
    GROUP_BATCH_EVENTS,
    RETRY_OUT_OF_PROCESSING_LIMIT_EVENTS,
    CONVERT_EVENTS,
    SEND_BATCH_EVENTS_TO_RABBIT,
    FINISH_BATCH
  }
}
