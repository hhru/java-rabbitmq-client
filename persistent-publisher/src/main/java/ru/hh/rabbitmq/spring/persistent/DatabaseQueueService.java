package ru.hh.rabbitmq.spring.persistent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.PostConstruct;
import javax.persistence.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;
import ru.hh.metrics.timinglogger.StageTimings;
import ru.hh.nab.metrics.Histograms;
import ru.hh.nab.metrics.StatsDSender;
import ru.hh.nab.metrics.Tag;
import ru.hh.rabbitmq.spring.persistent.dto.TargetedDestination;
import ru.hh.rabbitmq.spring.send.CorrelatedMessage;
import ru.hh.rabbitmq.spring.send.MessageSender;
import static java.util.stream.Collectors.toList;
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
                              int batchSizeHistogramSize, int stageHistogramsSize,
                              long statsSendIntervalMs) {
    this.databaseQueueDao = databaseQueueDao;
    this.persistentPublisherRegistry = persistentPublisherRegistry;
    this.statsDSender = statsDSender;
    this.batchSizeHistogram = new Histograms(batchSizeHistogramSize, persistentPublisherRegistry.numberOfSenders());
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
      List<MessageEventContainer> events = getNextBatchEvents(batchIdValue, sender, maxEventsPerBatchToProcess);
      stageTimings.markStage(SendingStage.GET_BATCH_EVENTS);
      events.forEach(messageEventContainer -> {
        TargetedDestination destination = messageEventContainer.getDestination();
        try {
          Object message = Optional.ofNullable(destination.getCorrelationData())
            .map(correlationData -> (Object) new CorrelatedMessage(correlationData, messageEventContainer.getMessage()))
            .orElseGet(messageEventContainer::getMessage);
          MessageSender messageSender = persistentPublisherRegistry.getSender(destination.getSenderKey()).getMessageSender();
          messageSender.publishMessage(message, destination);
        } catch (Exception e) {
          sender.onAmpqException(e, messageEventContainer.getId(), batchIdValue, destination, messageEventContainer.getMessage());
        }
      });
      stageTimings.markStage(SendingStage.SEND_BATCH_EVENTS_TO_RABBIT);
      databaseQueueDao.finishBatch(batchIdValue);
      stageTimings.markStage(SendingStage.FINISH_BATCH);
      batchSizeHistogram.save(events.size(), new Tag("queueName", queueName));
      LOGGER.debug("Batch {}, containing {} events finished", batchIdValue, events.size());
      if (multiBatchOnlyForEmptyBatches && !events.isEmpty()) {
        return;
      }
      i++;
    }
    LOGGER.info("Processed {} batches in {}ms", i, System.currentTimeMillis() - sendStartMs);
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

  private List<MessageEventContainer> getNextBatchEvents(long batchId, DatabaseQueueSender sender, int maxEventsToProcess) {
    LOGGER.debug("Getting next events batch for ID {}", batchId);
    List<Tuple> currentBatch = databaseQueueDao.getNextBatchEvents(batchId);
    List<MessageEventContainer> readyToSend = currentBatch.stream().limit(maxEventsToProcess).map(event -> {
      long eventId = event.get(0, Number.class).longValue();
      String data = event.get(1, String.class);
      String type = event.get(2, String.class);
      try {
        TargetedDestination destination = DESTINATION_CONVERTER.readValue(type, TargetedDestination.class);
        return new MessageEventContainer(eventId, destination, sender.getConverter(destination.getConverterKey()), data);
      } catch (Exception e) {
        return sender.onConvertationException(e, eventId, data, type);
      }
    }).filter(Objects::nonNull).collect(toList());

    int size = currentBatch.size();
    if (size > maxEventsToProcess) {
      LOGGER.warn("In batch {} we have {} events and limit={}, the rest will be retried", batchId, size, maxEventsToProcess);
      currentBatch.subList(maxEventsToProcess, size).forEach(event -> {
        long eventId = event.get(0, Number.class).longValue();
        databaseQueueDao.retryEvent(eventId, batchId, sender.getRetryDuration().getSeconds());
      });
    }
    return readyToSend;
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

    private MessageEventContainer(long id, TargetedDestination destination, DbQueueProcessor dbQueueProcessor, String data) {
      this.id = id;
      this.destination = destination;
      message = dbQueueProcessor.convertFromDb(data, destination.getMsgClass());
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

  enum SendingStage {
    GET_BATCH_EVENTS,
    SEND_BATCH_EVENTS_TO_RABBIT,
    FINISH_BATCH
  }
}
