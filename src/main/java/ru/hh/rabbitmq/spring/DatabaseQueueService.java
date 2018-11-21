package ru.hh.rabbitmq.spring;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.hibernate.SessionFactory;
import org.hibernate.transform.ResultTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.transaction.annotation.Transactional;
import ru.hh.rabbitmq.spring.send.Destination;
import ru.hh.rabbitmq.spring.send.MessageSender;
import ru.hh.rabbitmq.spring.simple.SimpleMessage;
import static java.lang.String.join;
import static java.util.stream.Collectors.toMap;
import static ru.hh.rabbitmq.spring.PersistentPublisherResource.PGQ_PUBLISH;
import static ru.hh.rabbitmq.spring.PersistentPublisherResource.RETRY_MS;

public class DatabaseQueueService {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseQueueService.class);
  private static final int NEW_REGISTRATION = 1;

  private final String databaseQueueName;
  private final MessageSender messageSender;
  private final ObjectMapper objectMapper;
  private final SessionFactory sessionFactory;

  public DatabaseQueueService(String databaseQueueName, MessageSender messageSender, SessionFactory sessionFactory, ObjectMapper objectMapper) {
    this.databaseQueueName = databaseQueueName;
    this.messageSender = messageSender;
    this.objectMapper = objectMapper;
    this.sessionFactory = sessionFactory;
  }

  //TODO http API to register job
  @Transactional
  public void registerHhInvokerJob(String serviceName, String upstreamName, Duration pollingInterval, Duration retryInterval) {
    sessionFactory.getCurrentSession().createQuery("INSERT INTO hh_invoker.task(name, description, enabled, schedule_type, repeat_interval_sec, " +
      "target_url, " +
      "use_disabled_period_during_day, launch_lock_timeout_sec, fallback_timeout_sec)\n" +
      "VALUES (:name, :description, :enabled, :scheduleType, :repeatIntervalSec, " +
      ":targetUrl, " +
      ":useDisabledPeriodDuringDay, :launchLockTimeoutSec, :fallbackTimeoutSec) ON CONFLICT DO NOTHING")
      .setParameter("name", serviceName + "_publishJob")
      .setParameter("description", "job to publish rabbit messages from pgq")
      .setParameter("enabled", true)
      .setParameter("scheduleType", "COUNTER_STARTS_AFTER_TASK_FINISH")
      .setParameter("repeatIntervalSec", pollingInterval.getSeconds())
      .setParameter("targetUrl", "http://" + upstreamName + PGQ_PUBLISH + '?' + join("=", RETRY_MS, String.valueOf(retryInterval.toMillis())))
      .setParameter("useDisabledPeriodDuringDay", false)
      .setParameter("launchLockTimeoutSec", pollingInterval.getSeconds() / 2)
      .setParameter("fallbackTimeoutSec", TimeUnit.MINUTES.toSeconds(pollingInterval.getSeconds()))
      .executeUpdate();
  }

  @Transactional
  public void publish(SimpleMessage message, Destination destination) throws JsonProcessingException {
    Long eventId = sessionFactory.getCurrentSession().createQuery("SELECT pg.insert_event(:queue_name, :destination, :message)", Long.class)
      .setParameter("queue_name", databaseQueueName)
      .setParameter("destination", objectMapper.writeValueAsString(destination))
      .setParameter("message", objectMapper.writeValueAsString(message)).uniqueResult();
  }

  @Transactional
  public void sendBatch(Duration retryEventDelay) {
    Long batchId = getNextBatchId();
    if (batchId == null) {
      LOGGER.info("No batchId is available. Just wait for next iteration");
    }
    if (batchId != null) {
      List<IdentifiedRoutedMessage> events = getNextBatch(batchId);
      try {
        Map<SimpleMessage, Destination> toSend = events.stream()
          .collect(toMap(IdentifiedRoutedMessage::getSimpleMessage, IdentifiedRoutedMessage::getDestination));
        messageSender.publishMessages(toSend);
      } catch (AmqpException e) {
        LOGGER.warn("Error on publishing events of batch {}, setting retry", batchId, e);
        retryEvents(events, batchId, retryEventDelay);
      }
      finishBatch(batchId);
    }
  }

  @Transactional
  public boolean registerConsumerIfPossible() {
    LOGGER.debug("Registering same name PGQ consumer for queue: {}", databaseQueueName);
    Integer result = sessionFactory.getCurrentSession().createQuery("SELECT pgq.register_consumer(:queueName, :consumerName)", Integer.class)
      .setParameter("queueName", databaseQueueName)
      .setParameter("consumerName", databaseQueueName)
      .getSingleResult();
    boolean newRegistration = NEW_REGISTRATION == result;
    if (newRegistration) {
      LOGGER.info("PGQ consumer for queue {} registered successfully for the first time", databaseQueueName);
    } else {
      LOGGER.info("PGQ consumer for queue {} was already registered", databaseQueueName);
    }
    return newRegistration;
  }

  private void retryEvents(List<IdentifiedRoutedMessage> events, long batchId, Duration retryEventDelay) {
    events.forEach(event -> {
      Integer result = sessionFactory.getCurrentSession()
        .createQuery("SELECT * FROM pgq.event_retry(:batchId, :eventId, :retryEventSec)", Integer.class)
        .setParameter("batchId", batchId).setParameter("eventId", event.getId()).setParameter("retryEventSec", retryEventDelay.getSeconds())
        .getSingleResult();
      if (NEW_REGISTRATION != result) {
        LOGGER.error("Error scheduling event {} for retry", event.getId());
      }
    });
  }

  private List<IdentifiedRoutedMessage> getNextBatch(long batchId) {
    LOGGER.debug("Getting next events batch for ID {}", batchId);
    return sessionFactory.getCurrentSession()
      .createQuery("SELECT ev_id, ev_data, ev_type FROM pgq.get_batch_events(:batchId)", IdentifiedRoutedMessage.class).setResultTransformer(new ResultTransformer() {
      @Override
      public Object transformTuple(Object[] tuple, String[] aliases) {
        long id = (long) tuple[0];
        String data = (String) tuple[1];
        String type = (String) tuple[2];
        try {
          return new IdentifiedRoutedMessage(id, objectMapper, data, type);
        } catch (IOException e) {
          LOGGER.warn("Exception on extracting event: data={}, type={}", data, type, e);
          return null;
        }
      }

      @Override
      public List<Object> transformList(List collection) {
        return (List<Object>) collection.stream().filter(Objects::nonNull).collect(Collectors.toList());
      }
    }).setParameter("batchId", batchId).getResultList();
  }

  private int finishBatch(long batchId) {
    LOGGER.debug("Finishing batch ID {}", batchId);
    return sessionFactory.getCurrentSession().createQuery("SELECT pgq.finish_batch(:batchId)", Integer.class)
      .setParameter("batchId", batchId).getSingleResult();
  }

  private Long getNextBatchId() {
    return sessionFactory.getCurrentSession().createQuery("SELECT pgq.next_batch(:queueName, :consumerName)", Long.class)
      .setParameter("queueName", databaseQueueName)
      .setParameter("consumerName", databaseQueueName)
      .getSingleResult();
  }

  private static final class IdentifiedRoutedMessage {
    private final long id;
    private final SimpleMessage simpleMessage;
    private final Destination destination;

    private IdentifiedRoutedMessage(long id, ObjectMapper objectMapper, String data, String type) throws IOException {
      this.id = id;
      simpleMessage = objectMapper.readValue(data, SimpleMessage.class);
      destination = objectMapper.readValue(type, Destination.class);
    }

    @Override
    public String toString() {
      return "IdentifiedRoutedMessage{" + id + "}: simpleMessage=" + simpleMessage + ", destination=" + destination;
    }

    public long getId() {
      return id;
    }

    public SimpleMessage getSimpleMessage() {
      return simpleMessage;
    }

    public Destination getDestination() {
      return destination;
    }
  }
}
