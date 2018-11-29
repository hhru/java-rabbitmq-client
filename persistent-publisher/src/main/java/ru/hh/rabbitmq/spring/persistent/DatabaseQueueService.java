package ru.hh.rabbitmq.spring.persistent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.persistence.ParameterMode;
import javax.persistence.StoredProcedureQuery;
import javax.persistence.Tuple;
import org.hibernate.SessionFactory;
import org.hibernate.type.IntegerType;
import org.hibernate.type.LongType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.transaction.annotation.Transactional;
import ru.hh.rabbitmq.spring.send.Destination;
import ru.hh.rabbitmq.spring.send.MessageSender;
import static java.lang.String.join;
import static java.util.stream.Collectors.toMap;
import static ru.hh.rabbitmq.spring.persistent.PersistentPublisherResource.PGQ_PUBLISH;
import static ru.hh.rabbitmq.spring.persistent.PersistentPublisherResource.RETRY_MS;

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
  public void registerHhInvokerJob(String serviceName, String upstreamName, String jerseyBasePath, Duration pollingInterval,
      Duration retryInterval) {
    sessionFactory.getCurrentSession()
      .createNativeQuery("INSERT INTO hh_invoker.task(name, description, enabled, schedule_type, repeat_interval_sec, " +
      "target_url, " +
      "use_disabled_period_during_day, launch_lock_timeout_sec, fallback_timeout_sec) " +
      " VALUES (:name, :description, :enabled, :scheduleType, :repeatIntervalSec, " +
      ":targetUrl, " +
      ":useDisabledPeriodDuringDay, :launchLockTimeoutSec, :fallbackTimeoutSec) ON CONFLICT (name) DO UPDATE SET " +
      "description = :description, enabled = :enabled, schedule_type = :scheduleType, repeat_interval_sec = :repeatIntervalSec, " +
      "target_url = :targetUrl, " +
      "use_disabled_period_during_day = :useDisabledPeriodDuringDay, launch_lock_timeout_sec = :launchLockTimeoutSec, " +
      "fallback_timeout_sec = :fallbackTimeoutSec")
      .setParameter("name", serviceName + "_publishJob")
      .setParameter("description", "job to publish rabbit messages from pgq")
      .setParameter("enabled", true)
      .setParameter("scheduleType", "COUNTER_STARTS_AFTER_TASK_FINISH")
      .setParameter("repeatIntervalSec", pollingInterval.getSeconds())
      .setParameter("targetUrl", "http://" + upstreamName + jerseyBasePath + PGQ_PUBLISH + '?'
        + join("=", RETRY_MS, String.valueOf(retryInterval.toMillis())))
      .setParameter("useDisabledPeriodDuringDay", false)
      .setParameter("launchLockTimeoutSec", pollingInterval.getSeconds() / 2)
      .setParameter("fallbackTimeoutSec", TimeUnit.MINUTES.toSeconds(pollingInterval.getSeconds()))
      .executeUpdate();
  }

  @Transactional
  public Long publish(Object message, TargetedDestination destination) throws JsonProcessingException {
    Object eventId = sessionFactory.getCurrentSession()
      .createNativeQuery("SELECT * FROM pgq.insert_event(:queueName, :destination, :message)")
      .setParameter("queueName", databaseQueueName)
      .setParameter("destination", objectMapper.writeValueAsString(destination))
      .setParameter("message", objectMapper.writeValueAsString(message)).getSingleResult();
    return ((Number) eventId).longValue();
  }

  @Transactional
  public void sendBatch(Duration retryEventDelay) {
    Optional<Long> batchId = getNextBatchId();
    if (!batchId.isPresent()) {
      LOGGER.info("No batchId is available. Just wait for next iteration");
      return;
    }
    List<IdentifiedRoutedMessage> events = getNextBatch(batchId.get());
    try {
      Map<Object, TargetedDestination> toSend = events.stream()
        .collect(toMap(IdentifiedRoutedMessage::getMessage, IdentifiedRoutedMessage::getDestination));
      messageSender.publishMessages(toSend);
    } catch (AmqpException e) {
      LOGGER.warn("Error on publishing events of batch {}, setting retry", batchId.get(), e);
      retryEvents(events, batchId.get(), retryEventDelay);
    }
    finishBatch(batchId.get());
  }

  @Transactional
  public boolean registerConsumerIfPossible() {
    registerQueueIfPossible();
    LOGGER.debug("Registering same name PGQ consumer for queue: {}", databaseQueueName);
    StoredProcedureQuery storedProcedureQuery = sessionFactory.getCurrentSession().createStoredProcedureQuery("pgq.register_consumer")
      .registerStoredProcedureParameter("queueName", String.class, ParameterMode.IN)
      .registerStoredProcedureParameter("consumerName", String.class, ParameterMode.IN)
      .registerStoredProcedureParameter("result", int.class, ParameterMode.OUT)
      .setParameter("queueName", databaseQueueName)
      .setParameter("consumerName", databaseQueueName);
    storedProcedureQuery.execute();
    Integer result = (Integer) storedProcedureQuery.getOutputParameterValue("result");
    boolean newRegistration = NEW_REGISTRATION == result;
    if (newRegistration) {
      LOGGER.info("PGQ consumer for queue {} registered successfully for the first time", databaseQueueName);
    } else {
      LOGGER.info("PGQ consumer for queue {} was already registered", databaseQueueName);
    }
    return newRegistration;
  }

  @Transactional
  public boolean registerQueueIfPossible() {
    LOGGER.debug("Registering queue: {}", databaseQueueName);
    StoredProcedureQuery storedProcedureQuery = sessionFactory.getCurrentSession().createStoredProcedureQuery("pgq.create_queue")
      .registerStoredProcedureParameter("queueName", String.class, ParameterMode.IN)
      .registerStoredProcedureParameter("result", int.class, ParameterMode.OUT)
      .setParameter("queueName", databaseQueueName);
    storedProcedureQuery.execute();
    Integer result = (Integer) storedProcedureQuery.getOutputParameterValue("result");
    boolean newRegistration = NEW_REGISTRATION == result;
    if (newRegistration) {
      LOGGER.info("PGQ queue {} registered successfully for the first time", databaseQueueName);
    } else {
      LOGGER.info("PGQ queue {} was already registered", databaseQueueName);
    }
    return newRegistration;
  }

  @Transactional
  public boolean isQueueRegistered() {
    Optional<?> queueName = sessionFactory.getCurrentSession().createNativeQuery("SELECT queue_name FROM pgq.get_queue_info(:queueName)")
      .setParameter("queueName", databaseQueueName).uniqueResultOptional();
    return queueName.isPresent();
  }

  @Transactional
  public boolean isConsumerRegistered() {
    Optional<?> consumerName = sessionFactory.getCurrentSession()
      .createNativeQuery("SELECT consumer_name FROM pgq.get_consumer_info(:consumerName)")
      .setParameter("consumerName", databaseQueueName).uniqueResultOptional();
    return consumerName.isPresent();
  }

  @Transactional
  public boolean isAllRegistered() {
    return isQueueRegistered() && isConsumerRegistered();
  }

  private void retryEvents(List<IdentifiedRoutedMessage> events, long batchId, Duration retryEventDelay) {
    events.forEach(event -> {
      int result = ((Number)sessionFactory.getCurrentSession()
        .createNativeQuery("SELECT * FROM pgq.event_retry(:batchId, :eventId, :retryEventSec)")
        .setParameter("batchId", batchId, LongType.INSTANCE)
        .setParameter("eventId", event.getId(), LongType.INSTANCE)
        .setParameter("retryEventSec", Math.toIntExact(retryEventDelay.getSeconds()), IntegerType.INSTANCE).getSingleResult()).intValue();
      if (NEW_REGISTRATION != result) {
        LOGGER.error("Error scheduling event {} for retry", event.getId());
      }
    });
  }

  private List<IdentifiedRoutedMessage> getNextBatch(long batchId) {
    LOGGER.debug("Getting next events batch for ID {}", batchId);
    return sessionFactory.getCurrentSession()
      .createNativeQuery("SELECT ev_id, ev_data, ev_type FROM pgq.get_batch_events(:batchId)", Tuple.class)
      .setParameter("batchId", batchId, LongType.INSTANCE)
      .getResultList()
      .stream().map(row -> {
        long id = row.get(0, Long.class);
        String data = row.get(1, String.class);
        String type = row.get(2, String.class);
        try {
          return new IdentifiedRoutedMessage(id, objectMapper, data, type);
        } catch (IOException e) {
          LOGGER.warn("Exception on extracting event: data={}, type={}", data, type, e);
          return null;
        }
      }).filter(Objects::nonNull).collect(Collectors.toList());
  }

  private int finishBatch(long batchId) {
    LOGGER.debug("Finishing batch ID {}", batchId);
    int result = ((Number) sessionFactory.getCurrentSession().createNativeQuery("SELECT * FROM pgq.finish_batch(:batchId)")
      .setParameter("batchId", batchId)
      .getSingleResult()).intValue();
    return result;
  }

  private Optional<Long> getNextBatchId() {
    Optional<?> batchId = sessionFactory.getCurrentSession().createNativeQuery("SELECT pgq.next_batch(:queueName, :consumerName)")
      .setParameter("queueName", databaseQueueName)
      .setParameter("consumerName", databaseQueueName).uniqueResultOptional();
    return batchId.map(id -> ((Number) id).longValue());
  }

  private static final class IdentifiedRoutedMessage {
    private final long id;
    private final Object message;
    private final TargetedDestination destination;

    private IdentifiedRoutedMessage(long id, ObjectMapper objectMapper, String data, String type) throws IOException {
      this.id = id;
      destination = objectMapper.readValue(type, TargetedDestination.class);
      message = objectMapper.readValue(data, destination.msgClass);
    }

    @Override
    public String toString() {
      return "IdentifiedRoutedMessage{" + id + "}: message=" + message + ", destination=" + destination;
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

  public static class TargetedDestination extends Destination {

    private Class<?> msgClass;
    private String rabbitTemplateKey;

    public TargetedDestination(String exchange, String routingKey, Class<?> msgClass, String rabbitTemplateKey) {
      super(exchange, routingKey);
      this.msgClass = msgClass;
      this.rabbitTemplateKey = rabbitTemplateKey;
    }

    public TargetedDestination() {
    }

    public Class<?> getMsgClass() {
      return msgClass;
    }

    public String getRabbitTemplateKey() {
      return rabbitTemplateKey;
    }

    public void setMsgClass(Class<?> msgClass) {
      this.msgClass = msgClass;
    }

    public void setRabbitTemplateKey(String rabbitTemplateKey) {
      this.rabbitTemplateKey = rabbitTemplateKey;
    }

    @Override
    public String toString() {
      return "TargetedDestination{" +
        "msgClass=" + msgClass +
        ", rabbitTemplateKey='" + rabbitTemplateKey + '\'' + ",inherited=" +super.toString() +
        '}';
    }
  }

  public static class CorrelatedTargetedDestination extends TargetedDestination {
    private CorrelationData correlationData;

    public CorrelatedTargetedDestination(String exchange, String routingKey, Class<?> msgClass, String rabbitTemplateKey,
        CorrelationData correlationData) {
      super(exchange, routingKey, msgClass, rabbitTemplateKey);
      this.correlationData = correlationData;
    }

    public CorrelationData getCorrelationData() {
      return correlationData;
    }

    public void setCorrelationData(CorrelationData correlationData) {
      this.correlationData = correlationData;
    }
  }
}
