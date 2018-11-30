package ru.hh.rabbitmq.spring.persistent;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.persistence.ParameterMode;
import javax.persistence.StoredProcedureQuery;
import javax.persistence.Tuple;
import org.hibernate.SessionFactory;
import org.hibernate.type.IntegerType;
import org.hibernate.type.LongType;
import static java.lang.String.join;
import static ru.hh.rabbitmq.spring.persistent.PersistentPublisherResource.DATABASE_QUEUE_RABBIT_PUBLISH;
import static ru.hh.rabbitmq.spring.persistent.PersistentPublisherResource.SENDER_KEY;

public class DatabaseQueueDao {

  private final SessionFactory sessionFactory;

  public DatabaseQueueDao(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  public void registerOrUpdateHhInvokerJob(String publisherKey, String upstreamName, String jerseyBasePath, Duration pollingInterval) {
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
      .setParameter("name", publisherKey + ":rabbit publication job")
      .setParameter("description", "job to publish rabbit messages from pgq")
      .setParameter("enabled", true)
      .setParameter("scheduleType", "COUNTER_STARTS_AFTER_TASK_FINISH")
      .setParameter("repeatIntervalSec", pollingInterval.getSeconds())
      .setParameter("targetUrl", "http://" + upstreamName + jerseyBasePath + DATABASE_QUEUE_RABBIT_PUBLISH + '?'
        + join("=", SENDER_KEY, publisherKey))
      .setParameter("useDisabledPeriodDuringDay", false)
      .setParameter("launchLockTimeoutSec", pollingInterval.getSeconds() / 2)
      .setParameter("fallbackTimeoutSec", TimeUnit.MINUTES.toSeconds(pollingInterval.getSeconds()))
      .executeUpdate();
  }

  public Long publish(String queueName, String type, String data) {
    Object eventId = sessionFactory.getCurrentSession()
      .createNativeQuery("SELECT * FROM pgq.insert_event(:queueName, :destination, :message)")
      .setParameter("queueName", queueName)
      .setParameter("destination", type)
      .setParameter("message", data).getSingleResult();
    return ((Number) eventId).longValue();
  }

  public Integer registerQueue(String queueName) {
    StoredProcedureQuery storedProcedureQuery = sessionFactory.getCurrentSession().createStoredProcedureQuery("pgq.create_queue")
      .registerStoredProcedureParameter("queueName", String.class, ParameterMode.IN)
      .registerStoredProcedureParameter("result", int.class, ParameterMode.OUT)
      .setParameter("queueName", queueName);
    storedProcedureQuery.execute();
    return (Integer) storedProcedureQuery.getOutputParameterValue("result");
  }

  public Integer registerConsumer(String queueName, String consumerName) {
    StoredProcedureQuery storedProcedureQuery = sessionFactory.getCurrentSession().createStoredProcedureQuery("pgq.register_consumer")
      .registerStoredProcedureParameter("queueName", String.class, ParameterMode.IN)
      .registerStoredProcedureParameter("consumerName", String.class, ParameterMode.IN)
      .registerStoredProcedureParameter("result", int.class, ParameterMode.OUT)
      .setParameter("queueName", queueName)
      .setParameter("consumerName", consumerName);
    storedProcedureQuery.execute();
    return (Integer) storedProcedureQuery.getOutputParameterValue("result");
  }

  @SuppressWarnings("unchecked")
  public Optional<String> getQueueInfo(String queueName) {
    return sessionFactory.getCurrentSession().createNativeQuery("SELECT queue_name FROM pgq.get_queue_info(:queueName)")
      .setParameter("queueName", queueName).uniqueResultOptional();
  }

  @SuppressWarnings("unchecked")
  public Optional<String> getConsumerInfo(String consumerName) {
    return sessionFactory.getCurrentSession()
      .createNativeQuery("SELECT consumer_name FROM pgq.get_consumer_info(:consumerName)")
      .setParameter("consumerName", consumerName).uniqueResultOptional();
  }

  public Optional<Long> getNextBatchId(String queueName, String consumerName) {
    Optional<?> batchId = sessionFactory.getCurrentSession().createNativeQuery("SELECT pgq.next_batch(:queueName, :consumerName)")
      .setParameter("queueName", queueName)
      .setParameter("consumerName", consumerName).uniqueResultOptional();
    return batchId.map(id -> ((Number) id).longValue());
  }

  public int finishBatch(long batchId) {
    return ((Number) sessionFactory.getCurrentSession().createNativeQuery("SELECT * FROM pgq.finish_batch(:batchId)")
      .setParameter("batchId", batchId)
      .getSingleResult()).intValue();
  }

  public List<Tuple> getNextBatchEvents(long batchId) {
    return sessionFactory.getCurrentSession()
      .createNativeQuery("SELECT ev_id, ev_data, ev_type FROM pgq.get_batch_events(:batchId)", Tuple.class)
      .setParameter("batchId", batchId, LongType.INSTANCE)
      .getResultList();
  }

  public int retryEvent(long eventId, long batchId, int retryEventDelaySec) {
    return ((Number)sessionFactory.getCurrentSession()
      .createNativeQuery("SELECT * FROM pgq.event_retry(:batchId, :eventId, :retryEventSec)")
      .setParameter("batchId", batchId, LongType.INSTANCE)
      .setParameter("eventId", eventId, LongType.INSTANCE)
      .setParameter("retryEventSec", retryEventDelaySec, IntegerType.INSTANCE).getSingleResult()).intValue();
  }
}
