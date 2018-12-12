package ru.hh.rabbitmq.spring.persistent;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import javax.persistence.Tuple;
import org.hibernate.SessionFactory;
import org.hibernate.type.IntegerType;
import org.hibernate.type.LongType;

public class DatabaseQueueDao {

  private final SessionFactory sessionFactory;

  public DatabaseQueueDao(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  public Long publish(String queueName, String type, String data) {
    Object eventId = sessionFactory.getCurrentSession()
      .createNativeQuery("SELECT * FROM pgq.insert_event(:queueName, :destination, :message)")
      .setParameter("queueName", queueName)
      .setParameter("destination", type)
      .setParameter("message", data).getSingleResult();
    return ((Number) eventId).longValue();
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

  public int retryEvent(long eventId, long batchId, long retryEventDelaySec) {
    return ((Number)sessionFactory.getCurrentSession()
      .createNativeQuery("SELECT * FROM pgq.event_retry(:batchId, :eventId, :retryEventSec)")
      .setParameter("batchId", batchId, LongType.INSTANCE)
      .setParameter("eventId", eventId, LongType.INSTANCE)
      .setParameter("retryEventSec", retryEventDelaySec, IntegerType.INSTANCE).getSingleResult()).intValue();
  }

  public void saveError(String tableName, LocalDateTime timestamp, long eventId, String queueName, String consumerName,
      String destinationContent, String msgContent) {
    sessionFactory.getCurrentSession().createNativeQuery("INSERT INTO " + tableName + "(event_id, log_date, " +
      "queue_name, consumer_name, " +
      "destination_content, message_content) VALUES (:eventId, :logDate, :queueName, :consumerName, :destinationContent, :msgContent)")
      .setParameter("eventId", eventId)
      .setParameter("logDate", timestamp)
      .setParameter("queueName", queueName)
      .setParameter("consumerName", consumerName)
      .setParameter("destinationContent", destinationContent)
      .setParameter("msgContent", msgContent)
      .executeUpdate();
  }
}
