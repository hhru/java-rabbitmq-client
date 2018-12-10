package ru.hh.rabbitmq.spring.persistent;

import java.util.List;
import java.util.Optional;
import javax.persistence.ParameterMode;
import javax.persistence.StoredProcedureQuery;
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
