package ru.hh.rabbitmq.spring;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import ru.hh.rabbitmq.spring.send.Destination;
import ru.hh.rabbitmq.spring.send.MessageSender;
import ru.hh.rabbitmq.spring.simple.SimpleMessage;
import static java.lang.String.join;
import static java.util.stream.Collectors.toMap;
import static ru.hh.rabbitmq.spring.PersistentPublisherResource.PGQ_PUBLISH;
import static ru.hh.rabbitmq.spring.PersistentPublisherResource.RETRY_MS;

public class PgqService {

  private static final Logger LOGGER = LoggerFactory.getLogger(PgqService.class);
  private static final int NEW_REGISTRATION = 1;

  private final String pgqQueueName;
  private final MessageSender messageSender;
  private final ObjectMapper objectMapper;
  private final JdbcTemplate jdbcTemplate;

  public PgqService(String pgqQueueName, MessageSender messageSender, ObjectMapper objectMapper, JdbcTemplate jdbcTemplate) {
    this.pgqQueueName = pgqQueueName;
    this.messageSender = messageSender;
    this.objectMapper = objectMapper;
    this.jdbcTemplate = jdbcTemplate;
  }

  //TODO http API to register job
  @Transactional
  public void registerHhInvokerJob(String serviceName, String upstreamName, Duration pollingInterval, Duration retryInterval) {
    jdbcTemplate.update("INSERT INTO hh_invoker.task(name, description, enabled, schedule_type, repeat_interval_sec, " +
      "target_url, " +
      "use_disabled_period_during_day, launch_lock_timeout_sec, fallback_timeout_sec)\n" +
      "VALUES (?, ?, ?, ?, ?, " +
      "?, " +
      "?, ?, ?) ON CONFLICT DO NOTHING",
      serviceName + "_publishJob", "job to publish rabbit messages from pgq", true, "COUNTER_STARTS_AFTER_TASK_FINISH", pollingInterval.getSeconds(),
      "http://" + upstreamName + PGQ_PUBLISH + '?' + join("=", RETRY_MS, String.valueOf(retryInterval.toMillis())),
      false, pollingInterval.getSeconds() / 2, 60 * pollingInterval.getSeconds()
    );
  }

  @Transactional
  public void publish(SimpleMessage message, Destination destination) throws JsonProcessingException {
    String sql = "SELECT pg.insert_event(?, ?, ?)";
    Long eventId = jdbcTemplate.queryForObject(sql, Long.class, pgqQueueName,
      objectMapper.writeValueAsString(message), objectMapper.writeValueAsString(destination));
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
    LOGGER.debug("Registering same name PGQ consumer for queue: {}", pgqQueueName);
    String sql = "SELECT pgq.register_consumer(?, ?)";
    Integer registrationResult = jdbcTemplate.queryForObject(sql, Integer.class, pgqQueueName, pgqQueueName);
    boolean newRegistration = NEW_REGISTRATION == registrationResult;
    if (newRegistration) {
      LOGGER.info("PGQ consumer for queue {} registered successfully for the first time", pgqQueueName);
    } else {
      LOGGER.info("PGQ consumer for queue {} was already registered", pgqQueueName);
    }
    return newRegistration;
  }

  private void retryEvents(List<IdentifiedRoutedMessage> events, long batchId, Duration retryEventDelay) {
    events.forEach(event -> {
      String sql = "SELECT * FROM pgq.event_retry(?, ?, ?)";
      Integer result = jdbcTemplate.queryForObject(sql, Integer.class, event.getId(), batchId, retryEventDelay.getSeconds());
      if (NEW_REGISTRATION != result) {
        LOGGER.error("Error scheduling event {} for retry", event.getId());
      }
    });
  }

  private List<IdentifiedRoutedMessage> getNextBatch(long batchId) {
    LOGGER.debug("Getting next events batch for ID {}", batchId);

    String sql = "SELECT * FROM pgq.get_batch_events(?);";
    return jdbcTemplate.query(sql, rs -> {
      List<IdentifiedRoutedMessage> result = new ArrayList<>();
      while (rs.next()) {
        long id = rs.getLong("ev_id");
        String data = rs.getString("ev_data");
        String type = rs.getString("ev_type");
        try {
          result.add(new IdentifiedRoutedMessage(id, objectMapper, data, type));
        } catch (IOException e) {
          LOGGER.warn("Exception on extracting event: data={}, type={}", data, type, e);
        }
      }
      return result;
    }, batchId);
  }

  private int finishBatch(long batchId) {
    LOGGER.debug("Finishing batch ID {}", batchId);
    String sql = "SELECT pgq.finish_batch(?)";
    return jdbcTemplate.queryForObject(sql, Integer.class, batchId);
  }

  private Long getNextBatchId() {
    String sql = "SELECT pgq.next_batch(?, ?)";
    return jdbcTemplate.queryForObject(sql, Long.class, pgqQueueName, pgqQueueName);
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
