package ru.hh.rabbitmq.spring.send;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.time.Duration;
import org.springframework.context.Lifecycle;
import ru.hh.rabbitmq.spring.DatabaseQueueService;
import ru.hh.rabbitmq.spring.simple.SimpleMessage;

public class PersistentPublisher implements Lifecycle {
  private final DatabaseQueueService databaseQueueService;
  private final String pgqQueueName;
  private final String upstreamName;
  private final Duration pollingInterval;
  private final Duration retryEventDelay;

  public PersistentPublisher(DatabaseQueueService databaseQueueService, String serviceName, String upstreamName, Duration retryEventDelay,
      Duration pollingInterval) {
    this.databaseQueueService = databaseQueueService;
    pgqQueueName = serviceName;
    this.upstreamName = upstreamName;
    this.retryEventDelay = retryEventDelay;
    this.pollingInterval = pollingInterval;
  }

  public void send(SimpleMessage simpleMessage, Destination destination) throws JsonProcessingException {
    databaseQueueService.publish(simpleMessage, destination);
  }

  @Override
  public void start() {
    databaseQueueService.registerConsumerIfPossible();
    databaseQueueService.registerHhInvokerJob(pgqQueueName, upstreamName, pollingInterval, retryEventDelay);
  }

  @Override
  public void stop() { }

  @Override
  public boolean isRunning() {
    return !databaseQueueService.registerConsumerIfPossible();
  }
}
