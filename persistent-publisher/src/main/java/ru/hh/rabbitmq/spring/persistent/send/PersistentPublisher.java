package ru.hh.rabbitmq.spring.persistent.send;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.time.Duration;
import org.springframework.context.SmartLifecycle;
import ru.hh.rabbitmq.spring.persistent.DatabaseQueueService;

public class PersistentPublisher implements SmartLifecycle {
  private final DatabaseQueueService databaseQueueService;
  private final String pgqQueueName;
  private final String upstreamName;
  private final String jerseyBasePath;
  private final Duration pollingInterval;
  private final Duration retryEventDelay;

  public PersistentPublisher(DatabaseQueueService databaseQueueService, String serviceName, String upstreamName,
      String jerseyBasePath, Duration retryEventDelay, Duration pollingInterval) {
    this.databaseQueueService = databaseQueueService;
    pgqQueueName = serviceName;
    this.upstreamName = upstreamName;
    this.jerseyBasePath = jerseyBasePath;
    this.retryEventDelay = retryEventDelay;
    this.pollingInterval = pollingInterval;
  }

  public void send(Object simpleMessage, DatabaseQueueService.TargetedDestination destination) throws JsonProcessingException {
    databaseQueueService.publish(simpleMessage, destination);
  }

  @Override
  public void start() {
    databaseQueueService.registerConsumerIfPossible();
  }

  @Override
  public void stop() { }

  @Override
  public boolean isRunning() {
    databaseQueueService.registerHhInvokerJob(pgqQueueName, upstreamName, jerseyBasePath, pollingInterval, retryEventDelay);
    return databaseQueueService.isAllRegistered();
  }

  @Override
  public boolean isAutoStartup() {
    return true;
  }

  @Override
  public void stop(Runnable callback) {
    stop();
    callback.run();
  }

  @Override
  public int getPhase() {
    return Integer.MAX_VALUE;
  }
}
