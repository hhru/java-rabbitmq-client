package ru.hh.rabbitmq.spring;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.MDC;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ConfirmCallback;
import org.springframework.amqp.rabbit.support.CorrelationData;
import ru.hh.rabbitmq.spring.receive.MapMessageListener;
import ru.hh.rabbitmq.spring.send.SyncPublisherBuilder;

public abstract class SyncRabbitIntegrationTestBase extends RabbitIntegrationTestBase {

  public static final long TIMEOUT_MILLIS = 5000;

  protected static class MessageHandler implements MapMessageListener {
    private final ArrayBlockingQueue<Map<String, Object>> queue = new ArrayBlockingQueue<>(100);
    private final ArrayBlockingQueue<Map<String, String>> mdcContextQueue = new ArrayBlockingQueue<>(100);

    private final boolean useMDC;

    public MessageHandler() {
      this.useMDC = false;
    }

    public MessageHandler(boolean useMDC) {
      this.useMDC = useMDC;
    }

    @Override
    public void handleMessage(Map<String, Object> data) {
      queue.add(data);
      if (useMDC) {
        mdcContextQueue.add(MDC.getCopyOfContextMap());
      }
    }

    public Map<String, Object> get() throws InterruptedException {
      return queue.poll(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }

    public Map<String, String> getMDC() throws InterruptedException {
      return mdcContextQueue.poll(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }
  }

  protected static class TestConfirmCallback implements ConfirmCallback {
    private final ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(3);

    @Override
    public void confirm(CorrelationData correlationData, boolean ack, String cause) {
      queue.add(correlationData.getId());
    }

    public String get() throws InterruptedException {
      return queue.poll(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }
  }

  protected static SyncPublisherBuilder publisher(String host, int port, boolean withDirections) {
    Properties properties = properties(host, port);
    return publisher(properties, withDirections, false);
  }

  protected static SyncPublisherBuilder publisher(String host, int port, boolean withDirections, boolean withConfirms) {
    Properties properties = properties(host, port);
    return publisher(properties, withDirections, withConfirms);
  }

  protected static SyncPublisherBuilder publisher(Properties properties, boolean withDirections, boolean withConfirms) {
    if (withDirections) {
      appendDirections(properties);
    }
    if (withConfirms) {
      properties.setProperty(ConfigKeys.PUBLISHER_CONFIRMS, "true");
    }
    ClientFactory factory = new ClientFactory(properties);
    return factory.createSyncPublisherBuilder();
  }

  protected static SyncPublisherBuilder publisherMDC(String host, int port) {
    Properties properties = properties(host, port);
    appendDirections(properties);
    properties.setProperty(ConfigKeys.PUBLISHER_USE_MDC, "true");
    ClientFactory factory = new ClientFactory(properties);
    return factory.createSyncPublisherBuilder();
  }
}
