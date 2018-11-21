package ru.hh.rabbitmq.spring.send;

import com.google.common.util.concurrent.AbstractService;
import static java.lang.Thread.currentThread;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;

abstract class AbstractChannelWorker extends AbstractService {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractChannelWorker.class);

  protected final MessageSender messageSender;
  private final Sleeper sleeper;
  private final Thread thread;

  protected AbstractChannelWorker(String name, MessageSender messageSender, Duration pollingInterval) {
    this.messageSender = messageSender;
    sleeper = pollingInterval.isZero() ? () -> {} : () -> Thread.sleep(pollingInterval.toMillis());
    thread = new Thread(name) {
      @Override
      public void run() {
        try {
          notifyStarted();
          processQueue();
          notifyStopped();
        } catch (RuntimeException e) {
          notifyFailed(e);
          LOGGER.error("crash", e);
        }
      }
    };
  }

  private void processQueue() {
    while (isRunning() && !currentThread().isInterrupted()) {
      try {
        handleTask();
        sleeper.sleep();
      } catch (InterruptedException e) {
        currentThread().interrupt();
        return;
      }
    }
  }

  protected abstract void handleTask() throws InterruptedException;

  protected void processPublishTask(PublishTaskFuture task) {
    if (task.getMDCContext() != null) {
      MDC.clear();
      if (task.getMDCContext().isPresent()) {
        MDC.setContextMap(task.getMDCContext().get());
      }
    }
    messageSender.publishMessages(task.getMessages());
  }

  @Override
  protected void doStart() {
    thread.start();
  }

  @Override
  protected void doStop() {
    ConnectionFactory connectionFactory = messageSender.getTemplate().getConnectionFactory();
    if (connectionFactory instanceof CachingConnectionFactory) {
      ((CachingConnectionFactory) connectionFactory).destroy();
    }

    thread.interrupt();
  }

  private interface Sleeper {
    void sleep() throws InterruptedException;
  }
}
