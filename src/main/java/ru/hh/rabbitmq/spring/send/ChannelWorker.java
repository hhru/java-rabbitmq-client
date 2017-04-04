package ru.hh.rabbitmq.spring.send;

import com.google.common.util.concurrent.AbstractService;
import static java.lang.Thread.currentThread;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;

class ChannelWorker extends AbstractService {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChannelWorker.class);

  private final MessageSender messageSender;
  private final BlockingQueue<PublishTaskFuture> taskQueue;
  private final int retryDelayMs;
  private final Thread thread;

  ChannelWorker(MessageSender messageSender,
                BlockingQueue<PublishTaskFuture> taskQueue,
                String name,
                int retryDelayMs) {
    this.taskQueue = taskQueue;
    this.retryDelayMs = retryDelayMs;
    this.thread = new Thread(name) {
      @Override
      public void run() {
        try {
          notifyStarted();

          processQueue();

          notifyStopped();

        } catch (RuntimeException e) {
          notifyFailed(e);
          LOGGER.error("crash: {}", e.toString(), e);
        }
      }
    };
    this.messageSender = messageSender;
  }

  private void processQueue() {
    while (isRunning() && !currentThread().isInterrupted()) {

      final PublishTaskFuture task;
      try {
        task = taskQueue.take();
      } catch (InterruptedException e) {
        currentThread().interrupt();
        return;
      }

      executeTaskUntilSuccess(task);
    }
  }

  private void executeTaskUntilSuccess(final PublishTaskFuture task) {
    while (!task.isCancelled()) {
      try {
        executeTask(task);
        task.complete();
        return;

      } catch (RuntimeException e) {
        final String message = String.format("failed to process task: %s, waiting before next attempt", e.toString());
        if (e instanceof AmqpException) {
          LOGGER.warn(message, e);
        } else {
          LOGGER.error(message, e);
        }

        try {
          Thread.sleep(retryDelayMs);
        } catch (InterruptedException ie) {
          currentThread().interrupt();
          throw new RuntimeException("failed to retry task: got interrupted signal, dropping task", ie);
        }

        if (!isRunning() || currentThread().isInterrupted()) {
          throw new RuntimeException("failed to retry task: ChannelWorker is stopped, dropping task");
        }
      }
    }
  }

  private void executeTask(PublishTaskFuture task) {
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
}
