package ru.hh.rabbitmq.spring.send;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import static java.lang.Thread.currentThread;

class ChannelWorker extends AbstractChannelWorker {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChannelWorker.class);

  private final BlockingQueue<PublishTaskFuture> taskQueue;
  private final Duration retrySendDelay;

  ChannelWorker(String name, MessageSender messageSender, BlockingQueue<PublishTaskFuture> taskQueue, Duration retrySendDelay) {
    super(name, messageSender, Duration.ZERO);
    this.taskQueue = taskQueue;
    this.retrySendDelay = retrySendDelay;
  }

  @Override
  protected void handleTask() throws InterruptedException {
    final PublishTaskFuture task = taskQueue.take();
    executeTaskUntilSuccess(task);
  }

  private void executeTaskUntilSuccess(final PublishTaskFuture task) {
    while (!task.isCancelled()) {
      try {
        processPublishTask(task);
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
          Thread.sleep(retrySendDelay.toMillis());
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
}
