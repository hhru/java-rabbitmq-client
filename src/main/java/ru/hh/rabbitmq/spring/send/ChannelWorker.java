package ru.hh.rabbitmq.spring.send;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;

import com.google.common.util.concurrent.AbstractService;

import static java.lang.Thread.currentThread;

class ChannelWorker extends AbstractService {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChannelWorker.class);

  private final RabbitTemplate template;
  private final BlockingQueue<PublishTaskFuture> taskQueue;
  private final int retryDelayMs;

  private final Thread thread;

  ChannelWorker(RabbitTemplate template, BlockingQueue<PublishTaskFuture> taskQueue, String name, int retryDelayMs) {
    this.template = template;
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
    publishMessages(task.getMessages());
  }

  private void publishMessages(Map<Object, Destination> messages) {
    for (Map.Entry<Object, Destination> entry : messages.entrySet()) {
      Object message = entry.getKey();
      Destination destination = entry.getValue();
      publishMessage(message, destination, template);
    }
  }

  static void publishMessage(Object message, Destination destination, RabbitTemplate template) {
    CorrelationData correlationData = null;
    if (message instanceof CorrelatedMessage) {
      CorrelatedMessage correlated = (CorrelatedMessage) message;
      correlationData = correlated.getCorrelationData();
      message = correlated.getMessage();
    }

    if (destination != null) {
      if (correlationData != null) {
        template.convertAndSend(destination.getExchange(), destination.getRoutingKey(), message, correlationData);
      }
      else {
        template.convertAndSend(destination.getExchange(), destination.getRoutingKey(), message);
      }
    }
    else {
      if (correlationData != null) {
        template.correlationConvertAndSend(message, correlationData);
      }
      else {
        template.convertAndSend(message);
      }
    }
  }

  @Override
  protected void doStart() {
    thread.start();
  }

  @Override
  protected void doStop() {
    thread.interrupt();
  }

  RabbitTemplate getRabbitTemplate() {
    return template;
  }
}
