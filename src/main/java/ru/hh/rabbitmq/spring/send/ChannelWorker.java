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
import static ru.hh.rabbitmq.spring.util.ConnectionFactoryTester.testUntilSuccess;

public class ChannelWorker extends AbstractService {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChannelWorker.class);

  private final RabbitTemplate template;
  private final BlockingQueue<PublishTaskFuture> taskQueue;
  private final int templateTestRetryDelayMs;

  private final Thread thread;

  public ChannelWorker(RabbitTemplate template, BlockingQueue<PublishTaskFuture> taskQueue, String name, int templateTestRetryDelayMs) {
    this.template = template;
    this.taskQueue = taskQueue;
    this.templateTestRetryDelayMs = templateTestRetryDelayMs;
    this.thread = new Thread(name) {
      @Override
      public void run() {
        try {
          ensureOpen();
          notifyStarted();

          while (isRunning() && !isInterrupted()) {
            try {
              processQueue();
            } catch (AmqpException e) {
              LOGGER.warn("failed to send message(s): {}", e.toString(), e);
            } catch (RuntimeException e) {
              LOGGER.error("failed to send message(s): {}", e.toString(), e);
            }

            if (isInterrupted()) {
              break;
            }

            ensureOpen();
          }

          notifyStopped();

        } catch (InterruptedException e) {
          notifyStopped();

        } catch (RuntimeException e) {
          notifyFailed(e);
          LOGGER.error("crash: {}", e.toString(), e);
        }
      }
    };
  }

  private void processQueue() throws InterruptedException {
    while (isRunning() && !currentThread().isInterrupted()) {
      final PublishTaskFuture task = taskQueue.take();
      try {
        if (task.isCancelled()) {
          continue;
        }
        executeTask(task);
        task.complete();
      } catch (RuntimeException e) {
        LOGGER.info("returning failed message(s) to queue due to {}", e.toString());
        if (!taskQueue.offer(task)) {
          LOGGER.warn("failed to return message(s) to queue, dropping");
          task.fail(e);
        }
        throw e;
      }
    }
  }

  private void ensureOpen() throws InterruptedException {
    testUntilSuccess(template.getConnectionFactory(), templateTestRetryDelayMs);
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
  }

  @Override
  protected void doStart() {
    thread.start();
  }

  @Override
  protected void doStop() {
    thread.interrupt();
  }
}
