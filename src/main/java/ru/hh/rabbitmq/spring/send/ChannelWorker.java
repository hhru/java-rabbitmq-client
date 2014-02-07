package ru.hh.rabbitmq.spring.send;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractService;

public class ChannelWorker extends AbstractService {
  public static final Logger logger = LoggerFactory.getLogger(ChannelWorker.class);
  
  private final RabbitTemplate template;
  private final BlockingQueue<PublishTaskFuture> taskQueue;
  private final Thread thread;

  public ChannelWorker(RabbitTemplate template, BlockingQueue<PublishTaskFuture> taskQueue, String name) {
    this.template = template;
    this.taskQueue = taskQueue;
    this.thread = new Thread(name) {
      @Override
      public void run() {
        try {
          notifyStarted();
          logger.info("worker started");
          while (isRunning()) {
            processQueue();
          }
          logger.info("worker stopped");
          notifyStopped();
        } catch (Throwable t) {
          notifyFailed(t);
          throw Throwables.propagate(t);
        }
      }
    };
  }

  private void processQueue() {
    try {
      while (isRunning()) {
        PublishTaskFuture task = this.taskQueue.take();
        if (!task.isCancelled()) {
          try {
            executeTask(template, task);
          } catch (Exception e) {
            task.fail(e);
            throw e;
          }
        }
      }
    } catch (InterruptedException e) {
      logger.debug("worker interrupted, stopping");
    } catch (Exception e) {
      logger.error("failed to execute task", e);
    }
  }

  private void executeTask(RabbitTemplate template, PublishTaskFuture task) throws IOException {
    publishMessages(template, task.getMessages());
    task.complete();
    logger.trace("task completed, sent {} messages, queue size is {}", task.getMessages().size(), 
      this.taskQueue.size());
  }

  private void publishMessages(RabbitTemplate template, Map<Object, Destination> messages) throws IOException {
    for (Map.Entry<Object, Destination> entry : messages.entrySet()) {
      Object message = entry.getKey();
      Destination destination = entry.getValue();
      if (destination != null) {
        template.convertAndSend(destination.getExchange(), destination.getRoutingKey(), message);
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
    logger.debug("interrupting worker {}", thread.getName());
    thread.interrupt();
  }
}
