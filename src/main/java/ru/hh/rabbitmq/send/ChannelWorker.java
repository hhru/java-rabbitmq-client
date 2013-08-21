package ru.hh.rabbitmq.send;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractService;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ReturnListener;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.hh.rabbitmq.ChannelFactory;
import ru.hh.rabbitmq.simple.Message;

class ChannelWorker extends AbstractService implements ReturnListener {
  public static final Logger logger = LoggerFactory.getLogger(ChannelWorker.class);
  
  private final ChannelFactory channelFactory;
  private final BlockingQueue<PublishTaskFuture> taskQueue;
  private final String name;
  private final Thread thread;

  ChannelWorker(ChannelFactory channelFactory, BlockingQueue<PublishTaskFuture> taskQueue, final String name) {
    this.channelFactory = channelFactory;
    this.taskQueue = taskQueue;
    this.name = name;
    this.thread = new Thread(name) {
      @Override
      public void run() {
        try {
          notifyStarted();
          logger.info("worker started");
          while (isRunning()) {
            withNewConnection();
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

  private void withNewConnection() {
    Channel plainChannel = null;
    Channel transactionalChannel = null;
    try {
      while (isRunning()) {
        PublishTaskFuture task = this.taskQueue.take();
        if (!task.isCancelled()) {
          try {
            transactionalChannel = ensureOpen(transactionalChannel, true);
            plainChannel = ensureOpen(plainChannel, false);
            executeTask(plainChannel, transactionalChannel, task);
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
    } finally {
      this.channelFactory.returnChannel(plainChannel);
      this.channelFactory.returnChannel(transactionalChannel);
    }
  }

  private void executeTask(Channel plainChannel, Channel transactionalChannel, PublishTaskFuture task) throws IOException {
    if (task.isTransactional()) {
      publishMessages(transactionalChannel, task.getMessages());
      if (!task.isCancelled()) {
        transactionalChannel.txCommit();
      } else {
        transactionalChannel.txRollback();
      }
    } else {
      publishMessages(plainChannel, task.getMessages());
    }
    task.complete();
    logger.trace("task completed, sent {} messages, queue size is {}", task.getMessages().size(), 
      this.taskQueue.size());
  }

  private Channel ensureOpen(Channel channel, boolean transactional) throws IOException {
    if (channel == null || !channel.isOpen()) {
      Date start = new Date();
      channel = channelFactory.getChannel();
      channel.setReturnListener(this);
      if (transactional) {
        channel.txSelect();
      }
      if(shouldLogMetrics()) {
        long openTime = new Date().getTime() - start.getTime();
        logger.warn("Inner queue of '{}' is too big (size {}, remain capacity {}). Channel opened in {} millis",
                new Object[]{name, taskQueue.size(), taskQueue.remainingCapacity(), openTime});
      }
    }
    return channel;
  }
  
  private void publishMessages(Channel channel, Map<Message, Destination> messages) throws IOException {
    for (Map.Entry<Message, Destination> entry : messages.entrySet()) {
      Date start = new Date();
      Message message = entry.getKey();
      Destination destination = entry.getValue();
      channel.basicPublish(destination.getExchange(), destination.getRoutingKey(), destination.isMandatory(),
              destination.isImmediate(), message.getProperties(), message.getBody());
      if(shouldLogMetrics()) {
        long sendTime = new Date().getTime() - start.getTime();
        logger.warn("Inner queue of '{}' is too big (size {}, remain capacity {}). Message sent to exchange '{}' with routing key '{}' in {} millis.",
                new Object[]{name, taskQueue.size(), taskQueue.remainingCapacity(), destination.getExchange(), destination.getRoutingKey(), sendTime});
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

  @Override
  public void handleBasicReturn(int replyCode, String replyText, String exchange, String routingKey, 
                                AMQP.BasicProperties properties, byte[] body) throws IOException {
    logger.error("message returned, replyCode {}, replyText '{}', exchange {}, routingKey {}, properties {}",
      new Object[]{replyCode, replyText, exchange, routingKey, properties});
  }

  private boolean shouldLogMetrics() {
    return taskQueue.size() > taskQueue.remainingCapacity() * 2;
  }
}
