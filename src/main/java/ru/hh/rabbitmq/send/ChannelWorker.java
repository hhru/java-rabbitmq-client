package ru.hh.rabbitmq.send;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractService;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ReturnListener;
import java.io.IOException;
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
  private final Thread thread;

  private final Long connectTimeTolerance;
  private final Long sendTimeTolerance;

  ChannelWorker(ChannelFactory channelFactory, BlockingQueue<PublishTaskFuture> taskQueue, final String name) {
    this(channelFactory, taskQueue, name, null, null);
  }

  ChannelWorker(ChannelFactory channelFactory, BlockingQueue<PublishTaskFuture> taskQueue, final String name,
                Long connectTimeTolerance, Long sendTimeTolerance) {
    this.connectTimeTolerance = connectTimeTolerance;
    this.sendTimeTolerance = sendTimeTolerance;
    this.channelFactory = channelFactory;
    this.taskQueue = taskQueue;
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
      long start = System.nanoTime();
      channel = channelFactory.getChannel();
      channel.setReturnListener(this);
      if (transactional) {
        channel.txSelect();
      }
      if(shouldLogMetrics(start, connectTimeTolerance)) {
        logger.warn("Channel to {}:{} opened in {} millis", new Object[]{channel.getConnection().getHost(),
                channel.getConnection().getPort(), toMillis(System.nanoTime() - start)});
      }
    }
    return channel;
  }
  
  private void publishMessages(Channel channel, Map<Message, Destination> messages) throws IOException {
    for (Map.Entry<Message, Destination> entry : messages.entrySet()) {
      long start = System.nanoTime();
      Message message = entry.getKey();
      Destination destination = entry.getValue();
      channel.basicPublish(destination.getExchange(), destination.getRoutingKey(), destination.isMandatory(),
              destination.isImmediate(), message.getProperties(), message.getBody());
      if(shouldLogMetrics(start, sendTimeTolerance)) {
        logger.warn("Message sent to exchange '{}' with routing key '{}' in {} millis.",
                new Object[]{destination.getExchange(), destination.getRoutingKey(), toMillis(System.nanoTime() - start)});
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

  private boolean shouldLogMetrics(long start, Long timeTolerance) {
    if(timeTolerance == null) {
      return false;
    }
    return toMillis(System.nanoTime() - start) > timeTolerance;
  }

  private long toMillis(long nanoTime) {
    return nanoTime / 1000000L;
  }
}
