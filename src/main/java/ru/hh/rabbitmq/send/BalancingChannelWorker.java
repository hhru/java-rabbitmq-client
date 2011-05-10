package ru.hh.rabbitmq.send;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractService;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ReturnListener;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.hh.rabbitmq.ChannelFactory;
import ru.hh.rabbitmq.simple.Message;

class BalancingChannelWorker extends AbstractService implements ReturnListener {
  public static final Logger logger = LoggerFactory.getLogger(BalancingChannelWorker.class);

  public static enum State {
    BROKEN,
    WORKING,
    RESTORING // try one, if succeed, change to WORKING
  }

  private final ChannelFactory channelFactory;
  private final BlockingQueue<PublishTaskFuture> taskQueue;
  private final FailedTaskProcessor failedTaskProcessor;
  private final Thread thread;

  // fail-fast settings
  private final int errorThreshold = 1;
  private long restoreIntervalMillis;

  private AtomicLong lastErrorMillis;
  private AtomicInteger errorCount;

  BalancingChannelWorker(
      ChannelFactory channelFactory, int normalQueueSize, FailedTaskProcessor failedTaskProcessor, final String name,
      long restoreIntervalMillis) {
    this.channelFactory = channelFactory;
    this.taskQueue = new ArrayBlockingQueue<PublishTaskFuture>(normalQueueSize);
    this.failedTaskProcessor = failedTaskProcessor;

    this.restoreIntervalMillis = restoreIntervalMillis;

    this.errorCount = new AtomicInteger(0);
    this.lastErrorMillis = new AtomicLong(0);

    this.thread =
      new Thread(name) {
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

  boolean offerFuture(PublishTaskFuture future) {
    boolean added = taskQueue.offer(future);
    if (added) {
      logger.trace("task added with {} messages, queue size is {}", future.getMessages().size(), taskQueue.size());
    }
    return added;
  }

  private void withNewConnection() {
    Channel plainChannel = null;
    Channel transactionalChannel = null;
    try {
      while (isRunning()) {
        if (isBroken()) {
          long timeToSleep = (lastErrorMillis.longValue() + restoreIntervalMillis) - System.currentTimeMillis();
          if (timeToSleep > 0) {
            Thread.sleep(timeToSleep);
          }
          continue;
        }
        if (isRestoring()) {
          // block following call if this one will fail
          lastErrorMillis.set(System.currentTimeMillis());
        }
        PublishTaskFuture task = this.taskQueue.take();

        if (!task.isCancelled()) {
          try {
            transactionalChannel = ensureOpen(transactionalChannel, true);
            plainChannel = ensureOpen(plainChannel, false);
            executeTask(plainChannel, transactionalChannel, task);
            errorCount.set(0);
          } catch (Exception e) {
            if (!(e instanceof InterruptedException)) {
              lastErrorMillis.set(System.currentTimeMillis());
              errorCount.incrementAndGet();
              failedTaskProcessor.returnFuture(task);
            }
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
    logger.trace("task completed, sent {} messages, queue size is {}", task.getMessages().size(), this.taskQueue.size());
  }

  private Channel ensureOpen(Channel channel, boolean transactional) throws IOException {
    if (channel == null || !channel.isOpen()) {
      channel = channelFactory.getChannel();
      channel.setReturnListener(this);
      if (transactional) {
        channel.txSelect();
      }
    }
    return channel;
  }

  private void publishMessages(Channel channel, Map<Message, Destination> messages) throws IOException {
    for (Map.Entry<Message, Destination> entry : messages.entrySet()) {
      channel.basicPublish(
        entry.getValue().getExchange(), entry.getValue().getRoutingKey(), entry.getValue().isMandatory(),
        entry.getValue().isImmediate(), entry.getKey().getProperties(), entry.getKey().getBody());
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
  public void handleBasicReturn(
      int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body)
    throws IOException {
    logger.error(
      "message returned, replyCode {}, replyText '{}', exchange {}, routingKey {}, properties {}",
      new Object[] { replyCode, replyText, exchange, routingKey, properties });
  }

  private State getState() {
    if (errorCount.intValue() < errorThreshold) {
      return State.WORKING;
    }
    if (System.currentTimeMillis() > lastErrorMillis.longValue() + restoreIntervalMillis) {
      return State.RESTORING;
    }
    return State.BROKEN;
  }

  boolean isBroken() {
    return getState() == State.BROKEN;
  }

  boolean isRestoring() {
    return getState() == State.RESTORING;
  }
}
