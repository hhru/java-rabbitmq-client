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

  ChannelWorker(ChannelFactory channelFactory, BlockingQueue<PublishTaskFuture> taskQueue, final String name) {
    this.channelFactory = channelFactory;
    this.taskQueue = taskQueue;
    this.thread = new Thread(name) {
      @Override
      public void run() {
        try {
          notifyStarted();
          logger.info("worker started");
          while (isRunning()) {
            Channel plainChannel = null;
            Channel transactionalChannel = null;
            try {
              while (isRunning()) {
                PublishTaskFuture task = ChannelWorker.this.taskQueue.take();
                transactionalChannel = ensureOpen(transactionalChannel, ChannelWorker.this.channelFactory, true);
                plainChannel = ensureOpen(plainChannel, ChannelWorker.this.channelFactory, false);
                if (!task.isCancelled()) {
                  try {
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
                      ChannelWorker.this.taskQueue.size());
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
              ChannelWorker.this.channelFactory.returnChannel(plainChannel);
              ChannelWorker.this.channelFactory.returnChannel(transactionalChannel);
            }
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

  private Channel ensureOpen(Channel channel, ChannelFactory factory, boolean transactional) throws IOException {
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
      channel.basicPublish(entry.getValue().getExchange(), entry.getValue().getRoutingKey(), entry.getValue().isMandatory(), 
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
  public void handleBasicReturn(int replyCode, String replyText, String exchange, String routingKey, 
                                AMQP.BasicProperties properties, byte[] body) throws IOException {
    logger.error("message returned, replyCode {}, replyText '{}', exchange {}, routingKey {}, properties {}",
      new Object[]{replyCode, replyText, exchange, routingKey, properties});
  }
}
