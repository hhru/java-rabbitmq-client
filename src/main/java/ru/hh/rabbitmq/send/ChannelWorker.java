package ru.hh.rabbitmq.send;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractService;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.hh.rabbitmq.ChannelFactory;
import ru.hh.rabbitmq.simple.Message;

// TODO ReturnListener
class ChannelWorker extends AbstractService {
  public static final Logger logger = LoggerFactory.getLogger(ChannelWorker.class);
  
  private final ChannelFactory channelFactory;
  private final BlockingQueue<PublishTaskFuture> taskQueue;
  private final ExecutorService executor;
  private final String name;

  ChannelWorker(ChannelFactory channelFactory, BlockingQueue<PublishTaskFuture> taskQueue, String name) {
    this.channelFactory = channelFactory;
    this.taskQueue = taskQueue;
    this.name = name;
    this.executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, ChannelWorker.this.name);
      }
    });
  }

  private Channel ensureOpen(Channel channel, ChannelFactory factory) {
    if (channel == null || !channel.isOpen()) {
      channel = channelFactory.getChannel();
    }
    return channel;
  }
  
  private void publishMessages(Channel channel, Destination destination, Collection<Message> messages) throws IOException {
    for (Message message : messages) {
      channel.basicPublish(destination.getExchange(), destination.getRoutingKey(), destination.isMandatory(), 
        destination.isImmediate(), message.getProperties(), message.getBody());
    }
  }
                             
  @Override
  protected void doStart() {
    executor.execute(new Runnable() {
      public void run() {
        // TODO too many try/catch/finally
        while (isRunning()) {
          Channel plainChannel = null;
          Channel transactionalChannel = null;
          try {
            while (isRunning()) {
              PublishTaskFuture task = taskQueue.take();
              if (task.isCancelled()) {
                continue;
              }
              try {
                if (task.isTransactional()) {
                  transactionalChannel = ensureOpen(transactionalChannel, channelFactory);
                  transactionalChannel.txSelect();
                  publishMessages(transactionalChannel, task.getDestination(), task.getMessages());
                  transactionalChannel.txCommit();
                } else {
                  plainChannel = ensureOpen(plainChannel, channelFactory);
                  publishMessages(plainChannel, task.getDestination(), task.getMessages());
                }
                task.complete();
              } catch (Exception e) {
                task.fail(e);
                throw e;
              }
            }
          } catch (Exception e) {
            logger.error("failed to execute task", e);
          } finally {
            channelFactory.returnChannel(plainChannel);
            channelFactory.returnChannel(transactionalChannel);
          }
        }
      }
    });
  }

  @Override
  protected void doStop() {
    executor.shutdownNow();
  }
}
