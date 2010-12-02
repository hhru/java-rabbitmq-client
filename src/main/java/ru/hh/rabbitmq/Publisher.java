package ru.hh.rabbitmq;

import com.google.common.base.Service;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.AbstractFuture;
import com.rabbitmq.client.Channel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Publisher {
  private static final Logger logger = LoggerFactory.getLogger(Publisher.class);
  
  private final Service[] workers;
  private final BlockingQueue<ChannelTask> taskQueue;
  
  private static interface ChannelTask {
    void run(Channel channel);
  }
  
  private class ChannelWorker extends AbstractExecutionThreadService {
    private final ChannelFactory channelFactory;

    private ChannelWorker(ChannelFactory channelFactory) {
      this.channelFactory = channelFactory;
    }

    // TODO service name

    @Override
    protected void run() throws Exception {
      while (isRunning()) {
        try {
          Channel channel = channelFactory.getChannel();
          try {
            while (isRunning()) {
              taskQueue.take().run(channel);
            }
          } catch (Exception e) {
            logger.error("failed to run task", e);
          } finally {
            channelFactory.returnChannel(channel);
          }
        } catch (Exception e) {
          logger.error("failed to get channel", e);
        }
      }
    }
  }
  
  private static class ChannelTaskFuture extends AbstractFuture<Void> implements ChannelTask {
    private final ChannelTask task;

    private ChannelTaskFuture(ChannelTask task) {
      this.task = task;
    }

    @Override
    public void run(Channel channel) {
      try {
        if (!isCancelled()) {
          task.run(channel);
          set(null);
        }
      } catch (RuntimeException e) {
        setException(e);
      }
    }
  }

  private Future<Void> submit(final ChannelTask task) {
    ChannelTaskFuture future = new ChannelTaskFuture(task);
    taskQueue.add(future);
    return future;
  }
  
  public void close() {
    for (Service worker : workers) {
      worker.stopAndWait();
    }
  }

}
