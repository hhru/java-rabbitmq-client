package ru.hh.rabbitmq;

import com.google.common.base.Service;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.AbstractFuture;
import com.rabbitmq.client.Channel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WithChannel {
  private static final Logger logger = LoggerFactory.getLogger(WithChannel.class);
  
  private final Service[] workers;
  private final BlockingQueue<ChannelTask> taskQueue;
  
  private static class WithChannelWorker extends AbstractExecutionThreadService {
    private final ChannelFactory channelFactory;

    private WithChannelWorker(ChannelFactory channelFactory) {
      this.channelFactory = channelFactory;
    }

    @Override
    protected void run() throws Exception {
      while (isRunning()) {
        // TODO get channel, execute ChannelTask, set future
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
        task.run(channel);
        set(null);
      } catch (RuntimeException e) {
        setException(e);
      }
    }
  }

  public Future<Void> submit(final ChannelTask task) {
    ChannelTaskFuture future = new ChannelTaskFuture(task);
    taskQueue.add(future);
    return future;
  }
}
