package ru.hh.rabbitmq.send;

import com.google.common.base.Service;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.AbstractFuture;
import com.rabbitmq.client.Channel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.hh.rabbitmq.ChannelFactory;

public class Publisher {
  private static final Logger logger = LoggerFactory.getLogger(Publisher.class);
  
  private final Service[] workers;
  private final BlockingQueue<ChannelTask> taskQueue;

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
