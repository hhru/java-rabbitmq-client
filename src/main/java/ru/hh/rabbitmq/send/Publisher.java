package ru.hh.rabbitmq.send;

import com.google.common.base.Service;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Publisher {
  private static final Logger logger = LoggerFactory.getLogger(Publisher.class);
  
  private final Service[] workers;
  private final BlockingQueue<ChannelTask> taskQueue;

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
