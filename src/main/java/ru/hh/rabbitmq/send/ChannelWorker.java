package ru.hh.rabbitmq.send;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractService;
import com.rabbitmq.client.Channel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.hh.rabbitmq.ChannelFactory;

class ChannelWorker extends AbstractService {
  public static final Logger logger = LoggerFactory.getLogger(ChannelWorker.class);
  
  private final ChannelFactory channelFactory;
  private final BlockingQueue<ChannelTask> taskQueue;
  private final ExecutorService executor;
  private final String name;

  ChannelWorker(ChannelFactory channelFactory, BlockingQueue<ChannelTask> taskQueue, String name) {
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

  @Override
  protected void doStart() {
    executor.execute(new Runnable() {
      public void run() {
        try {
          notifyStarted();
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
          notifyStopped();
        } catch (Throwable t) {
          notifyFailed(t);
          throw Throwables.propagate(t);
        }
      }
    });
  }

  @Override
  protected void doStop() {
    executor.shutdownNow();
  }
}
