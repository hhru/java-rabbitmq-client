package ru.hh.rabbitmq.spring.send;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionListener;
import org.springframework.amqp.rabbit.core.ChannelCallback;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Monitor;
import com.rabbitmq.client.Channel;

public class ChannelWorker extends AbstractService implements ConnectionListener {
  public static final Logger logger = LoggerFactory.getLogger(ChannelWorker.class);
  
  private final RabbitTemplate template;
  private final BlockingQueue<PublishTaskFuture> taskQueue;
  private final Thread thread;

  // connection state

  private long reconnectionDelay;
  private final ConnectionChecker connectionChecker = new ConnectionChecker();
  private AtomicBoolean connectionActive = new AtomicBoolean(true);
  private Monitor connectionMonitor = new Monitor();
  private Monitor.Guard connected = new Monitor.Guard(connectionMonitor) {
    @Override
    public boolean isSatisfied() {
      return connectionActive.get();
    }
  };

  public ChannelWorker(RabbitTemplate template, BlockingQueue<PublishTaskFuture> taskQueue, String name, long reconnectionDelay) {
    this.template = template;
    this.taskQueue = taskQueue;
    this.reconnectionDelay = reconnectionDelay;
    this.thread = new Thread(name) {
      @Override
      public void run() {
        try {
          notifyStarted();
          logger.info("worker started");
          while (isRunning()) {
            processQueue();
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

  private void processQueue() {
    try {
      while (isRunning()) {
        tryConnection();
        try {
          PublishTaskFuture task = this.taskQueue.take();
          try {
            ensureOpen();
            System.out.println("still open");
          }
          catch (AmqpConnectException e) {
            // connection is broken, requeue
            this.taskQueue.add(task);
          }
          if (!task.isCancelled()) {
            try {
              executeTask(template, task);
            }
            catch (Exception e) {
              task.fail(e);
              throw e;
            }
          }
        }
        finally {
          connectionMonitor.leave();
        }
      }
    } catch (InterruptedException e) {
      logger.debug("worker interrupted, stopping");
    } catch (Exception e) {
      logger.error("failed to execute task", e);
    }
  }

  private void tryConnection() throws InterruptedException {
    boolean entered = false;
    while (isRunning() && !entered) {
      entered = connectionMonitor.enterWhen(connected, reconnectionDelay, TimeUnit.MILLISECONDS);
      if (!entered) {
        try {
          ensureOpen();
        }
        catch (AmqpConnectException e) {
          // swallow, we're not interested in connection problems here
        }
      }
    }
  }

  private void ensureOpen() {
    template.execute(connectionChecker);
  }

  private void executeTask(RabbitTemplate template, PublishTaskFuture task) throws IOException {
    publishMessages(template, task.getMessages());
    task.complete();
    logger.trace("task completed, sent {} messages, queue size is {}", task.getMessages().size(), 
      this.taskQueue.size());
  }

  private void publishMessages(RabbitTemplate template, Map<Object, Destination> messages) throws IOException {
    for (Map.Entry<Object, Destination> entry : messages.entrySet()) {
      Object message = entry.getKey();
      Destination destination = entry.getValue();
      // System.out.println("------ sending " + message);
      if (destination != null) {
        template.convertAndSend(destination.getExchange(), destination.getRoutingKey(), message);
      }
      else {
        template.convertAndSend(message);
      }
    }
  }

  @Override
  protected void doStart() {
    template.getConnectionFactory().addConnectionListener(this);
    thread.start();
  }

  @Override
  protected void doStop() {
    logger.debug("interrupting worker {}", thread.getName());
    thread.interrupt();
  }

  @Override
  public void onCreate(@SuppressWarnings("unused") Connection connection) {
    System.out.println("connected");
    connectionActive.set(true);
  }

  @Override
  public void onClose(@SuppressWarnings("unused") Connection connection) {
    System.out.println("disconnected");
    connectionActive.set(false);
  }

  private static class ConnectionChecker implements ChannelCallback<Void> {
    @Override
    public Void doInRabbit(Channel channel) throws Exception {
      channel.isOpen();
      return null;
    }
  }
}
