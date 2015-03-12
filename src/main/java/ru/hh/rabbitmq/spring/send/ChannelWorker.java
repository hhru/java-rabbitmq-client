package ru.hh.rabbitmq.spring.send;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionListener;
import org.springframework.amqp.rabbit.core.ChannelCallback;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Monitor;
import com.rabbitmq.client.Channel;

public class ChannelWorker extends AbstractService implements ConnectionListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChannelWorker.class);
  
  private final RabbitTemplate template;
  private final BlockingQueue<PublishTaskFuture> taskQueue;
  private final Thread thread;

  // connection state fields. RabbitTemplate does not reconnect automatically, so have to handle it manually
  private long reconnectionDelay;
  private final ConnectionOpener connectionOpener = new ConnectionOpener();
  private AtomicReference<Connection> currentConnection = new AtomicReference<Connection>();
  private Monitor connectionMonitor = new Monitor();
  private Monitor.Guard connected = new Monitor.Guard(connectionMonitor) {
    @Override
    public boolean isSatisfied() {
      Connection connection = currentConnection.get();
      return connection != null && connection.isOpen();
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
          LOGGER.debug("worker started");
          forceOpenConnection();
          while (isRunning()) {
            processQueue();
          }
          LOGGER.debug("worker stopped");
          notifyStopped();
        } catch (Throwable t) {
          notifyFailed(t);
          throw Throwables.propagate(t);
        }
      }
    };
  }

  private void processQueue() {
    PublishTaskFuture task = null;
    try {
      while (isRunning()) {
        ensureOpen();
        if (!isRunning()) {
          continue;
        }
        try {
          task = this.taskQueue.take();
          
          // after possibly long waiting for new task, re-check connection, requeue if connection is broken
          if (!connected.isSatisfied()) {
            LOGGER.warn("requeued message on connection loss");
            this.taskQueue.add(task);
            continue;
          }
          
          // from now on we can't requeue - that might lead to duplicate messages
          if (!task.isCancelled()) {
            try {
              executeTask(template, task);
            } catch (Exception e) {
              task.fail(e);
              throw e;
            }
          }
        } finally {
          connectionMonitor.leave();
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.debug("worker interrupted, stopping");
    } catch (AmqpIOException e) { // Broken pipe, ...
      if (!taskQueue.offer(task)) {
        LOGGER.warn("network problem -- failed to execute task", e);
      }
    } catch (Exception e) {
      LOGGER.error("failed to execute task", e);
    }
  }

  private void ensureOpen() throws InterruptedException {
    boolean entered = false;
    while (isRunning() && !entered) {
      // wait until connected or timeout
      entered = connectionMonitor.enterWhen(connected, reconnectionDelay, TimeUnit.MILLISECONDS);
      // if still not in, force open connection
      if (!entered) {
        forceOpenConnection();
      }
    }
  }

  private void forceOpenConnection() {
    LOGGER.debug("forcing connection open");
    try {
      template.execute(connectionOpener);
    }
    catch (AmqpConnectException e) {
      // swallow, we're not interested in connection problems here
    }
  }

  private void executeTask(RabbitTemplate template, PublishTaskFuture task) throws IOException {
    if (task.getMDCContext() != null) {
      MDC.clear();
      if (task.getMDCContext().isPresent()) {
        MDC.setContextMap(task.getMDCContext().get());
      }
    }
    publishMessages(template, task.getMessages());
    task.complete();
    LOGGER.trace("task completed, sent {} messages, queue size is {}", task.getMessages().size(),
      this.taskQueue.size());
  }

  private void publishMessages(RabbitTemplate template, Map<Object, Destination> messages) throws IOException {
    for (Map.Entry<Object, Destination> entry : messages.entrySet()) {
      Object message = entry.getKey();
      Destination destination = entry.getValue();
      CorrelationData correlationData = null;
      if (message instanceof CorrelatedMessage) {
        CorrelatedMessage correlated = (CorrelatedMessage) message;
        correlationData = correlated.getCorrelationData();
        message = correlated.getMessage();
      }

      if (destination != null) {
        if (correlationData != null) {
          template.convertAndSend(destination.getExchange(), destination.getRoutingKey(), message, correlationData);
        }
        else {
          template.convertAndSend(destination.getExchange(), destination.getRoutingKey(), message);
        }
      }
      else {
        if (correlationData != null) {
          template.correlationconvertAndSend(message, correlationData);
        }
        else {
          template.convertAndSend(message);
        }
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
    LOGGER.debug("interrupting worker {}", thread.getName());
    thread.interrupt();
  }

  @Override
  public void onCreate(Connection connection) {
    LOGGER.debug("connection has been established");
    this.currentConnection.set(connection);
  }

  @Override
  public void onClose(@SuppressWarnings("unused") Connection connection) {
    LOGGER.debug("connection has been closed");
    this.currentConnection.set(null);
  }

  private static class ConnectionOpener implements ChannelCallback<Void> {
    @Override
    public Void doInRabbit(Channel channel) throws Exception {
      channel.isOpen();
      return null;
    }
  }
}
