package ru.hh.rabbitmq.send;

import com.google.common.base.Service;
import com.google.common.util.concurrent.ListenableFuture;
import com.rabbitmq.client.Address;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.hh.rabbitmq.ConnectionFactory;
import ru.hh.rabbitmq.impl.ChannelFactoryImpl;
import ru.hh.rabbitmq.impl.SingleConnectionFactory;
import ru.hh.rabbitmq.simple.Message;
import ru.hh.rabbitmq.util.Addresses;

public class BalancingPublisher implements FailedTaskProcessor {
  public static final Logger logger = LoggerFactory.getLogger(BalancingPublisher.class);

  private final ConnectionFactory[] connectionFactories;
  private final BalancingChannelWorker[] workers;
  private AtomicInteger balancerIndex = new AtomicInteger();

  public BalancingPublisher(
      com.rabbitmq.client.ConnectionFactory connectionFactory, TimeUnit retryUnit, long retryDelay, int attempts,
      int maxQueueLength, long restoreIntervalMs, Address... addresses) {
    if (addresses.length < 1) {
      throw new IllegalArgumentException("no connection addresses");
    }

    connectionFactories = new ConnectionFactory[addresses.length];
    workers = new BalancingChannelWorker[addresses.length];

    ChannelFactoryImpl channelFactory;

    for (int i = 0; i < addresses.length; i++) {
      connectionFactories[i] = new SingleConnectionFactory(connectionFactory, retryUnit, retryDelay, attempts, addresses[i]);
      channelFactory = new ChannelFactoryImpl(connectionFactories[i]);
      workers[i] =
        new BalancingChannelWorker(
          channelFactory, maxQueueLength, this, addresses[i].toString() + "-publisher-worker", restoreIntervalMs);
      workers[i].start();
    }
  }

  public BalancingPublisher(
      com.rabbitmq.client.ConnectionFactory connectionFactory, TimeUnit retryUnit, long retryDelay, int attempts, int queueLength,
      long restoreIntervalMillis, String hosts, int port) {
    this(connectionFactory, retryUnit, retryDelay, attempts, queueLength, restoreIntervalMillis, Addresses.split(hosts, port));
  }

  public void close() {
    for (Service worker : workers) {
      worker.stopAndWait();
    }
    for (ConnectionFactory factory : connectionFactories) {
      factory.close();
    }
  }

  /**
   * Nontransactional nonblocking method, enqueues messages internally, throws exception if local queue is full
   *
   * @return  ListenableFuture that gets completed after successful sending
   */
  public ListenableFuture<Void> send(Destination destination, Message... messages) {
    return send(destination, Arrays.asList(messages));
  }

  /**
   * Nontransactional nonblocking method, enqueues messages internally, throws exception if local queue is full
   *
   * @return  ListenableFuture that gets completed after successful sending
   */
  public ListenableFuture<Void> send(Destination destination, Collection<Message> messages) {
    PublishTaskFuture future = new PublishTaskFuture(destination, messages, false);
    addFuture(future);
    return future;
  }

  /**
   * Nontransactional nonblocking method, enqueues messages internally, throws exception if local queue is full
   *
   * @return  ListenableFuture that gets completed after successful sending
   */
  public ListenableFuture<Void> send(Map<Message, Destination> messages) {
    PublishTaskFuture future = new PublishTaskFuture(messages, false);
    addFuture(future);
    return future;
  }

  /**
   * Transactional nonblocking method, enqueues messages internally, throws exception if local queue is full
   *
   * @return  ListenableFuture that gets completed after successful sending
   */
  public ListenableFuture<Void> sendTransactional(Destination destination, Message... messages) {
    return sendTransactional(destination, Arrays.asList(messages));
  }

  /**
   * Transactional nonblocking method, enqueues messages internally, throws exception if local queue is full
   *
   * @return  ListenableFuture that gets completed after successful sending
   */
  public ListenableFuture<Void> sendTransactional(Destination destination, Collection<Message> messages) {
    PublishTaskFuture future = new PublishTaskFuture(destination, messages, true);
    addFuture(future);
    return future;
  }

  /**
   * Transactional nonblocking method, enqueues messages internally, throws exception if local queue is full
   *
   * @return  ListenableFuture that gets completed after successful sending
   */
  public ListenableFuture<Void> sendTransactional(Map<Message, Destination> messages) {
    PublishTaskFuture future = new PublishTaskFuture(messages, true);
    addFuture(future);
    return future;
  }

  /**
   * Walk over all workers. For each one check if worker is not broken, then offer future. If all workers are either broken or not
   * accepting new future, throw QueueIsFullException.
   *
   * @param  future
   */
  private void addFuture(PublishTaskFuture future) {
    int cycle = 0;
    BalancingChannelWorker worker;
    do {
      worker = getWorker();
      if (worker.isBroken()) {
        continue;
      }
      if (worker.offerFuture(future)) {
        return;
      }
    } while (++cycle < workers.length);
    throw new QueueIsFullException();
  }

  /**
   * Cyclicly return next worker.
   *
   * @return
   */
  private BalancingChannelWorker getWorker() {
    // simple round-robin
    return workers[Math.abs(balancerIndex.getAndIncrement()) % workers.length];
  }

  @Override
  public void returnFuture(PublishTaskFuture future) {
    try {
      addFuture(future);
    } catch (Exception e) {
      logger.warn("Failed to re-add future returned by one of workers, dropping it", e);
      future.fail(e);
    }
  }
}
