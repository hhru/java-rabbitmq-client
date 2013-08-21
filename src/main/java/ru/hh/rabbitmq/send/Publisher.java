package ru.hh.rabbitmq.send;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.rabbitmq.client.Address;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.hh.rabbitmq.ConnectionFactory;
import ru.hh.rabbitmq.impl.ChannelFactoryImpl;
import ru.hh.rabbitmq.impl.SingleConnectionFactory;
import ru.hh.rabbitmq.simple.Message;
import ru.hh.rabbitmq.util.Addresses;

public class Publisher {

  private static final long DEFAULT_RETRY_DELAY = 100;
  private static final int DEFAULT_ATTEMPTS_NUMBER = 3;
  private static final int DEFAULT_INNER_QUEUE_CAPACITY = 1000;

  public static final Logger logger = LoggerFactory.getLogger(Publisher.class);

  private com.rabbitmq.client.ConnectionFactory connectionFactory;
  private List<Address> addresses = new ArrayList<Address>();
  private long retryDelay = DEFAULT_RETRY_DELAY;
  private TimeUnit retryDelayUnits = TimeUnit.MILLISECONDS;
  private int attemptsNumber = DEFAULT_ATTEMPTS_NUMBER;
  private int innerQueueCapacity = DEFAULT_INNER_QUEUE_CAPACITY;

  private Long connectTimeTolerance;
  private Long sendTimeTolerance;

  private final List<ConnectionFactory> factories = new ArrayList<ConnectionFactory>();
  private final List<Service> workers = new ArrayList<Service>();

  private BlockingQueue<PublishTaskFuture> taskQueue;

  public Publisher(com.rabbitmq.client.ConnectionFactory connectionFactory, String hosts, int port) {
    this(connectionFactory, Addresses.split(hosts, port));
  }

  public Publisher(com.rabbitmq.client.ConnectionFactory connectionFactory, Address... addresses) {
    if (addresses == null || addresses.length == 0) {
      throw new IllegalArgumentException("no connection addresses");
    }
    this.connectionFactory = connectionFactory;
    this.addresses.addAll(Arrays.asList(addresses));
  }

  public void setRetryDelay(long retryDelay) {
    this.retryDelay = retryDelay;
  }

  public void setRetryDelayUnits(TimeUnit retryDelayUnits) {
    this.retryDelayUnits = retryDelayUnits;
  }

  public void setAttemptsNumber(int attemptsNumber) {
    this.attemptsNumber = attemptsNumber;
  }

  public void setInnerQueueCapacity(int innerQueueCapacity) {
    this.innerQueueCapacity = innerQueueCapacity;
  }

  public void setConnectTimeTolerance(Long connectTimeTolerance) {
    this.connectTimeTolerance = connectTimeTolerance;
  }

  public void setSendTimeTolerance(Long sendTimeTolerance) {
    this.sendTimeTolerance = sendTimeTolerance;
  }

  public void start() {
    taskQueue = new ArrayBlockingQueue<PublishTaskFuture>(innerQueueCapacity);
    for(Address address : addresses) {
      ConnectionFactory factory = new SingleConnectionFactory(connectionFactory, retryDelayUnits, retryDelay, attemptsNumber, address);
      factories.add(factory);
      Service worker = new ChannelWorker(new ChannelFactoryImpl(factory), taskQueue, address.toString() + "-publisher-worker",
              connectTimeTolerance, sendTimeTolerance);
      workers.add(worker);
      worker.start();
    }
  }

  public void close() {
    for (Service worker : workers) {
      worker.stopAndWait();
    }
    for (ConnectionFactory factory : factories) {
      factory.close();
    }
  }

  @Deprecated
  public Publisher(
      com.rabbitmq.client.ConnectionFactory connectionFactory, TimeUnit retryUnit, long retryDelay, int attempts,
      int maxQueueLength, Address... addresses) {
    this(connectionFactory, addresses);
    setRetryDelay(retryDelay);
    setRetryDelayUnits(retryUnit);
    setAttemptsNumber(attempts);
    setInnerQueueCapacity(maxQueueLength);
    start();
  }

  @Deprecated
  public Publisher(
      com.rabbitmq.client.ConnectionFactory connectionFactory, TimeUnit retryUnit, long retryDelay, int attempts, int queueLength,
      String hosts, int port) {
    this(connectionFactory, retryUnit, retryDelay, attempts, queueLength, Addresses.split(hosts, port));
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

  private void addFuture(PublishTaskFuture future) {
    try {
      taskQueue.add(future);
      logger.trace("task added with {} messages, queue size is {}", future.getMessages().size(), taskQueue.size());
    } catch (IllegalStateException e) {
      throw new QueueIsFullException(e);
    }
  }
}
