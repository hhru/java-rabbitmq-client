package ru.hh.rabbitmq.send;

import com.google.common.base.Service;
import com.google.common.util.concurrent.ListenableFuture;
import com.rabbitmq.client.Address;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import ru.hh.rabbitmq.ConnectionFactory;
import ru.hh.rabbitmq.impl.ChannelFactoryImpl;
import ru.hh.rabbitmq.impl.SingleConnectionFactory;
import ru.hh.rabbitmq.simple.Message;
import ru.hh.rabbitmq.util.Addresses;

public class Publisher {
  private final ConnectionFactory[] connectionFactories;
  private final Service[] workers;
  private final BlockingQueue<PublishTaskFuture> taskQueue;

  public Publisher(com.rabbitmq.client.ConnectionFactory connectionFactory, TimeUnit retryUnit, long retryDelay,
                   int attempts, int maxQueueLength, Address... addresses) {
    if (addresses.length < 1) {
      throw new IllegalArgumentException("can't create Publisher without connection addresses");
    }
    connectionFactories = new ConnectionFactory[addresses.length];
    workers = new Service[addresses.length];
    taskQueue = new ArrayBlockingQueue<PublishTaskFuture>(maxQueueLength);
    for(int i = 0; i < addresses.length; i ++) {
      connectionFactories[i] = new SingleConnectionFactory(connectionFactory, retryUnit, retryDelay, attempts, addresses[i]);
      workers[i] = new ChannelWorker(new ChannelFactoryImpl(connectionFactories[i]), taskQueue, addresses[i].toString() + "-publisher-worker");
      workers[i].start();
    }
  }

  public Publisher(com.rabbitmq.client.ConnectionFactory connectionFactory, TimeUnit retryUnit, long retryDelay,
                   int attempts, int queueLength, String hosts, int port) {
    this(connectionFactory, retryUnit, retryDelay, attempts, queueLength, Addresses.split(hosts, port));
  }

  public void close() {
    for (Service worker : workers) {
      worker.stopAndWait();
    }
    for(ConnectionFactory factory : connectionFactories) {
      factory.close();
    }
  }

  /**
   * Nontransactional nonblocking method, enqueues messages internally, throws exception if local queue is full
   * 
   * @return ListenableFuture that gets completed after successful sending
   */
  public ListenableFuture<Void> send(Destination destination, Message... messages) {
    return send(destination, Arrays.asList(messages));
  }

  /**
   * Nontransactional nonblocking method, enqueues messages internally, throws exception if local queue is full
   * 
   * @return ListenableFuture that gets completed after successful sending
   */
  public ListenableFuture<Void> send(Destination destination, Collection<Message> messages) {
    PublishTaskFuture future = new PublishTaskFuture(destination, messages, false);
    taskQueue.add(future);
    return future;
  }

  /**
   * Nontransactional nonblocking method, enqueues messages internally, throws exception if local queue is full
   * 
   * @return ListenableFuture that gets completed after successful sending
   */
  public ListenableFuture<Void> send(Map<Message, Destination> messages) {
    PublishTaskFuture future = new PublishTaskFuture(messages, false);
    taskQueue.add(future);
    return future;
  }
  

  /**
   * Transactional nonblocking method, enqueues messages internally, throws exception if local queue is full
   * 
   * @return ListenableFuture that gets completed after successful sending
   */
  public ListenableFuture<Void> sendTransactional(Destination destination, Message... messages) {
    return sendTransactional(destination, Arrays.asList(messages));
  }

  /**
   * Transactional nonblocking method, enqueues messages internally, throws exception if local queue is full
   * 
   * @return ListenableFuture that gets completed after successful sending
   */
  public ListenableFuture<Void> sendTransactional(Destination destination, Collection<Message> messages) {
    PublishTaskFuture future = new PublishTaskFuture(destination, messages, true);
    taskQueue.add(future);
    return future;
  }
  
  /**
   * Transactional nonblocking method, enqueues messages internally, throws exception if local queue is full
   * 
   * @return ListenableFuture that gets completed after successful sending
   */
  public ListenableFuture<Void> sendTransactional(Map<Message, Destination> messages) {
    PublishTaskFuture future = new PublishTaskFuture(messages, true);
    taskQueue.add(future);
    return future;
  }
}
