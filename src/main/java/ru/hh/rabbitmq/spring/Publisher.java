package ru.hh.rabbitmq.spring;

import static com.google.common.base.Preconditions.checkNotNull;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_EXCHANGE;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_INNER_QUEUE_SIZE;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_MANDATORY;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_RECONNECTION_DELAY;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_ROUTING_KEY;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_TRANSACTIONAL;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ConfirmCallback;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ReturnCallback;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;

import ru.hh.rabbitmq.spring.send.ChannelWorker;
import ru.hh.rabbitmq.spring.send.Destination;
import ru.hh.rabbitmq.spring.send.PublishTaskFuture;
import ru.hh.rabbitmq.spring.send.QueueIsFullException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

/**
 * <p>
 * Helper class that hides implementation of multiple broker containers and provides basic methods for configuring them as batch. Use
 * {@link #getContainers()} to specify other configuration parameters.
 * </p>
 * <p>
 * See {@link ConfigKeys} constants for configuration options.
 * </p>
 */
public class Publisher {

  public static final Logger logger = LoggerFactory.getLogger(Publisher.class);

  private static final int DEFAULT_INNER_QUEUE_SIZE = 1000;

  private int innerQueueSize;

  private List<RabbitTemplate> templates;
  private final List<Service> workers = new ArrayList<Service>();
  private BlockingQueue<PublishTaskFuture> taskQueue;

  private int reconnectionDelay = 1000;

  Publisher(List<ConnectionFactory> connectionFactories, Properties properties) {
    PropertiesHelper props = new PropertiesHelper(properties);
    List<RabbitTemplate> templates = Lists.newArrayList();
    for (ConnectionFactory factory : connectionFactories) {
      RabbitTemplate template = new RabbitTemplate(factory);

      innerQueueSize = props.integer(PUBLISHER_INNER_QUEUE_SIZE, DEFAULT_INNER_QUEUE_SIZE);

      String exchange = props.string(PUBLISHER_EXCHANGE);
      if (exchange != null) {
        template.setExchange(exchange);
      }

      String routingKey = props.string(PUBLISHER_ROUTING_KEY);
      if (routingKey != null) {
        template.setRoutingKey(routingKey);
      }

      Boolean mandatory = props.bool(PUBLISHER_MANDATORY);
      if (mandatory != null) {
        template.setMandatory(mandatory);
      }

      Boolean transactional = props.bool(PUBLISHER_TRANSACTIONAL);
      if (transactional != null) {
        template.setChannelTransacted(transactional);
      }

      templates.add(template);

      Integer reconnectionDelay = props.integer(PUBLISHER_RECONNECTION_DELAY);
      if (reconnectionDelay != null) {
        this.reconnectionDelay = reconnectionDelay;
      }
    }
    this.templates = ImmutableList.copyOf(templates);
  }

  /**
   * Returns immutable list of all rabbit templates for additional configuration. Doing this after {@link #start()} might lead to unexpected behavior.
   * 
   * @return list of all rabbit templates
   */
  public List<RabbitTemplate> getRabbitTemplates() {
    return templates;
  }

  /**
   * Set transactional mode. Must be called before {@link #start()}.
   * 
   * @param transactional
   * @return this
   */
  public Publisher setTransactional(boolean transactional) {
    for (RabbitTemplate template : templates) {
      template.setChannelTransacted(transactional);
    }
    return this;
  }

  /**
   * Use provided converter for message conversion. Must be called before {@link #start()}.
   * 
   * @param converter
   * @return this
   */
  public Publisher withMessageConverter(MessageConverter converter) {
    checkNotStarted();
    for (RabbitTemplate template : templates) {
      template.setMessageConverter(converter);
    }
    return this;
  }

  /**
   * Use {@link Jackson2JsonMessageConverter} for message conversion. Must be called before {@link #start()}.
   * 
   * @return this
   */
  public Publisher withJsonMessageConverter() {
    checkNotStarted();
    Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter();
    return withMessageConverter(converter);
  }

  /**
   * Specify confirm callback. {@link ConfigKeys#PUBLISHER_CONFIRMS} should be set to true. Must be called before {@link #start()}.
   * 
   * @param callback
   * @return this
   */
  public Publisher withConfirmCallback(ConfirmCallback callback) {
    checkNotStarted();
    for (RabbitTemplate template : templates) {
      template.setConfirmCallback(callback);
    }
    return this;
  }

  /**
   * Specify return callback. {@link ConfigKeys#PUBLISHER_RETURNS} should be set to true. Must be called before {@link #start()}.
   * 
   * @param callback
   * @return
   */
  public Publisher withReturnCallback(ReturnCallback callback) {
    checkNotStarted();
    for (RabbitTemplate template : templates) {
      template.setReturnCallback(callback);
    }
    return this;
  }

  /**
   * Starting publisher, includes starting threads that process inner queue.
   * 
   * @return this
   */
  public Publisher start() {
    checkNotStarted();
    taskQueue = new ArrayBlockingQueue<PublishTaskFuture>(innerQueueSize);
    for (RabbitTemplate template : templates) {
      ConnectionFactory factory = template.getConnectionFactory();
      Service worker = new ChannelWorker(template, taskQueue, "rabbit-publisher-" + factory.getHost() + ":" + factory.getPort(), reconnectionDelay);
      workers.add(worker);
      worker.start();
    }
    return this;
  }

  /**
   * Stop processing inner queue, release all connections and thread pools.
   */
  public void shutdown() {
    checkStarted();
    for (Service worker : workers) {
      worker.stopAndWait();
    }
    for (RabbitTemplate template : templates) {
      CachingConnectionFactory factory = (CachingConnectionFactory) template.getConnectionFactory();
      factory.destroy();
    }
    taskQueue = null;
  }

  /**
   * Nonblocking method, enqueues messages internally, throws exception if local queue is full
   * 
   * @return ListenableFuture that gets completed after successful sending
   */
  public ListenableFuture<Void> send(Destination destination, Object... messages) {
    return send(destination, Arrays.asList(messages));
  }

  /**
   * Nonblocking method, enqueues messages internally, throws exception if local queue is full
   * 
   * @return ListenableFuture that gets completed after successful sending
   */
  public ListenableFuture<Void> send(Destination destination, Collection<Object> messages) {
    checkNotNull(destination, "Destination can't be null");
    PublishTaskFuture future = new PublishTaskFuture(destination, messages);
    addFuture(future);
    return future;
  }

  /**
   * Nonblocking method, enqueues messages internally, throws exception if local queue is full
   * 
   * @return ListenableFuture that gets completed after successful sending
   */
  public ListenableFuture<Void> send(Map<Object, Destination> messages) {
    for (Destination destination : messages.values()) {
      checkNotNull(destination, "Destination can't be null");
    }
    PublishTaskFuture future = new PublishTaskFuture(messages);
    addFuture(future);
    return future;
  }

  /**
   * <p>
   * Nonblocking method, enqueues messages internally, throws exception if local queue is full
   * </p>
   * <p>
   * Configuration options {@link ConfigKeys#PUBLISHER_EXCHANGE} and {@link ConfigKeys#PUBLISHER_ROUTING_KEY} must be set.
   * </p>
   * 
   * @return ListenableFuture that gets completed after successful sending
   */
  public ListenableFuture<Void> send(Object... messages) {
    return send(Arrays.asList(messages));
  }

  /**
   * <p>
   * Nonblocking method, enqueues messages internally, throws exception if local queue is full
   * </p>
   * <p>
   * Configuration options {@link ConfigKeys#PUBLISHER_EXCHANGE} and {@link ConfigKeys#PUBLISHER_ROUTING_KEY} must be set.
   * </p>
   * 
   * @return ListenableFuture that gets completed after successful sending
   */
  public ListenableFuture<Void> send(Collection<Object> messages) {
    PublishTaskFuture future = new PublishTaskFuture(null, messages);
    addFuture(future);
    return future;
  }

  private void addFuture(PublishTaskFuture future) {
    checkStarted();
    try {
      taskQueue.add(future);
      logger.trace("task added with {} messages, queue size is {}", future.getMessages().size(), taskQueue.size());
    }
    catch (IllegalStateException e) {
      throw new QueueIsFullException(e);
    }
  }

  public int getInnerQueueSize() {
    checkStarted();
    return taskQueue.size();
  }

  public int getInnerQueueRemainingCapacity() {
    checkStarted();
    return taskQueue.remainingCapacity();
  }

  private void checkStarted() {
    if (taskQueue == null) {
      throw new IllegalStateException("Publisher was not started");
    }
  }

  private void checkNotStarted() {
    if (taskQueue != null) {
      throw new IllegalStateException("Publisher was already started");
    }
  }
}
