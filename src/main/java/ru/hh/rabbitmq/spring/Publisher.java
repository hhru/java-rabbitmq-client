package ru.hh.rabbitmq.spring;

import static com.google.common.base.Preconditions.checkNotNull;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_EXCHANGE;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_INNER_QUEUE_SIZE;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_MANDATORY;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_NAME;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_RECONNECTION_DELAY;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_ROUTING_KEY;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_TRANSACTIONAL;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractService;
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
 * <p>
 * Publisher is not restartable - once {@link #stop()} is called, calling {@link #start()} will do nothing.
 */
public class Publisher extends AbstractService {

  private static final Logger LOGGER = LoggerFactory.getLogger(Publisher.class);

  private static final int DEFAULT_INNER_QUEUE_SIZE = 1000;

  private int innerQueueSize;

  private final Map<RabbitTemplate, String> templates;
  private final List<Service> workers = new ArrayList<>();
  private BlockingQueue<PublishTaskFuture> taskQueue;

  private int reconnectionDelay = 1000;

  Publisher(List<ConnectionFactory> connectionFactories, Properties properties) {
    PropertiesHelper props = new PropertiesHelper(properties);
    Map<RabbitTemplate, String> templates = new LinkedHashMap<>(connectionFactories.size());

    String commonName = props.string(PUBLISHER_NAME, "");

    for (ConnectionFactory factory : connectionFactories) {
      RabbitTemplate template = new RabbitTemplate(factory);

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

      String name = "rabbit-publisher-" + commonName + "-" + factory.getHost() + ":" + factory.getPort();
      templates.put(template, name);
    }

    innerQueueSize = props.integer(PUBLISHER_INNER_QUEUE_SIZE, DEFAULT_INNER_QUEUE_SIZE);

    Integer reconnectionDelay = props.integer(PUBLISHER_RECONNECTION_DELAY);
    if (reconnectionDelay != null) {
      this.reconnectionDelay = reconnectionDelay;
    }

    this.templates = ImmutableMap.copyOf(templates);
  }

  /**
   * Returns immutable collection of all rabbit templates for additional configuration. Doing this after {@link #start()} might lead to unexpected
   * behavior.
   * 
   * @return list of all rabbit templates
   */
  public Collection<RabbitTemplate> getRabbitTemplates() {
    return templates.keySet();
  }

  /**
   * Set transactional mode. Must be called before {@link #start()}.
   * 
   * @param transactional
   * @return this
   */
  public Publisher setTransactional(boolean transactional) {
    checkNotStarted();
    for (RabbitTemplate template : templates.keySet()) {
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
    for (RabbitTemplate template : templates.keySet()) {
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
    for (RabbitTemplate template : templates.keySet()) {
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
    for (RabbitTemplate template : templates.keySet()) {
      template.setReturnCallback(callback);
    }
    return this;
  }

  @Override
  protected void doStart() {
    checkNotStarted();
    taskQueue = new ArrayBlockingQueue<PublishTaskFuture>(innerQueueSize);
    for (Entry<RabbitTemplate, String> entry : templates.entrySet()) {
      RabbitTemplate template = entry.getKey();
      String name = entry.getValue();
      Service worker = new ChannelWorker(template, taskQueue, name, reconnectionDelay);
      workers.add(worker);
      worker.start();
    }
    notifyStarted();
    LOGGER.debug("started " + toString());
  }

  @Override
  protected void doStop() {
    checkStarted();
    for (Service worker : workers) {
      worker.stopAndWait();
    }
    for (RabbitTemplate template : templates.keySet()) {
      CachingConnectionFactory factory = (CachingConnectionFactory) template.getConnectionFactory();
      factory.destroy();
    }
    taskQueue = null;
    notifyStopped();
    LOGGER.debug("stopped " + toString());
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
      LOGGER.trace("task added with {} messages, queue size is {}", future.getMessages().size(), taskQueue.size());
    }
    catch (IllegalStateException e) {
      throw new QueueIsFullException(toString(), e);
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

  private boolean isStarted() {
    return taskQueue != null;
  }

  private void checkStarted() {
    if (!isStarted()) {
      throw new IllegalStateException("Publisher was not started for " + toString());
    }
  }

  private void checkNotStarted() {
    if (isStarted()) {
      throw new IllegalStateException("Publisher was already started for " + toString());
    }
  }

  @Override
  public String toString() {
    if (templates == null) {
      return "uninitialized";
    }
    return Joiner.on(',').join(templates.values());
  }

}
