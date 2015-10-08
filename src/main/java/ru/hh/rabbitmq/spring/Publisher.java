package ru.hh.rabbitmq.spring;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static ru.hh.rabbitmq.spring.ConfigKeys.CONNECTION_FACTORY_TEST_RETRY_DELAY_MS;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_EXCHANGE;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_INNER_QUEUE_SHUTDOWN_MS;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_INNER_QUEUE_SIZE;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_MANDATORY;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_NAME;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_ROUTING_KEY;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_TRANSACTIONAL;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_USE_MDC;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ConfirmCallback;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ReturnCallback;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import ru.hh.rabbitmq.spring.send.ChannelWorker;
import ru.hh.rabbitmq.spring.send.CorrelatedMessage;
import ru.hh.rabbitmq.spring.send.Destination;
import ru.hh.rabbitmq.spring.send.PublishTaskFuture;
import ru.hh.rabbitmq.spring.send.QueueIsFullException;
import com.google.common.base.Joiner;
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

  private final BlockingQueue<PublishTaskFuture> taskQueue;

  private final Collection<RabbitTemplate> templates;
  private final Collection<Service> workers;
  private final String name;

  private final boolean useMDC;
  private long innerQueueShutdownMs = 1000;

  Publisher(List<ConnectionFactory> connectionFactories, Properties properties) {
    PropertiesHelper props = new PropertiesHelper(properties);
    List<RabbitTemplate> templates = new ArrayList<>(connectionFactories.size());
    List<Service> workers = new ArrayList<>(connectionFactories.size());
    List<String> connectionFactoriesNames = new ArrayList<>(connectionFactories.size());

    String commonName = props.getString(PUBLISHER_NAME, "");

    String exchange = props.getString(PUBLISHER_EXCHANGE);
    String routingKey = props.getString(PUBLISHER_ROUTING_KEY);
    Boolean mandatory = props.getBoolean(PUBLISHER_MANDATORY);
    Boolean transactional = props.getBoolean(PUBLISHER_TRANSACTIONAL);
    useMDC = props.getBoolean(PUBLISHER_USE_MDC, false);

    int innerQueueSize = props.getInteger(PUBLISHER_INNER_QUEUE_SIZE, DEFAULT_INNER_QUEUE_SIZE);
    taskQueue = new ArrayBlockingQueue<>(innerQueueSize);

    int templateTestRetryMs = props.getInteger(CONNECTION_FACTORY_TEST_RETRY_DELAY_MS, 2000);

    for (ConnectionFactory factory : connectionFactories) {
      RabbitTemplate template = new RabbitTemplate(factory);

      if (exchange != null) {
        template.setExchange(exchange);
      }

      if (routingKey != null) {
        template.setRoutingKey(routingKey);
      }

      if (mandatory != null) {
        template.setMandatory(mandatory);
      }

      if (transactional != null) {
        template.setChannelTransacted(transactional);
      }

      if (useMDC) {
        template.setMessagePropertiesConverter(new MDCMessagePropertiesConverter());
      }

      templates.add(template);

      String connectionFactoryName = factory.getHost() + ':' + factory.getPort();

      String workerName = "rabbit-publisher-" + commonName + '-' + connectionFactoryName;
      Service worker = new ChannelWorker(template, taskQueue, workerName, templateTestRetryMs);
      workers.add(worker);

      connectionFactoriesNames.add(connectionFactoryName);
    }

    this.templates = Collections.unmodifiableList(templates);
    this.workers = Collections.unmodifiableList(workers);
    name = getClass().getSimpleName() + '{' + commonName + ',' + Joiner.on(',').join(connectionFactoriesNames) + '}';

    Long innerQueueShutdownMs = props.getLong(PUBLISHER_INNER_QUEUE_SHUTDOWN_MS);
    if (innerQueueShutdownMs != null) {
      this.innerQueueShutdownMs = innerQueueShutdownMs;
    }
  }

  /**
   * Returns immutable collection of all rabbit templates for additional configuration. Doing this after {@link #start()} might lead to unexpected
   * behavior.
   *
   * @return list of all rabbit templates
   */
  public Collection<RabbitTemplate> getRabbitTemplates() {
    return templates;
  }

  /**
   * Set transactional mode. Must be called before {@link #start()}.
   *
   * @param transactional
   * @return this
   */
  public Publisher setTransactional(boolean transactional) {
    checkNotStarted();
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

  public void startSync() {
    startAsync();
    awaitRunning();
  }

  public void startSync(long timeout, TimeUnit timeUnit) throws TimeoutException {
    startAsync();
    awaitRunning(timeout, timeUnit);
  }

  @Override
  protected void doStart() {
    checkNotStarted();
    for (Service worker: workers) {
      worker.startAsync();
    }
    notifyStarted();
    LOGGER.debug("started " + toString());
  }

  public void stopSync() {
    stopAsync();
    awaitTerminated();
  }

  public void stopSync(long timeout, TimeUnit timeUnit) throws TimeoutException {
    stopAsync();
    awaitTerminated(timeout, timeUnit);
  }

  @Override
  protected void doStop() {
    // wait till inner queue is empty
    long maxWaitTimeMs = currentTimeMillis() + innerQueueShutdownMs;
    while (currentTimeMillis() < maxWaitTimeMs && !taskQueue.isEmpty()) {
      sleepUninterruptibly(100, MILLISECONDS);
    }
    if (!taskQueue.isEmpty()) {
      LOGGER.warn("Shutting down with {} tasks still in inner queue, they will be dropped", taskQueue.size());
    }
    for (Service worker : workers) {
      worker.stopAsync();
      worker.awaitTerminated();
    }
    for (RabbitTemplate template : templates) {
      CachingConnectionFactory factory = (CachingConnectionFactory) template.getConnectionFactory();
      factory.destroy();
    }
    notifyStopped();
    LOGGER.debug("stopped " + toString());
  }


  /**
   * Potentially blocking method, enqueues messages internally, waiting if necessary, throws exception if local queue is full.
   * <p>
   * Wrap message with {@link CorrelatedMessage} to attach {@link CorrelationData} for publisher confirms.
   * </p>
   *
   * @return ListenableFuture that gets completed after successful sending
   * @throws InterruptedException
   */
  public ListenableFuture<Void> offer(long timeoutMs, Destination destination, Object... messages) throws InterruptedException {
    return offer(timeoutMs, destination, Arrays.asList(messages));
  }

  /**
   * Potentially blocking method, enqueues messages internally, waiting if necessary, throws exception if local queue is full.
   * <p>
   * Wrap message with {@link CorrelatedMessage} to attach {@link CorrelationData} for publisher confirms.
   * </p>
   *
   * @return ListenableFuture that gets completed after successful sending
   * @throws InterruptedException
   */
  public ListenableFuture<Void> offer(long timeoutMs, Destination destination, Collection<Object> messages) throws InterruptedException {
    checkNotNull(destination, "Destination can't be null");
    PublishTaskFuture future = new PublishTaskFuture(destination, messages);
    offerFuture(future, timeoutMs);
    return future;
  }

  /**
   * Potentially blocking method, enqueues messages internally, waiting if necessary, throws exception if local queue is full.
   * <p>
   * Wrap message with {@link CorrelatedMessage} to attach {@link CorrelationData} for publisher confirms.
   * </p>
   *
   * @return ListenableFuture that gets completed after successful sending
   * @throws InterruptedException
   */
  public ListenableFuture<Void> offer(long timeoutMs, Map<Object, Destination> messages) throws InterruptedException {
    for (Destination destination : messages.values()) {
      checkNotNull(destination, "Destination can't be null");
    }
    PublishTaskFuture future = new PublishTaskFuture(messages);
    offerFuture(future, timeoutMs);
    return future;
  }

  /**
   * <p>
   * Potentially blocking method, enqueues messages internally, waiting if necessary, throws exception if local queue is full.
   * </p>
   * <p>
   * Wrap message with {@link CorrelatedMessage} to attach {@link CorrelationData} for publisher confirms.
   * </p>
   * <p>
   * Configuration options {@link ConfigKeys#PUBLISHER_EXCHANGE} and {@link ConfigKeys#PUBLISHER_ROUTING_KEY} must be set.
   * </p>
   *
   * @return ListenableFuture that gets completed after successful sending
   * @throws InterruptedException
   */
  public ListenableFuture<Void> offer(long timeoutMs, Object... messages) throws InterruptedException {
    return offer(timeoutMs, Arrays.asList(messages));
  }

  /**
   * <p>
   * Potentially blocking method, enqueues messages internally, waiting if necessary, throws exception if local queue is full.
   * </p>
   * <p>
   * Wrap message with {@link CorrelatedMessage} to attach {@link CorrelationData} for publisher confirms.
   * </p>
   * <p>
   * Configuration options {@link ConfigKeys#PUBLISHER_EXCHANGE} and {@link ConfigKeys#PUBLISHER_ROUTING_KEY} must be set.
   * </p>
   *
   * @return ListenableFuture that gets completed after successful sending
   * @throws InterruptedException
   */
  public ListenableFuture<Void> offer(long timeoutMs, Collection<Object> messages) throws InterruptedException {
    PublishTaskFuture future = new PublishTaskFuture(null, messages);
    offerFuture(future, timeoutMs);
    return future;
  }

  private void offerFuture(PublishTaskFuture future, long timeoutMs) throws InterruptedException {
    checkAndCopyMDC(future);
    boolean added = taskQueue.offer(future, timeoutMs, MILLISECONDS);
    if (!added) {
      throw new QueueIsFullException(toString());
    }
    LOGGER.trace("task added with {} messages, queue size is {}", future.getMessages().size(), taskQueue.size());
  }

  /**
   * Nonblocking method, enqueues messages internally, throws exception if local queue is full.
   * <p>
   * Wrap message with {@link CorrelatedMessage} to attach {@link CorrelationData} for publisher confirms.
   * </p>
   *
   * @return ListenableFuture that gets completed after successful sending
   */
  public ListenableFuture<Void> send(Destination destination, Object... messages) {
    return send(destination, Arrays.asList(messages));
  }

  /**
   * Nonblocking method, enqueues messages internally, throws exception if local queue is full.
   * <p>
   * Wrap message with {@link CorrelatedMessage} to attach {@link CorrelationData} for publisher confirms.
   * </p>
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
   * Nonblocking method, enqueues messages internally, throws exception if local queue is full.
   * <p>
   * Wrap message with {@link CorrelatedMessage} to attach {@link CorrelationData} for publisher confirms.
   * </p>
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
   * Nonblocking method, enqueues messages internally, throws exception if local queue is full.
   * </p>
   * <p>
   * Wrap message with {@link CorrelatedMessage} to attach {@link CorrelationData} for publisher confirms.
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
   * Nonblocking method, enqueues messages internally, throws exception if local queue is full.
   * </p>
   * <p>
   * Wrap message with {@link CorrelatedMessage} to attach {@link CorrelationData} for publisher confirms.
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
    checkAndCopyMDC(future);
    try {
      taskQueue.add(future);
      LOGGER.trace("task added with {} messages, queue size is {}", future.getMessages().size(), taskQueue.size());
    }
    catch (IllegalStateException e) {
      throw new QueueIsFullException(toString(), e);
    }
  }

  private void checkAndCopyMDC(PublishTaskFuture future) {
    checkStarted();
    if (useMDC) {
      future.setMDCContext(MDC.getCopyOfContextMap());
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
    return isRunning();
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
    return name;
  }

}
