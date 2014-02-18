package ru.hh.rabbitmq.spring;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static ru.hh.rabbitmq.spring.ConfigKeys.RECEIVER_NAME;
import static ru.hh.rabbitmq.spring.ConfigKeys.RECEIVER_PREFETCH_COUNT;
import static ru.hh.rabbitmq.spring.ConfigKeys.RECEIVER_QUEUES;
import static ru.hh.rabbitmq.spring.ConfigKeys.RECEIVER_QUEUES_SEPARATOR;
import static ru.hh.rabbitmq.spring.ConfigKeys.RECEIVER_THREADPOOL;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.util.ErrorHandler;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * <p>
 * Helper class that hides implementation of multiple broker containers and provides basic methods for configuring them as batch. Use
 * {@link #getContainers()} to specify other configuration parameters.
 * </p>
 * <p>
 * See {@link ConfigKeys} constants for configuration options.
 * </p>
 */
public class Receiver extends AbstractService {

  private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);

  private Map<SimpleMessageListenerContainer, ExecutorService> containers;
  private Map<SimpleMessageListenerContainer, String> names;

  Receiver(List<ConnectionFactory> connectionFactories, Properties properties) {
    PropertiesHelper props = new PropertiesHelper(properties);
    Map<SimpleMessageListenerContainer, ExecutorService> containers = new LinkedHashMap<>(connectionFactories.size());
    Map<SimpleMessageListenerContainer, String> names = new LinkedHashMap<>(connectionFactories.size());

    String commonName = props.string(RECEIVER_NAME, "");

    for (ConnectionFactory factory : connectionFactories) {
      SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(factory);

      // set default queue names
      String queueNames = props.string(RECEIVER_QUEUES);
      if (queueNames != null) {
        Iterable<String> queueNamesList = Splitter.on(RECEIVER_QUEUES_SEPARATOR).split(queueNames);
        container.setQueueNames(Iterables.toArray(queueNamesList, String.class));
      }

      // configure thread pool
      String name = "rabbit-receiver-" + commonName + "-" + factory.getHost() + ":" + factory.getPort();

      Integer threadPoolSize = props.integer(RECEIVER_THREADPOOL, 1);
      ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(name + "-%d").build();
      ExecutorService executor = newFixedThreadPool(threadPoolSize, threadFactory);
      container.setTaskExecutor(executor);
      container.setConcurrentConsumers(threadPoolSize);

      // configure prefetch count
      Integer prefetchCount = props.integer(RECEIVER_PREFETCH_COUNT);
      if (prefetchCount != null) {
        container.setPrefetchCount(prefetchCount);
      }

      containers.put(container, executor);
      names.put(container, name);
    }
    this.containers = ImmutableMap.copyOf(containers);
    this.names = ImmutableMap.copyOf(names);
  }

  /**
   * Returns immutable list of all broker containers for additional configuration. Doing this after {@link #start()} might lead to unexpected
   * behavior.
   * 
   * @return list of all broker containers
   */
  public Iterable<SimpleMessageListenerContainer> getContainers() {
    return containers.keySet();
  }

  /**
   * Set queue names. Each call of this method overrides previously set queue names. If default queue names were specified using
   * {@link ConfigKeys#RECEIVER_QUEUES} they will be overridden as well. Must be called before {@link #start()}.
   * 
   * @param names
   *          queue names to set
   * @return this
   */
  public Receiver forQueues(String... names) {
    checkNotStarted();
    for (SimpleMessageListenerContainer container : containers.keySet()) {
      container.setQueueNames(names);
    }
    return this;
  }

  /**
   * Set listener that will receive and process messages. If listener implements {@link ErrorHandler}, it will be set to handle errors as well. Must
   * be called before {@link #start()}.
   * 
   * Conversion of messages is performed using {@link SimpleMessageConverter}.
   * 
   * @param listener
   *          listener to set
   * @return this
   */
  public Receiver withListener(Object listener) {
    checkNotStarted();
    if (ErrorHandler.class.isAssignableFrom(listener.getClass())) {
      withErrorHandler((ErrorHandler) listener);
    }
    for (SimpleMessageListenerContainer container : containers.keySet()) {
      container.setMessageListener(listener);
    }
    return this;
  }

  /**
   * Set listener and converter that will receive and process messages. If listener implements {@link ErrorHandler}, it will be set to handle errors
   * as well. Must be called before {@link #start()}.
   * 
   * @param listener
   *          listener to set
   * @param converter
   *          converter to use
   * @return this
   */
  public Receiver withListenerAndConverter(Object listener, MessageConverter converter) {
    checkNotStarted();
    MessageListenerAdapter adapter = new MessageListenerAdapter(listener, converter);
    if (ErrorHandler.class.isAssignableFrom(listener.getClass())) {
      withErrorHandler((ErrorHandler) listener);
    }
    return withListener(adapter);
  }

  /**
   * Set error handler. Must be called before {@link #start()}.
   * 
   * @param errorHandler
   *          error handler to set
   * @return this
   */
  public Receiver withErrorHandler(ErrorHandler errorHandler) {
    checkNotStarted();
    for (SimpleMessageListenerContainer container : containers.keySet()) {
      container.setErrorHandler(errorHandler);
    }
    return this;
  }

  /**
   * Set listener that will receive json messages. Conversion to java types is performed by Jackson2. See {@link Jackson2JsonMessageConverter} for
   * details. If listener implements {@link ErrorHandler}, it will be set to handle errors as well. Must be called before {@link #start()}.
   * 
   * @param listener
   *          listener to set
   * @return this
   */
  public Receiver withJsonListener(Object listener) {
    checkNotStarted();
    Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter();
    MessageListenerAdapter adapter = new MessageListenerAdapter(listener, converter);
    if (ErrorHandler.class.isAssignableFrom(listener.getClass())) {
      withErrorHandler((ErrorHandler) listener);
    }
    return withListener(adapter);
  }

  private boolean isStarted() {
    boolean started = true;
    for (SimpleMessageListenerContainer container : containers.keySet()) {
      started &= container.isActive();
    }
    return started;
  }

  private void checkStarted() {
    if (!isStarted()) {
      throw new IllegalStateException("Receiver was not started for " + toString());
    }
  }

  private void checkNotStarted() {
    if (isStarted()) {
      throw new IllegalStateException("Receiver was already started for " + toString());
    }
  }

  @Override
  protected void doStart() {
    checkNotStarted();
    for (SimpleMessageListenerContainer container : containers.keySet()) {
      container.start();
    }
    notifyStarted();
    LOGGER.debug("started " + toString());
  }

  @Override
  protected void doStop() {
    checkStarted();
    for (SimpleMessageListenerContainer container : containers.keySet()) {
      container.shutdown();
      CachingConnectionFactory factory = (CachingConnectionFactory) container.getConnectionFactory();
      factory.destroy();
    }
    for (ExecutorService executor : containers.values()) {
      executor.shutdown();
    }
    notifyStopped();
    LOGGER.debug("stopped " + toString());
  }

  @Override
  public String toString() {
    if (names == null) {
      return "uninitialized";
    }
    return Joiner.on(',').join(names.values());
  }

}
