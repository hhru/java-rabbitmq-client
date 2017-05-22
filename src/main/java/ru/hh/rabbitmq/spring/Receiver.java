package ru.hh.rabbitmq.spring;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import static java.util.concurrent.Executors.newFixedThreadPool;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.util.ErrorHandler;
import ru.hh.metrics.Counters;
import ru.hh.metrics.StatsDSender;
import ru.hh.metrics.Tag;
import static ru.hh.rabbitmq.spring.ConfigKeys.RECEIVER_NAME;
import static ru.hh.rabbitmq.spring.ConfigKeys.RECEIVER_PREFETCH_COUNT;
import static ru.hh.rabbitmq.spring.ConfigKeys.RECEIVER_QUEUES;
import static ru.hh.rabbitmq.spring.ConfigKeys.RECEIVER_QUEUES_SEPARATOR;
import static ru.hh.rabbitmq.spring.ConfigKeys.RECEIVER_THREADPOOL;
import static ru.hh.rabbitmq.spring.ConfigKeys.RECEIVER_USE_MDC;
import ru.hh.rabbitmq.spring.receive.GenericMessageListener;

/**
 * <p>
 * Helper class that hides implementation of multiple broker containers and provides basic methods for configuring them as batch. Use
 * {@link #getContainers()} to specify other configuration parameters.
 * </p>
 * <p>
 * See {@link ConfigKeys} constants for configuration options.
 * </p>
 */
public class Receiver {

  private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);

  private Map<SimpleMessageListenerContainer, ExecutorService> containers;
  private Map<SimpleMessageListenerContainer, String> names;

  @Nullable  // when monitoring is turned off
  private Counters receiverCounters;

  private AtomicBoolean shutDown = new AtomicBoolean(false);

  Receiver(List<ConnectionFactory> connectionFactories,
           Properties properties,
           @Nullable
           String serviceName,
           @Nullable
           StatsDSender statsDSender) {
    PropertiesHelper props = new PropertiesHelper(properties);
    Map<SimpleMessageListenerContainer, ExecutorService> containers = new LinkedHashMap<>(connectionFactories.size());
    Map<SimpleMessageListenerContainer, String> names = new LinkedHashMap<>(connectionFactories.size());

    String commonName = props.getString(RECEIVER_NAME, "");
    String queueNames = props.getString(RECEIVER_QUEUES);
    int threadPoolSize = props.getInteger(RECEIVER_THREADPOOL, 1);
    Long shutdownTimeout = props.getLong(ConfigKeys.RECEIVER_SHUTDOWN_TIMEOUT);
    Integer prefetchCount = props.getInteger(RECEIVER_PREFETCH_COUNT);
    boolean useMDC = props.getBoolean(RECEIVER_USE_MDC, false);

    for (ConnectionFactory factory : connectionFactories) {
      SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(factory);
      container.setAutoDeclare(false);

      // set default queue names
      if (queueNames != null) {
        Iterable<String> queueNamesList = Splitter.on(RECEIVER_QUEUES_SEPARATOR).split(queueNames);
        container.setQueueNames(Iterables.toArray(queueNamesList, String.class));
      }

      // configure thread pool
      String name = "rabbit-receiver-" + commonName + "-" + factory.getHost() + ":" + factory.getPort();

      ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(name + "-%d").build();
      ExecutorService executor = newFixedThreadPool(threadPoolSize, threadFactory);
      container.setTaskExecutor(executor);
      container.setConcurrentConsumers(threadPoolSize);
      if (shutdownTimeout != null) {
        container.setShutdownTimeout(shutdownTimeout);
      }

      // configure prefetch count
      if (prefetchCount != null) {
        container.setPrefetchCount(prefetchCount);
      }

      if (useMDC) {
        container.setMessagePropertiesConverter(new MDCMessagePropertiesConverter());
      }

      containers.put(container, executor);
      names.put(container, name);
    }
    this.containers = ImmutableMap.copyOf(containers);
    this.names = ImmutableMap.copyOf(names);

    if (statsDSender != null) {
      receiverCounters = new Counters(20);
      statsDSender.sendCountersPeriodically(serviceName + ".rabbit.receivers.messages", receiverCounters);
    }
  }

  /**
   * Returns immutable list of all broker containers for additional configuration. Doing this after {@link #start()} might lead to unexpected
   * behavior.
   * 
   * @return list of all broker containers
   */
  public Iterable<SimpleMessageListenerContainer> getContainers() {
    checkNotShutDown();
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
  public Receiver withListener(MessageListener listener) {
    return withListenerObject(listener);
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
  public Receiver withListener(ChannelAwareMessageListener listener) {
    return withListenerObject(listener);
  }

  private Receiver withListenerObject(Object listener) {
    checkNotStarted();
    if (ErrorHandler.class.isAssignableFrom(listener.getClass())) {
      withErrorHandler((ErrorHandler) listener);
    }

    if (receiverCounters != null) {
      listener = wrapIntoMonitoringListener(listener);
    }

    for (SimpleMessageListenerContainer container : containers.keySet()) {
      container.setMessageListener(listener);
    }
    return this;
  }

  private Object wrapIntoMonitoringListener(Object listener) {
    if (listener instanceof MessageListener) {
      MessageListener messageListener = (MessageListener) listener;
      return (MessageListener) message -> {
        messageListener.onMessage(message);
        increaseReceivedMessageCount(message);
      };
    } else {
      ChannelAwareMessageListener channelAwareMessageListener = (ChannelAwareMessageListener) listener;
      return (ChannelAwareMessageListener) (message, channel) -> {
        channelAwareMessageListener.onMessage(message, channel);
        increaseReceivedMessageCount(message);
      };
    }
  }

  private void increaseReceivedMessageCount(Message message) {
    receiverCounters.add(1, new Tag("queue", message.getMessageProperties().getConsumerQueue()));
  }

  /**
   * Set listener and converter that will receive and process messages. If listener implements {@link ErrorHandler}, it will be set to handle errors
   * as well. Must be called before {@link #start()}.
   * 
   * See {@link MessageListenerAdapter} documentation on how to name listener handling method.
   * 
   * @param listener
   *          listener to set
   * @param converter
   *          converter to use
   * @return this
   */
  public Receiver withListenerAndConverter(GenericMessageListener<?> listener, MessageConverter converter) {
    checkNotStarted();
    MessageListenerAdapter adapter = new MessageListenerAdapter(listener, converter);
    if (ErrorHandler.class.isAssignableFrom(listener.getClass())) {
      withErrorHandler((ErrorHandler) listener);
    }
    return withListenerObject(adapter);
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
  public Receiver withJsonListener(GenericMessageListener<?> listener) {
    checkNotStarted();
    Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter();
    MessageListenerAdapter adapter = new MessageListenerAdapter(listener, converter);
    if (ErrorHandler.class.isAssignableFrom(listener.getClass())) {
      withErrorHandler((ErrorHandler) listener);
    }
    return withListenerObject(adapter);
  }

  public boolean isActive() {
    boolean started = true;
    for (SimpleMessageListenerContainer container : containers.keySet()) {
      started &= container.isActive();
    }
    return started;
  }

  public boolean isShutDown() {
    return shutDown.get();
  }

  private void checkStarted() {
    checkNotShutDown();
    if (!isActive()) {
      throw new IllegalStateException("Receiver was not started for " + toString());
    }
  }

  private void checkNotStarted() {
    checkNotShutDown();
    if (isActive()) {
      throw new IllegalStateException("Receiver was already started for " + toString());
    }
  }

  private void checkNotShutDown() {
    if (shutDown.get()) {
      throw new IllegalStateException("Receiver was shut down for " + toString());
    }
  }

  /**
   * Start receiving messages.
   */
  public Receiver start() {
    checkNotStarted();
    for (SimpleMessageListenerContainer container : containers.keySet()) {
      container.start();
    }
    LOGGER.debug("started {}", toString());
    return this;
  }

  /**
   * Stop (pause) receiving messages. Once this is called, configuration methods can be used again. Previously set configuration parameters are
   * preserved.
   */
  public Receiver stop() {
    checkStarted();
    doStop();
    return this;
  }

  private void doStop() {
    for (SimpleMessageListenerContainer container : containers.keySet()) {
      container.stop();
    }
    LOGGER.debug("stopped {}", toString());
  }

  /**
   * Stop receiving messages, release all resources. Once called, this instance can't be used again. Will attempt to stop all actively executing
   * tasks, halts the processing of waiting tasks in underlying Executor Services
   */
  public void shutdownNow() {
    shutdown(true);
  }

  /**
   * Stop receiving messages, release all resources. Once called, this instance can't be used again.
   */
  public void shutdown() {
    shutdown(false);
  }

  /**
   * Stop receiving messages, release all resources. Once called, this instance can't be used again.
   * @param now if true will attempts to stop all actively executing tasks, halts the processing of waiting tasks in underlying Executor Services
   * that's it {@link java.util.concurrent.ExecutorService#shutdownNow()} vs {@link java.util.concurrent.ExecutorService#shutdown()}
   */
  public void shutdown(boolean now) {
    if (!shutDown.compareAndSet(false, true)) {
      throw new IllegalStateException("Already shut down: " + toString());
    }
    if (isActive()) {
      doStop();
    }
    for (SimpleMessageListenerContainer container : containers.keySet()) {
      container.shutdown();
      CachingConnectionFactory factory = (CachingConnectionFactory) container.getConnectionFactory();
      factory.destroy();
    }
    for (ExecutorService executor : containers.values()) {
      if (now) {
        executor.shutdownNow();
      } else {
        executor.shutdown();
      }
    }
    LOGGER.debug("shut down {}", toString());
  }

  @Override
  public String toString() {
    if (names == null) {
      return "uninitialized";
    }
    return Joiner.on(',').join(names.values());
  }

}
