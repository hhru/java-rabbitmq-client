package ru.hh.rabbitmq.spring.send;

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.System.currentTimeMillis;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;
import ru.hh.metrics.StatsDSender;
import ru.hh.rabbitmq.spring.ConfigKeys;

public class Publisher extends AbstractService {

  private static final Logger LOGGER = LoggerFactory.getLogger(Publisher.class);

  private final BlockingQueue<PublishTaskFuture> taskQueue;

  private final Collection<ChannelWorker> workers;
  private final String name;
  private final boolean useMDC;
  private final int innerQueueShutdownMs;

  Publisher(String commonName,
            int innerQueueSize,
            Collection<RabbitTemplate> templates,
            int retryDelayMs,
            boolean useMDC,
            int innerQueueShutdownMs,
            @Nullable
            String serviceName,
            @Nullable
            StatsDSender statsDSender) {

    taskQueue = new ArrayBlockingQueue<>(innerQueueSize);

    final List<ChannelWorker> workers = new ArrayList<>(templates.size());
    final List<String> connectionFactoriesNames = new ArrayList<>(templates.size());
    for (RabbitTemplate template : templates) {
      ConnectionFactory connectionFactory = template.getConnectionFactory();

      String connectionFactoryName = connectionFactory.getHost() + ':' + connectionFactory.getPort();
      connectionFactoriesNames.add(connectionFactoryName);

      String workerName = "rabbit-publisher-" + commonName + '-' + connectionFactoryName;
      MessageSender messageSender = new MessageSender(template, serviceName, statsDSender);
      ChannelWorker worker = new ChannelWorker(workerName, messageSender, taskQueue, Duration.ofMillis(retryDelayMs));
      workers.add(worker);

      connectionFactoriesNames.add(connectionFactoryName);
    }
    this.workers = Collections.unmodifiableList(workers);
    name = getClass().getSimpleName() + '{' + commonName + ',' + String.join(",", connectionFactoriesNames) + '}';

    this.useMDC = useMDC;

    this.innerQueueShutdownMs = innerQueueShutdownMs;
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
    for (Service worker: workers) {
      worker.startAsync();
    }
    for (Service worker: workers) {
      worker.awaitRunning();
    }
    notifyStarted();
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
    }
    for (Service worker : workers) {
      worker.awaitTerminated();
    }

    notifyStopped();
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

  private void checkStarted() {
    if (!isRunning()) {
      throw new IllegalStateException("Publisher was not started for " + toString());
    }
  }

  @Override
  public String toString() {
    return name;
  }

}
