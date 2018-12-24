package ru.hh.rabbitmq.spring;

import org.springframework.amqp.rabbit.connection.AbstractConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;

import java.util.regex.Pattern;

public interface ConfigKeys {

  /**
   * If connections is used more than {@link #RECREATE_CONNECTION_INTERVAL_MINUTES} - connection needs to be reopen. It helps to keep balancing
   */
  String RECREATE_CONNECTION_INTERVAL_MINUTES = "connection.recreate.interval.mins";

  /**
   * Port for {@link #HOST} or {@link #HOSTS} (if it does not include it specifically).
   */
  String PORT = "port";
  /**
   * Single rabbitmq broker host. Is used if {@link #HOSTS} is not specified. Either this or {@link #HOSTS} is required.
   */
  String HOST = "host";
  /**
   * Multiple rabbitmq broker hosts in the form of 'host:port,host:port' or 'host,host'. Either this or {@link #HOST} is required.
   */
  String HOSTS = "hosts";
  String HOSTS_SEPARATOR = ",";
  Pattern HOSTS_SEPARATOR_PATTERN = Pattern.compile(HOSTS_SEPARATOR);
  String HOSTS_PORT_SEPARATOR = ":";
  Pattern HOSTS_PORT_SEPARATOR_PATTERN = Pattern.compile(HOSTS_PORT_SEPARATOR);
  /**
   * Same as {{@link #HOSTS} but for publisher only.
   */
  String PUBLISHER_HOSTS = "publisher.hosts";
  /**
   * Same as {{@link #HOSTS} but for receiver only.
   */
  String RECEIVER_HOSTS = "receiver.hosts";

  String CONNECTION_TIMEOUT_MS = "connection.timeout.millis";
  /**
   * Username (required), must be same for all brokers.
   */
  String USERNAME = "username";
  /**
   * Password (required), must be same for all brokers.
   */
  String PASSWORD = "password";
  /**
   * Virtualhost, must be same for all brokers.
   */
  String VIRTUALHOST = "virtualhost";
  /**
   * Heartbit interval in seconds, will be same for connections to all brokers. See {@link AbstractConnectionFactory#setRequestedHeartBeat(int)}.
   */
  String HEARTBIT_SEC = "heartbit.interval.seconds";
  /**
   * Channel cache size setting, will be same for connections to all brokers. See {@link CachingConnectionFactory#setChannelCacheSize(int)}.
   */
  String CHANNEL_CACHE_SIZE = "channel.cache.size";
  /**
   * Connection close timeout in milliseconds, will be same for connections to all brokers. See {@link AbstractConnectionFactory#setCloseTimeout(int)}
   * .
   */
  String CLOSE_TIMEOUT = "close.timeout.millis";

  /**
   * Name will be used in thread name of receiver workers.
   */
  String RECEIVER_NAME = "receiver.name";
  /**
   * Set default queue names receiver will be listening. Multiple queue names are separated using {@link #RECEIVER_QUEUES_SEPARATOR_PATTERN}.
   */
  String RECEIVER_QUEUES = "receiver.queues";
  String RECEIVER_QUEUES_SEPARATOR = ",";
  Pattern RECEIVER_QUEUES_SEPARATOR_PATTERN = Pattern.compile(RECEIVER_QUEUES_SEPARATOR);
  /**
   * Set the size of receiver's executor threadpool. Will be same for connections to all brokers. See
   * {@link SimpleMessageListenerContainer#setTaskExecutor(java.util.concurrent.Executor)}.
   */
  String RECEIVER_THREADPOOL = "receiver.threadpool";
  /**
   * Set receiver's prefetch count option. Will be same for connections to all brokers. See
   * {@link SimpleMessageListenerContainer#setPrefetchCount(int)}.
   */
  String RECEIVER_PREFETCH_COUNT = "receiver.prefetch.count";
  /**
   * Whether or not receiver should use MDC-related headers in message and put them to MDC context
   */
  String RECEIVER_USE_MDC = "receiver.use.mdc";
  /**
   * @see SimpleMessageListenerContainer#setShutdownTimeout(long)
   */
  String RECEIVER_SHUTDOWN_TIMEOUT = "receiver.shutdown.timeout";

  /**
   * Name will be used in thread name of publisher workers.
   */
  String PUBLISHER_NAME = "publisher.name";
  /**
   * Configure publisher confirms, will be same for connections to all brokers. See {@link CachingConnectionFactory#setPublisherConfirms(boolean)}.
   */
  String PUBLISHER_CONFIRMS = "publisher.confirms";
  /**
   * Configure publisher returns, will be same for connections to all brokers. See {@link CachingConnectionFactory#setPublisherReturns(boolean)}.
   */
  String PUBLISHER_RETURNS = "publisher.returns";
  /**
   * Set the size of inner (inmemory) queue for all publisher connections.
   */
  String PUBLISHER_INNER_QUEUE_SIZE = "publisher.innerqueue.size";
  /**
   * Set default exchange for publisher.
   */
  String PUBLISHER_EXCHANGE = "publisher.exchange";
  /**
   * Set default routing key for publisher.
   */
  String PUBLISHER_ROUTING_KEY = "publisher.routingKey";
  /**
   * Set 'mandatory' flag for publisher.
   */
  String PUBLISHER_MANDATORY = "publisher.mandatory";
  /**
   * Configure transactional mode of publisher.
   */
  String PUBLISHER_TRANSACTIONAL = "publisher.transactional";

  String PUBLISHER_RETRY_DELAY_MS = "publisher.retryDelay.millis";
  /**
   * Whether or not publisher should store MDC context to message
   */
  String PUBLISHER_USE_MDC = "publisher.use.mdc";
  /**
   * How long publisher will wait for inner queue to clear when shutting down
   */
  String PUBLISHER_INNER_QUEUE_SHUTDOWN_MS = "publisher.innerqueue.shutdown.ms";
  /**
   * Should connection factory enable automatic recovering
   */
  String AUTOMATIC_RECOVERY = "auto.recovery";
  /**
   * Should connection factory enable topology recovering
   */
  String TOPOLOGY_RECOVERY = "topology.recovery";
}
