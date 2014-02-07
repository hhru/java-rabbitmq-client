package ru.hh.rabbitmq.spring;

import org.springframework.amqp.rabbit.connection.AbstractConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;

public class ConfigKeys {

  /**
   * Port for {@link #HOST} or {@link #HOSTS} (if it does not include it specifically).
   */
  public static final String PORT = "port";
  /**
   * Single rabbitmq broker host. Is used if {@link #HOSTS} is not specified.
   */
  public static final String HOST = "host";
  /**
   * Multiple rabbitmq broker hosts in the form of 'host:port,host:port' or 'host,host'.
   */
  public static final String HOSTS = "hosts";
  public static final String HOSTS_SEPARATOR = ",";
  public static final String HOSTS_PORT_SEPARATOR = ":";
  /**
   * Username, must be same for all brokers.
   */
  public static final String USERNAME = "username";
  /**
   * Password, must be same for all brokers.
   */
  public static final String PASSWORD = "password";
  /**
   * Virtualhost, must be same for all brokers.
   */
  public static final String VIRTUALHOST = "virtualhost";
  /**
   * Heartbit interval in seconds, will be same for connections to all brokers. See {@link AbstractConnectionFactory#setRequestedHeartBeat(int)}.
   */
  public static final String HEARTBIT = "heartbit.interval.seconds";
  /**
   * Channel cache size setting, will be same for connections to all brokers. See {@link CachingConnectionFactory#setChannelCacheSize(int)}.
   */
  public static final String CHANNEL_CACHE_SIZE = "channel.cache.size";
  /**
   * Connection close timeout in milliseconds, will be same for connections to all brokers. See {@link AbstractConnectionFactory#setCloseTimeout(int)}
   * .
   */
  public static final String CLOSE_TIMEOUT = "close.timeout.millis";

  /**
   * Set default queue names receiver will be listening. Multiple queue names are separated using {@link #RECEIVER_QUEUES_SEPARATOR}.
   */
  public static final String RECEIVER_QUEUES = "reciever.queues";
  public static final String RECEIVER_QUEUES_SEPARATOR = ",";
  /**
   * Set the size of receiver's executor threadpool. Will be same for connections to all brokers. See
   * {@link SimpleMessageListenerContainer#setTaskExecutor(java.util.concurrent.Executor)}.
   */
  public static final String RECEIVER_THREADPOOL = "receiver.threadpool";
  /**
   * Set receiver's prefetch count option. Will be same for connections to all brokers. See
   * {@link SimpleMessageListenerContainer#setPrefetchCount(int)}.
   */
  public static final String RECEIVER_PREFETCH_COUNT = "receiver.prefetch.count";

  /**
   * Configure publisher confirms, will be same for connections to all brokers. See {@link CachingConnectionFactory#setPublisherConfirms(boolean)}.
   */
  public static final String PUBLISHER_CONFIRMS = "publisher.confirms";
  /**
   * Configure publisher returns, will be same for connections to all brokers. See {@link CachingConnectionFactory#setPublisherReturns(boolean)}.
   */
  public static final String PUBLISHER_RETURNS = "publisher.returns";
  /**
   * Set the size of inner (inmemory) queue for all publisher connections.
   */
  public static final String PUBLISHER_INNER_QUEUE_SIZE = "publisher.innerqueue.size";
  /**
   * Set default exchange for publisher.
   */
  public static final String PUBLISHER_EXCHANGE = "publisher.exchange";
  /**
   * Set default routing key for publisher.
   */
  public static final String PUBLISHER_ROUTING_KEY = "publisher.routingKey";
  /**
   * Set 'mandatory' flag for publisher.
   */
  public static final String PUBLISHER_MANDATORY = "publisher.mandatory";
  /**
   * Configure transactional mode of publisher.
   */
  public static final String PUBLISHER_TRANSACTIONAL = "publisher.transactional";
}
