package ru.hh.rabbitmq.spring;

import static ru.hh.rabbitmq.spring.ConfigKeys.CHANNEL_CACHE_SIZE;
import static ru.hh.rabbitmq.spring.ConfigKeys.CLOSE_TIMEOUT;
import static ru.hh.rabbitmq.spring.ConfigKeys.HEARTBIT;
import static ru.hh.rabbitmq.spring.ConfigKeys.HOST;
import static ru.hh.rabbitmq.spring.ConfigKeys.HOSTS;
import static ru.hh.rabbitmq.spring.ConfigKeys.HOSTS_PORT_SEPARATOR;
import static ru.hh.rabbitmq.spring.ConfigKeys.HOSTS_SEPARATOR;
import static ru.hh.rabbitmq.spring.ConfigKeys.PASSWORD;
import static ru.hh.rabbitmq.spring.ConfigKeys.PORT;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_CONFIRMS;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_HOSTS;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_RETURNS;
import static ru.hh.rabbitmq.spring.ConfigKeys.RECEIVER_HOSTS;
import static ru.hh.rabbitmq.spring.ConfigKeys.USERNAME;
import static ru.hh.rabbitmq.spring.ConfigKeys.VIRTUALHOST;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

/**
 * <p>
 * Create and configure {@link Receiver} and/or {@link Publisher}.
 * </p>
 * <p>
 * See {@link ConfigKeys} constants for configuration options.
 * </p>
 */
public class ClientFactory {

  private PropertiesHelper properties;
  private List<ConnectionFactory> factories;

  public ClientFactory(Properties properties) {
    this.properties = new PropertiesHelper(properties);
  }

  /**
   * Create new receiver. Reuse connections to brokers. Won't use {@link ConfigKeys#RECEIVER_HOSTS}.
   * 
   * @return new receiver
   */
  public Receiver createReceiver() {
    return createReceiver(true);
  }

  /**
   * Create new receiver.
   * 
   * @param reuseConnections
   *          whether to reuse connections to brokers or not. If true, won't use {@link ConfigKeys#RECEIVER_HOSTS}.
   * @return new receiver
   */
  public Receiver createReceiver(boolean reuseConnections) {
    List<ConnectionFactory> factories;
    if (reuseConnections) {
      factories = getOrCreateConnectionFactories();
    }
    else {
      factories = createConnectionFactories(RECEIVER_HOSTS);
    }
    return new Receiver(factories, properties.getProperties());
  }

  /**
   * Create new publisher. Reuse connections to brokers. Won't use {@link ConfigKeys#PUBLISHER_HOSTS}.
   * 
   * @return new publisher
   */
  public Publisher createPublisher() {
    return createPublisher(true);
  }

  /**
   * Create new publisher.
   * 
   * @param reuseConnections
   *          whether to resue connections to brokers or not. If true, won't use {@link ConfigKeys#PUBLISHER_HOSTS}.
   * @return new publisher
   */
  public Publisher createPublisher(boolean reuseConnections) {
    List<ConnectionFactory> factories;
    if (reuseConnections) {
      factories = getOrCreateConnectionFactories();
    }
    else {
      factories = createConnectionFactories(PUBLISHER_HOSTS);
    }
    return new Publisher(factories, properties.getProperties());
  }

  private List<ConnectionFactory> getOrCreateConnectionFactories() {
    if (factories == null) {
      factories = createConnectionFactories(null);
    }
    return factories;
  }

  private Iterable<String> getHosts(String... settingNames) {
    String value;
    for (String settingName : settingNames) {
      value = isEmpty(settingName) ? null : properties.string(settingName);
      if (!isEmpty(value)) {
        return splitHosts(value);
      }
    }
    throw new ConfigException(String.format("Any of these properties must be set and not empty: %s", Joiner.on(',').join(settingNames)));
  }

  private Iterable<String> splitHosts(String hosts) {
    return Splitter.on(HOSTS_SEPARATOR).split(hosts);
  }

  private List<ConnectionFactory> createConnectionFactories(String hostsSettingName) {
    List<ConnectionFactory> factories = new ArrayList<>();
    try {
      Integer commonPort = properties.integer(PORT);
      // something_HOSTS -> HOSTS -> HOST -> exception
      Iterable<String> hosts = getHosts(hostsSettingName, HOSTS, HOST);
      for (String hostAndPortString : hosts) {
        Iterator<String> hostAndPort = Splitter.on(HOSTS_PORT_SEPARATOR).split(hostAndPortString).iterator();
        String host = hostAndPort.next();
        Integer port = commonPort;
        if (hostAndPort.hasNext()) {
          port = Integer.parseInt(hostAndPort.next());
        }
        factories.add(createConnectionFactory(host, port));
      }
      return factories;
    }
    catch (ConfigException e) {
      throw e;
    }
    catch (Exception e) {
      throw new ConfigException("Failed to create connection factories", e);
    }
  }

  private ConnectionFactory createConnectionFactory(String host, Integer port) {
    try {
      CachingConnectionFactory factory = new CachingConnectionFactory();
      factory.setHost(host);
      if (port != null) {
        factory.setPort(port);
      }
      factory.setUsername(properties.notNullString(USERNAME));
      factory.setPassword(properties.notNullString(PASSWORD));
      String virtualhost = properties.string(VIRTUALHOST);
      if (virtualhost != null) {
        factory.setVirtualHost(virtualhost);
      }
      Integer heartbit = properties.integer(HEARTBIT);
      if (heartbit != null) {
        factory.setRequestedHeartBeat(heartbit);
      }
      Integer channelCacheSize = properties.integer(CHANNEL_CACHE_SIZE);
      if (channelCacheSize != null) {
        factory.setChannelCacheSize(channelCacheSize);
      }
      Integer closeTimeout = properties.integer(CLOSE_TIMEOUT);
      if (closeTimeout != null) {
        factory.setCloseTimeout(closeTimeout);
      }
      Boolean publisherConfirms = properties.bool(PUBLISHER_CONFIRMS);
      if (publisherConfirms != null) {
        factory.setPublisherConfirms(publisherConfirms);
      }
      Boolean publisherReturns = properties.bool(PUBLISHER_RETURNS);
      if (publisherReturns != null) {
        factory.setPublisherReturns(publisherReturns);
      }
      return factory;
    }
    catch (Exception e) {
      throw new ConfigException(String.format("Failed to create ConnectionFactory (%s)", host), e);
    }
  }

  private boolean isEmpty(String value) {
    return value == null || value.trim().length() == 0;
  }
}
