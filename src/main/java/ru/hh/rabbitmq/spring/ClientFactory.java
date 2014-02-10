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
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_RETURNS;
import static ru.hh.rabbitmq.spring.ConfigKeys.USERNAME;
import static ru.hh.rabbitmq.spring.ConfigKeys.VIRTUALHOST;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

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

  public Receiver createReceiver() {
    List<ConnectionFactory> factories = getOrCreateConnectionFactories();
    Receiver receiver = new Receiver(factories, properties.getProperties());
    return receiver;
  }

  public Publisher createPublisher() {
    List<ConnectionFactory> factories = getOrCreateConnectionFactories();
    Publisher publisher = new Publisher(factories, properties.getProperties());
    return publisher;
  }

  private List<ConnectionFactory> getOrCreateConnectionFactories() {
    if (factories == null) {
      factories = createConnectionFactories();
    }
    return factories;
  }

  private List<ConnectionFactory> createConnectionFactories() {
    List<ConnectionFactory> factories = new ArrayList<>();
    Integer commonPort = properties.integer(PORT);

    String hosts = properties.string(HOSTS);
    if (hosts == null) {
      String host = properties.notNullString(HOST);
      return Lists.newArrayList(createConnectionFactory(host, commonPort));
    }

    Iterable<String> hostsList = Splitter.on(HOSTS_SEPARATOR).split(hosts);
    for (String hostAndPortString : hostsList) {
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

  private ConnectionFactory createConnectionFactory(String host, Integer port) {
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
}
