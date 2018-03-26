package ru.hh.rabbitmq.spring;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import javax.annotation.Nullable;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import ru.hh.metrics.StatsDSender;
import static java.util.Collections.emptyList;
import static org.springframework.util.StringUtils.hasText;
import static ru.hh.rabbitmq.spring.ConfigKeys.CHANNEL_CACHE_SIZE;
import static ru.hh.rabbitmq.spring.ConfigKeys.CLOSE_TIMEOUT;
import static ru.hh.rabbitmq.spring.ConfigKeys.CONNECTION_TIMEOUT_MS;
import static ru.hh.rabbitmq.spring.ConfigKeys.HEARTBIT_SEC;
import static ru.hh.rabbitmq.spring.ConfigKeys.HOSTS_PORT_SEPARATOR;
import static ru.hh.rabbitmq.spring.ConfigKeys.HOSTS_SEPARATOR;
import static ru.hh.rabbitmq.spring.ConfigKeys.PASSWORD;
import static ru.hh.rabbitmq.spring.ConfigKeys.PORT;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_CONFIRMS;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_RETURNS;
import static ru.hh.rabbitmq.spring.ConfigKeys.USERNAME;
import static ru.hh.rabbitmq.spring.ConfigKeys.VIRTUALHOST;

abstract class ConnectionsFactory {

  protected final PropertiesHelper properties;
  @Nullable
  final StatsDSender statsDSender;
  @Nullable
  final String serviceName;

  ConnectionsFactory(Properties properties, @Nullable String serviceName, @Nullable StatsDSender statsDSender, boolean sendStats) {
    this.properties = new PropertiesHelper(properties);
    if (!sendStats) {
      statsDSender = null;
    }
    this.serviceName = serviceName;
    this.statsDSender = statsDSender;
  }

  ConnectionsFactory(Properties properties) {
    this(properties, null, null, false);
  }

  private Iterable<String> getHosts(boolean throwOnEmpty, String... settingNames) {
    String value;
    for (String settingName : settingNames) {
      value = !hasText(settingName) ? null : properties.getString(settingName);
      if (hasText(value)) {
        return splitHosts(value);
      }
    }
    if (throwOnEmpty) {
      throw new ConfigException(String.format("Any of these properties must be set and not empty: %s", Joiner.on(',').join(settingNames)));
    }
    return emptyList();
  }

  private static Iterable<String> splitHosts(String hosts) {
    return Splitter.on(HOSTS_SEPARATOR).split(hosts);
  }

  public List<ConnectionFactory> createConnectionFactories(boolean throwOnEmpty, String... hostsSettingNames) {
    List<ConnectionFactory> factories = new ArrayList<>();
    try {
      Integer commonPort = properties.getInteger(PORT);
      // something_HOSTS -> HOSTS -> HOST -> exception
      Iterable<String> hosts = getHosts(throwOnEmpty, hostsSettingNames);
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
    catch (RuntimeException e) {
      throw new ConfigException("Failed to create connection factories", e);
    }
  }

  protected ConnectionFactory createConnectionFactory(String host, Integer port) {
    try {
      CachingConnectionFactory factory = new CachingConnectionFactory(host);
      if (port != null) {
        factory.setPort(port);
      }

      factory.setConnectionTimeout(properties.getInteger(CONNECTION_TIMEOUT_MS, 200));

      factory.setUsername(properties.getNotNullString(USERNAME));
      factory.setPassword(properties.getNotNullString(PASSWORD));
      String virtualhost = properties.getString(VIRTUALHOST);
      if (virtualhost != null) {
        factory.setVirtualHost(virtualhost);
      }

      factory.setRequestedHeartBeat(properties.getInteger(HEARTBIT_SEC, 2));

      Integer channelCacheSize = properties.getInteger(CHANNEL_CACHE_SIZE);
      if (channelCacheSize != null) {
        factory.setChannelCacheSize(channelCacheSize);
      }
      Integer closeTimeout = properties.getInteger(CLOSE_TIMEOUT);
      if (closeTimeout != null) {
        factory.setCloseTimeout(closeTimeout);
      }
      Boolean publisherConfirms = properties.getBoolean(PUBLISHER_CONFIRMS);
      if (publisherConfirms != null) {
        factory.setPublisherConfirms(publisherConfirms);
      }
      Boolean publisherReturns = properties.getBoolean(PUBLISHER_RETURNS);
      if (publisherReturns != null) {
        factory.setPublisherReturns(publisherReturns);
      }
      return factory;
    }
    catch (RuntimeException e) {
      throw new ConfigException(String.format("Failed to create ConnectionFactory (%s)", host), e);
    }
  }
}
