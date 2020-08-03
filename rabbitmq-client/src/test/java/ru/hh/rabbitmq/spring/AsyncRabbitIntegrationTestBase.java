package ru.hh.rabbitmq.spring;

import java.util.Properties;
import ru.hh.rabbitmq.spring.send.PublisherBuilder;

public class AsyncRabbitIntegrationTestBase extends RabbitIntegrationTestBase {

  protected static PublisherBuilder publisher(String host, int port, boolean withDirections, int innerQueueSize) {
    Properties properties = properties(host, port);
    properties.setProperty(ConfigKeys.PUBLISHER_INNER_QUEUE_SIZE, Integer.toString(innerQueueSize));
    return publisher(properties, withDirections, false);
  }

  protected static PublisherBuilder publisher(String host, int port, boolean withDirections) {
    Properties properties = properties(host, port);
    return publisher(properties, withDirections, false);
  }

  protected static PublisherBuilder publisher(String host, int port, boolean withDirections, boolean withConfirms) {
    Properties properties = properties(host, port);
    return publisher(properties, withDirections, withConfirms);
  }

  protected static PublisherBuilder publisher(Properties properties, boolean withDirections, boolean withConfirms) {
    if (withDirections) {
      appendDirections(properties);
    }
    if (withConfirms) {
      properties.setProperty(ConfigKeys.PUBLISHER_CONFIRMS, "true");
    }
    ClientFactory factory = new ClientFactory(properties);
    return factory.createPublisherBuilder();
  }

  protected static PublisherBuilder publisherMDC(String host, int port) {
    Properties properties = properties(host, port);
    appendDirections(properties);
    properties.setProperty(ConfigKeys.PUBLISHER_USE_MDC, "true");
    ClientFactory factory = new ClientFactory(properties);
    return factory.createPublisherBuilder();
  }

}
