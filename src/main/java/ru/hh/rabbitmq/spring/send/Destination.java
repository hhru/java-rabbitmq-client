package ru.hh.rabbitmq.spring.send;

/**
 * {@link com.rabbitmq.client.Channel#basicPublish(java.lang.String, java.lang.String, boolean, boolean, com.rabbitmq.client.AMQP.BasicProperties, byte[])}
 */
public class Destination {
  private final String exchange;
  private final String routingKey;

  public Destination(String exchange, String routingKey) {
    this.exchange = exchange;
    this.routingKey = routingKey;
  }

  public String getExchange() {
    return exchange;
  }

  public String getRoutingKey() {
    return routingKey;
  }

  @Override
  public String toString() {
    return "Destination{" +
      "exchange='" + exchange + '\'' +
      ", routingKey='" + routingKey + '\'' +
      '}';
  }
}
