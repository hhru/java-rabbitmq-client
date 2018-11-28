package ru.hh.rabbitmq.spring.send;

/**
 * {@link com.rabbitmq.client.Channel#basicPublish(java.lang.String, java.lang.String, boolean, boolean, com.rabbitmq.client.AMQP.BasicProperties, byte[])}
 */
public class Destination {
  private String exchange;
  private String routingKey;

  public Destination(String exchange, String routingKey) {
    this.exchange = exchange;
    this.routingKey = routingKey;
  }

  public Destination() {
  }

  public String getExchange() {
    return exchange;
  }

  public String getRoutingKey() {
    return routingKey;
  }

  public void setExchange(String exchange) {
    this.exchange = exchange;
  }

  public void setRoutingKey(String routingKey) {
    this.routingKey = routingKey;
  }

  @Override
  public String toString() {
    return "Destination{" +
      "exchange='" + exchange + '\'' +
      ", routingKey='" + routingKey + '\'' +
      '}';
  }
}
