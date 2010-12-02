package ru.hh.rabbitmq.send;

/**
 * {@link com.rabbitmq.client.Channel#basicPublish(java.lang.String, java.lang.String, boolean, boolean, com.rabbitmq.client.AMQP.BasicProperties, byte[])}
 */
public class Destination {
  private final String exchange;
  private final String routingKey;
  private final boolean mandatory;
  private final boolean immediate;

  public Destination(String exchange, String routingKey, boolean mandatory, boolean immediate) {
    this.exchange = exchange;
    this.routingKey = routingKey;
    this.mandatory = mandatory;
    this.immediate = immediate;
  }

  public String getExchange() {
    return exchange;
  }

  public String getRoutingKey() {
    return routingKey;
  }

  public boolean isMandatory() {
    return mandatory;
  }

  public boolean isImmediate() {
    return immediate;
  }
}
