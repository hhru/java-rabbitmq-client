package ru.hh.rabbitmq.send;

public class PublishOptions {
  private final String exchange;
  private final String routingKey;
  private final boolean mandatory;
  private final boolean immediate;

  public PublishOptions(String exchange, String routingKey, boolean mandatory, boolean immediate) {
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
