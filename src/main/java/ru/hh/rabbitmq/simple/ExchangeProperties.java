package ru.hh.rabbitmq.simple;

public class ExchangeProperties {
  public static final String TYPE_DIRECT = "direct";
  public static final String TYPE_FANOUT = "fanout";
  public static final String TYPE_TOPIC = "topic";

  private String name;
  private String type;
  private boolean durable;

  public ExchangeProperties(String name, String type, boolean durable) {
    this.name = name;
    this.type = type;
    this.durable = durable;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public boolean isDurable() {
    return durable;
  }
}
