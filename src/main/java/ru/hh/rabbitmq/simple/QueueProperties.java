package ru.hh.rabbitmq.simple;

public class QueueProperties {
  private String name;
  private boolean durable;

  public QueueProperties(String name, boolean durable) {
    this.name = name;
    this.durable = durable;
  }

  public String getName() {
    return name;
  }

  public boolean isDurable() {
    return durable;
  }
}
