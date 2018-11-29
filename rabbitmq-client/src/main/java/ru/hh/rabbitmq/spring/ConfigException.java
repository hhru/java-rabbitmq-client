package ru.hh.rabbitmq.spring;

public class ConfigException extends RuntimeException {

  public ConfigException(String message, Exception e) {
    super(message, e);
  }

  public ConfigException(String message) {
    super(message);
  }

}
