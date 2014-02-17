package ru.hh.rabbitmq.spring.send;

public class QueueIsFullException extends RuntimeException {
  public QueueIsFullException(String instanceName, Throwable cause) {
    super("Queue is full for " + instanceName, cause);
  }
}
