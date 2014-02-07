package ru.hh.rabbitmq.spring.send;

public class QueueIsFullException extends RuntimeException {
  public QueueIsFullException(Throwable cause) {
    super(cause);
  }
}
