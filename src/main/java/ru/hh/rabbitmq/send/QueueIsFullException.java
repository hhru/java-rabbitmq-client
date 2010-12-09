package ru.hh.rabbitmq.send;

public class QueueIsFullException extends RuntimeException {
  public QueueIsFullException(Throwable cause) {
    super(cause);
  }
}
