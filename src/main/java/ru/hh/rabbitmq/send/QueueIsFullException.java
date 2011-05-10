package ru.hh.rabbitmq.send;

public class QueueIsFullException extends RuntimeException {
  public QueueIsFullException() { }

  public QueueIsFullException(Throwable cause) {
    super(cause);
  }
}
