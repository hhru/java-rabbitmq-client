package ru.hh.rabbitmq.send;

public class SendTimeoutException extends RuntimeException {
  public SendTimeoutException() {
  }

  public SendTimeoutException(String message) {
    super(message);
  }

  public SendTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }

  public SendTimeoutException(Throwable cause) {
    super(cause);
  }
}
