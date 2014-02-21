package ru.hh.rabbitmq.spring.send;

import java.util.concurrent.TimeoutException;

public class CancelOnTimeoutFailedException extends TimeoutException {
  public CancelOnTimeoutFailedException() {
  }

  public CancelOnTimeoutFailedException(String message) {
    super(message);
  }
}
