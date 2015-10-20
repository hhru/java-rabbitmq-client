package ru.hh.rabbitmq.spring.send;

import java.util.concurrent.TimeoutException;

public class CancelOnTimeoutFailedException extends TimeoutException {
  CancelOnTimeoutFailedException() {
  }

  CancelOnTimeoutFailedException(String message) {
    super(message);
  }
}
