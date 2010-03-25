package ru.hh.rabbitmq;

public class TransactionException extends RuntimeException {
  public TransactionException(String message, Throwable cause) {
    super(message, cause);
  }
}
