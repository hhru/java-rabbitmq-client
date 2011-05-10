package ru.hh.rabbitmq.send;

public interface FailedTaskProcessor {
  void returnFuture(PublishTaskFuture future);
}
