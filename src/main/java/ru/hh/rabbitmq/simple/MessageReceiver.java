package ru.hh.rabbitmq.simple;

public interface MessageReceiver {
  void receive(Message message) throws InterruptedException;
}
