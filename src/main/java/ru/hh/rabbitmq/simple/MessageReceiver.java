package ru.hh.rabbitmq.simple;

import ru.hh.rabbitmq.NackException;

public interface MessageReceiver {
  void receive(Message message) throws InterruptedException, NackException;
}
