package ru.hh.rabbitmq.simple;

import ru.hh.rabbitmq.NackException;

public interface MessagesReceiver {
  void receive(Message message) throws InterruptedException, NackException;

  boolean isEnough();

  void onStart();

  void onFinish();
}
