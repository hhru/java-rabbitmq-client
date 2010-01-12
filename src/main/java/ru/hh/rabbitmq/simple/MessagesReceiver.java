package ru.hh.rabbitmq.simple;

public interface MessagesReceiver {
  void receive(Message message) throws InterruptedException;

  boolean isEnough();

  void onStart();

  void onFinish();
}
