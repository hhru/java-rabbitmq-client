package ru.hh.rabbitmq.simple;

public interface MessagesReceiver extends MessageReceiver {

  boolean isEnough();

  void onStart();

  void onFinish();
}
