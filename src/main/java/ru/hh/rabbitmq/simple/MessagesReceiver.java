package ru.hh.rabbitmq.simple;

import java.io.IOException;

public interface MessagesReceiver {
  void receive(Message message) throws IOException;

  boolean isEnough();

  void onStart();

  void onFinish();
}
