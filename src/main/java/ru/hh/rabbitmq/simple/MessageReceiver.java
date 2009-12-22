package ru.hh.rabbitmq.simple;

import java.io.IOException;

public interface MessageReceiver {
  void receive(Message message) throws IOException;
}
