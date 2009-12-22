package ru.hh.rabbitmq.simple;

import java.io.IOException;

public class DummyMessageReceiver implements MessageReceiver, MessagesReceiver {
  public Message message;
  public boolean startCalled;
  public boolean finishCalled;
  public boolean isEnoughCalled;

  public void receive(Message message) throws IOException {
    this.message = message;
  }

  public boolean isEnough() {
    isEnoughCalled = true;
    return false;
  }

  public void onFinish() {
    finishCalled = true;
  }

  public void onStart() {
    startCalled = true;
  }
}
