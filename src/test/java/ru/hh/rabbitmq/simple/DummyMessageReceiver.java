package ru.hh.rabbitmq.simple;

public class DummyMessageReceiver implements MessageReceiver, MessagesReceiver {
  public Message message;
  public boolean startCalled;
  public boolean finishCalled;
  public boolean isEnoughCalled;

  public void receive(Message message) {
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
