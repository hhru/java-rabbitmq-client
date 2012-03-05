package ru.hh.rabbitmq.send;

import com.google.common.util.concurrent.ForwardingFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import ru.hh.rabbitmq.simple.Message;

class PublishTaskFuture extends ForwardingFuture<Void> implements ListenableFuture<Void> {
  private final Map<Message, Destination> messages;
  private final boolean transactional;
  private final SettableFuture<Void> future = SettableFuture.create();

  public PublishTaskFuture(Destination destination, Collection<Message> messages, boolean transactional) {
    this.messages = new HashMap<Message, Destination>();
    for (Message message : messages) {
      this.messages.put(message, destination);
    }
    this.transactional = transactional;
  }

  PublishTaskFuture(Map<Message, Destination> messages, boolean transactional) {
    this.messages = messages;
    this.transactional = transactional;
  }

  @Override
  protected Future<Void> delegate() {
    return future;
  }

  @Override
  public void addListener(Runnable listener, Executor exec) {
    future.addListener(listener, exec);
  }

  public Map<Message, Destination> getMessages() {
    return messages;
  }

  public boolean isTransactional() {
    return transactional;
  }

  public void complete() {
    future.set(null);
  }

  public void fail(Throwable t) {
    future.setException(t);
  }
}
