package ru.hh.rabbitmq.spring.send;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

import com.google.common.util.concurrent.ForwardingFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

public class PublishTaskFuture extends ForwardingFuture<Void> implements ListenableFuture<Void> {
  private final Map<Object, Destination> messages;
  private final SettableFuture<Void> future = SettableFuture.create();

  public PublishTaskFuture(Destination destination, Collection<Object> messages) {
    this.messages = new HashMap<Object, Destination>();
    for (Object message : messages) {
      this.messages.put(message, destination);
    }
  }

  public PublishTaskFuture(Map<Object, Destination> messages) {
    this.messages = messages;
  }

  @Override
  protected Future<Void> delegate() {
    return future;
  }

  @Override
  public void addListener(Runnable listener, Executor exec) {
    future.addListener(listener, exec);
  }

  public Map<Object, Destination> getMessages() {
    return messages;
  }

  public void complete() {
    future.set(null);
  }

  public void fail(Throwable t) {
    future.setException(t);
  }
}
