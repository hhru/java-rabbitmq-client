package ru.hh.rabbitmq.spring.send;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ForwardingFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

class PublishTaskFuture extends ForwardingFuture<Void> implements ListenableFuture<Void> {
  private final Map<Object, Destination> messages;
  private final SettableFuture<Void> future = SettableFuture.create();
  private Optional<Map<String, String>> MDCContext;

  PublishTaskFuture(Destination destination, Collection<Object> messages) {
    this.messages = new HashMap<Object, Destination>();
    for (Object message : messages) {
      this.messages.put(message, destination);
    }
  }

  PublishTaskFuture(Map<Object, Destination> messages) {
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

  Map<Object, Destination> getMessages() {
    return messages;
  }

  Optional<Map<String, String>> getMDCContext() {
    return MDCContext;
  }

  void setMDCContext(Map<String, String> MDCContext) {
    this.MDCContext = Optional.fromNullable(MDCContext);
  }

  void complete() {
    future.set(null);
  }

  void fail(Throwable t) {
    future.setException(t);
  }
}
