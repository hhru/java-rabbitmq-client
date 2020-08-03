package ru.hh.rabbitmq.spring.send;

import com.google.common.util.concurrent.ForwardingFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

class PublishTaskFuture extends ForwardingFuture<Void> implements ListenableFuture<Void> {
  private final Map<Object, Destination> messages;
  private final SettableFuture<Void> future = SettableFuture.create();
  private Optional<Map<String, String>> mdcContext;

  PublishTaskFuture(Destination destination, Collection<?> messages) {
    this.messages = messages.stream().collect(HashMap::new, (m, v) -> m.put(v, destination), HashMap::putAll);
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

  Optional<Map<String, String>> getMdcContext() {
    return mdcContext;
  }

  void setMdcContext(Map<String, String> mdcContext) {
    this.mdcContext = Optional.ofNullable(mdcContext);
  }

  void complete() {
    future.set(null);
  }

  void fail(Throwable t) {
    future.setException(t);
  }
}
