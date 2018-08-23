package ru.hh.rabbitmq.spring.send;

import com.google.common.util.concurrent.ForwardingFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

class PublishTaskFuture extends ForwardingFuture<Void> implements ListenableFuture<Void> {
  private final Map<Object, Destination> messages;
  private final SettableFuture<Void> future = SettableFuture.create();
  private Optional<Map<String, String>> MDCContext;

  PublishTaskFuture(Destination destination, Collection<Object> messages) {
    this.messages = messages.stream().collect(toMap(Function.identity(), message -> destination));
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

  void setMDCContext(Map<String, String> mdcContext) {
    this.MDCContext = Optional.ofNullable(mdcContext);
  }

  void complete() {
    future.set(null);
  }

  void fail(Throwable t) {
    future.setException(t);
  }
}
