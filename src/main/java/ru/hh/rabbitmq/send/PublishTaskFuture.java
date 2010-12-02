package ru.hh.rabbitmq.send;

import com.google.common.util.concurrent.ForwardingFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ValueFuture;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import ru.hh.rabbitmq.simple.Message;

class PublishTaskFuture extends ForwardingFuture<Void> implements ListenableFuture<Void> {
  private final Destination destination;
  private final Collection<Message> messages;
  private final boolean transactional;
  private final ValueFuture<Void> future = ValueFuture.create();

  public PublishTaskFuture(Destination destination, Collection<Message> messages, boolean transactional) {
    this.messages = messages;
    this.destination = destination;
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

  public Destination getDestination() {
    return destination;
  }

  public Collection<Message> getMessages() {
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
