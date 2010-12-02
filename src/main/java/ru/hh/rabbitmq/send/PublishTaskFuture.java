package ru.hh.rabbitmq.send;

import com.google.common.util.concurrent.ValueFuture;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import ru.hh.rabbitmq.simple.Message;

class PublishTaskFuture implements Future<Void> {
  private final Destination destination;
  private final Collection<Message> messages;
  private final boolean transactional;
  private final ValueFuture<Void> future;

  public PublishTaskFuture(Destination destination, Collection<Message> messages, boolean transactional) {
    this.messages = messages;
    this.destination = destination;
    this.transactional = transactional;
    this.future = ValueFuture.create();
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

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return future.cancel(mayInterruptIfRunning);
  }

  @Override
  public boolean isCancelled() {
    return future.isCancelled();
  }

  @Override
  public boolean isDone() {
    return future.isDone();
  }

  @Override
  public Void get() throws InterruptedException, ExecutionException {
    return future.get();
  }

  @Override
  public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return future.get(timeout, unit);
  }
  
  public void complete() {
    future.set(null);
  }
  
  public void fail(Throwable t) {
    future.setException(t);
  }
}
