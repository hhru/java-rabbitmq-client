package ru.hh.rabbitmq.send;

import com.google.common.base.Throwables;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import ru.hh.rabbitmq.simple.Message;

public class SynchronousPublisher {
  private final AsynchronousPublisher publisher;
  private final long timeout;
  private final TimeUnit unit;
  
  public SynchronousPublisher(AsynchronousPublisher publisher, long timeout, TimeUnit unit) {
    this.publisher = publisher;
    this.timeout = timeout;
    this.unit = unit;
  }
  
  private void withTimeout(Callable<Future<Void>> task) {
    try {
      Future<Void> future = task.call();
      try {
        future.get(timeout, unit);
      } catch (TimeoutException e) {
        if (future.cancel(true)) {
          throw new SendTimeoutException();
        } else {
          if (future.isDone()) {
            future.get(); // will return or throw immediately
          } else {
            throw new IllegalStateException("sending failed with timeout, cancellation failed too and future is not done");
          }
        }
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public void send(final Destination destination, final Message... messages) {
    withTimeout(new Callable<Future<Void>>() {
      @Override
      public Future<Void> call() throws Exception {
        return publisher.send(destination, messages);
      }
    });
  }

  public void send(final Destination destination, final Collection<Message> messages) {
    withTimeout(new Callable<Future<Void>>() {
      @Override
      public Future<Void> call() throws Exception {
        return publisher.send(destination, messages);
      }
    });
  }

  public void send(final Map<Message, Destination> messages) {
    withTimeout(new Callable<Future<Void>>() {
      @Override
      public Future<Void> call() throws Exception {
        return publisher.send(messages);
      }
    });
  }

  public void sendTransactional(final Destination destination, final Message... messages) {
    withTimeout(new Callable<Future<Void>>() {
      @Override
      public Future<Void> call() throws Exception {
        return publisher.sendTransactional(destination, messages);
      }
    });
  }

  public void sendTransactional(final Destination destination, final Collection<Message> messages) {
    withTimeout(new Callable<Future<Void>>() {
      @Override
      public Future<Void> call() throws Exception {
        return publisher.sendTransactional(destination, messages);
      }
    });
  }

  public void sendTransactional(final Map<Message, Destination> messages) {
    withTimeout(new Callable<Future<Void>>() {
      @Override
      public Future<Void> call() throws Exception {
        return publisher.sendTransactional(messages);
      }
    });
  }
}
