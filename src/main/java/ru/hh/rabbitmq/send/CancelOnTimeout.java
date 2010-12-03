package ru.hh.rabbitmq.send;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CancelOnTimeout {
  private final long timeout;
  private final TimeUnit unit;

  public CancelOnTimeout(long timeout, TimeUnit unit) {
    this.timeout = timeout;
    this.unit = unit;
  }

  void apply(Future<Void> future) throws ExecutionException, InterruptedException, TimeoutException {
    try {
      future.get(timeout, unit);
    } catch (TimeoutException e) {
      if (future.cancel(true)) {
        throw e;
      } else {
        if (future.isDone()) {
          future.get(); // will return or throw immediately
        } else {
          throw new CancelOnTimeoutFailedException();
        }
      }
    }
  }
}
