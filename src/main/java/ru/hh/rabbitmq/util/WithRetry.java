package ru.hh.rabbitmq.util;

import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WithRetry implements Executor {
  public static final Logger logger = LoggerFactory.getLogger(WithRetry.class);
  
  private final Random random = new Random();
  private final TimeUnit unit;
  private final int minDelay;
  private final int maxDelay;
  private final int attempts;

  public WithRetry(TimeUnit unit, int minDelay, int maxDelay, int attempts) {
    this.unit = unit;
    this.minDelay = minDelay;
    this.maxDelay = maxDelay;
    this.attempts = attempts;
  }

  @Override
  public void execute(Runnable task) {
    int remains = attempts; 
    while (remains > 0) {
      try {
        remains--;
        task.run();
      } catch (RuntimeException taskError) {
        if (remains > 0) {
          logger.warn(String.format("task %s failed, remaining retries %d", task.toString(), remains), taskError);
          try {
            unit.sleep(minDelay + random.nextInt(maxDelay - minDelay + 1));
          } catch (InterruptedException interrupt) {
            logger.warn("retry sleep for task {} interrupted, won't retry", task.toString());
            Thread.currentThread().interrupt();
            throw new RuntimeException(interrupt);
          }
        } else {
          logger.error(String.format("task %s failed, won't retry", task.toString()), taskError);
          throw taskError;
        }
      }
    }
  }
}
