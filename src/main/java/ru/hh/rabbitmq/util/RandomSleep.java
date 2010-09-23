package ru.hh.rabbitmq.util;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Sleeps random time between min and max, threadsafe
 */
@Deprecated
public class RandomSleep {
  private final Random random = new Random();
  private final int min;
  private final int max;
  private final TimeUnit unit;

  public RandomSleep(TimeUnit unit, int min, int max) {
    this.unit = unit;
    this.max = max;
    this.min = min;
  }
  
  public void sleep() throws InterruptedException {
    unit.sleep(min + random.nextInt(max - min + 1));
  }
}
