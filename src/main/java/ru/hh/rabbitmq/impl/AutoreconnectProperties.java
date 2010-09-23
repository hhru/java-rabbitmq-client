package ru.hh.rabbitmq.impl;

import java.util.concurrent.TimeUnit;
import ru.hh.rabbitmq.util.RandomSleep;

@Deprecated
public class AutoreconnectProperties {
  private int attempts = 3;
  private int minDelay = 100;
  private int maxDelay = 500;
  private RandomSleep sleeper;

  public AutoreconnectProperties(int attempts) {
    this.attempts = attempts;
  }

  public AutoreconnectProperties(int attempts, int minDelay, int maxDelay) {
    this.attempts = attempts;
    this.minDelay = minDelay;
    this.maxDelay = maxDelay;
  }

  public int getAttempts() {
    return attempts;
  }

  public int getMinDelay() {
    return minDelay;
  }

  public int getMaxDelay() {
    return maxDelay;
  }

  public RandomSleep getSleeper() {
    if (sleeper == null) {
      sleeper = new RandomSleep(TimeUnit.MILLISECONDS, minDelay, maxDelay);
    }
    return sleeper;
  }
}
