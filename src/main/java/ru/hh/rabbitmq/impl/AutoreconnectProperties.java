package ru.hh.rabbitmq.impl;

import java.util.concurrent.TimeUnit;
import ru.hh.rabbitmq.util.RandomSleep;

public class AutoreconnectProperties {
  private Integer attempts = 3;
  private int minDelay = 100;
  private int maxDelay = 500;
  private RandomSleep sleeper;

  public AutoreconnectProperties(Integer attempts) {
    this.attempts = attempts;
  }

  public AutoreconnectProperties(Integer attempts, int minDelay, int maxDelay) {
    this.attempts = attempts;
    this.minDelay = minDelay;
    this.maxDelay = maxDelay;
  }

  public Integer getAttempts() {
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
