package ru.hh.rabbitmq.impl;

public class AutoreconnectProperties {
  private boolean enabled;
  private Integer attempts = 3;
  private Long delay = 100L;

  public AutoreconnectProperties(boolean enabled) {
    this.enabled = enabled;
  }

  public AutoreconnectProperties(boolean enabled, Integer attempts) {
    this.enabled = enabled;
    this.attempts = attempts;
  }

  public AutoreconnectProperties(boolean enabled, Integer attempts, Long delay) {
    this.enabled = enabled;
    this.attempts = attempts;
    this.delay = delay;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public Integer getAttempts() {
    return attempts;
  }

  public Long getDelay() {
    return delay;
  }
}
