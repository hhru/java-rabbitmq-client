package ru.hh.rabbitmq;

/** Tells {@link ru.hh.rabbitmq.receive.Receiver} that message should be basic_nack'ed
 * */
public class NackException extends Exception {
  private boolean reQueue;

  public NackException(boolean reQueue) {
    this.reQueue = reQueue;
  }

  public NackException(boolean reQueue, Throwable cause) {
    super(cause);
    this.reQueue = reQueue;
  }

  public boolean getReQueue() {
    return reQueue;
  }
}
