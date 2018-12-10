package ru.hh.rabbitmq.spring.send;

import org.springframework.amqp.rabbit.support.CorrelationData;

/**
 * Wraps message and allows to attach {@link CorrelationData}.
 */
public class CorrelatedMessage {

  private CorrelationData correlationData;
  private Object message;

  public CorrelatedMessage(CorrelationData correlationData, Object message) {
    this.correlationData = correlationData;
    this.message = message;
  }

  public CorrelationData getCorrelationData() {
    return correlationData;
  }

  public void setCorrelationData(CorrelationData correlationData) {
    this.correlationData = correlationData;
  }

  public Object getMessage() {
    return message;
  }

  public void setMessage(Object message) {
    this.message = message;
  }
}
