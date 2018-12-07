package ru.hh.rabbitmq.spring.persistent.dto;

import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.springframework.amqp.rabbit.support.CorrelationData;
import ru.hh.rabbitmq.spring.send.Destination;

public class TargetedDestination extends Destination {

  private Class<?> msgClass;
  private String converterKey;

  private String senderKey;
  private CorrelationData correlationData;

  public TargetedDestination(String exchange, String routingKey, Class<?> msgClass, String converterKey,
    String senderKey, CorrelationData correlationData) {
    super(exchange, routingKey);
    this.converterKey = converterKey;
    this.msgClass = msgClass;
    this.senderKey = senderKey;
    this.correlationData = correlationData;
  }

  public static TargetedDestination build(@Nullable Destination destination,
      Object message, @Nullable CorrelationData correlationData,  String converterKey,
      String senderKey) {
    return new TargetedDestination(
      Optional.ofNullable(destination).map(Destination::getExchange).orElse(null),
      Optional.ofNullable(destination).map(Destination::getRoutingKey).orElse(null),
      message.getClass(), converterKey, senderKey, correlationData);
  }

  private TargetedDestination() {
  }

  public String getConverterKey() {
    return converterKey;
  }

  public Class<?> getMsgClass() {
    return msgClass;
  }

  public String getSenderKey() {
    return senderKey;
  }

  public CorrelationData getCorrelationData() {
    return correlationData;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TargetedDestination that = (TargetedDestination) o;
    return Objects.equals(senderKey, that.senderKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(senderKey);
  }

  @Override
  public String toString() {
    return "TargetedDestination{" +
      "destination='" + super.toString() + '\'' +
      "converterKey='" + converterKey + '\'' +
      ", msgClass=" + msgClass +
      ", senderKey='" + senderKey + '\'' +
      ", correlationData=" + correlationData +
      '}';
  }
}
