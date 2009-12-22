package ru.hh.rabbitmq.simple;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.QueueingConsumer.Delivery;

public class Message {
  protected byte[] body;
  protected BasicProperties properties;
  protected Envelope envelope;

  public Message(byte[] body, BasicProperties properties) {
    this.body = body;
    this.properties = properties;
  }

  public Message(byte[] body, BasicProperties properties, Envelope envelope) {
    this.body = body;
    this.properties = properties;
    this.envelope = envelope;
  }

  public byte[] getBody() {
    return body;
  }

  public BasicProperties getProperties() {
    return properties;
  }

  public Envelope getEnvelope() {
    return envelope;
  }

  public static Message fromGetResponse(GetResponse response) {
    Message message = new Message(response.getBody(), response.getProps(), response.getEnvelope());
    return message;
  }

  public static Message fromDelivery(Delivery delivery) {
    Message message = new Message(delivery.getBody(), delivery.getProperties(), delivery.getEnvelope());
    return message;
  }
}
