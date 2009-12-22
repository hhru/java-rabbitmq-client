package ru.hh.rabbitmq.simple;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import junit.framework.Assert;
import org.junit.Test;

public class MessageConversionTest {
  @Test
  public void testFromGetResponse() {
    Envelope envelope = new Envelope(0, false, "1", "2");
    BasicProperties props = new BasicProperties();
    byte[] body = new byte[] { 1 };
    GetResponse response = new GetResponse(envelope, props, body, 0);

    Message message = Message.fromGetResponse(response);

    Assert.assertEquals(envelope, message.getEnvelope());
    Assert.assertEquals(props, message.getProperties());
    Assert.assertEquals(body, message.getBody());
  }

  @Test
  public void testFromDelivery() {
    Envelope envelope = new Envelope(0, false, "1", "2");
    BasicProperties props = new BasicProperties();
    byte[] body = new byte[] { 1 };
    Delivery delivery = new Delivery(envelope, props, body);

    Message message = Message.fromDelivery(delivery);

    Assert.assertEquals(envelope, message.getEnvelope());
    Assert.assertEquals(props, message.getProperties());
    Assert.assertEquals(body, message.getBody());
  }
}
