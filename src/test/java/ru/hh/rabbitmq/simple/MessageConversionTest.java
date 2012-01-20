package ru.hh.rabbitmq.simple;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
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

  @Test
  public void testJsonMessage() throws JsonGenerationException, JsonMappingException, IOException, InterruptedException {
    Map<String, Object> body = new HashMap<String, Object>();
    body.put("someInt1", 1);
    body.put("someString1", "test");

    Map<String, Object> headers = new HashMap<String, Object>();
    headers.put("someInt2", 2);
    headers.put("someString2", "test2");

    JsonMessage message = new JsonMessage(body, headers);
    DummyJsonMessageReceiver receiver = new DummyJsonMessageReceiver();
    receiver.receive(message);

    Assert.assertEquals(body, receiver.getBody());
    Assert.assertEquals(headers, receiver.getHeaders());
  }

  @Test
  public void testJsonObject() throws JsonGenerationException, JsonMappingException, IOException, InterruptedException {
    DummyJsonObject obj = new DummyJsonObject();

    Map<String, Object> headers = new HashMap<String, Object>();
    headers.put("someInt2", 2);
    headers.put("someString2", "test2");

    JsonMessage message = new JsonMessage(obj, headers);
    DummyJsonObjectReceiver receiver = new DummyJsonObjectReceiver();
    receiver.receive(message);

    Assert.assertEquals(obj, receiver.getBody());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidJsonMessage() throws InterruptedException {
    String body = "{";

    Map<String, Object> headers = new HashMap<String, Object>();
    headers.put("someInt2", 2);
    headers.put("someString2", "test2");

    Message message = new Message(body.getBytes(), headers);
    DummyJsonMessageReceiver receiver = new DummyJsonMessageReceiver();
    receiver.receive(message);
  }
}
