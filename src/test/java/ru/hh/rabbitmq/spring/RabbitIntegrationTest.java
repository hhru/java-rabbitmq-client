package ru.hh.rabbitmq.spring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import ru.hh.rabbitmq.spring.receive.MapMessageListener;
import ru.hh.rabbitmq.spring.send.Destination;

public class RabbitIntegrationTest extends RabbitIntegrationTestBase {

  public static final long TIMEOUT_MILLIS = 1000;

  @Test
  public void testDirectionsConfigured() throws InterruptedException {
    Publisher publisherHost1 = publisher(HOST1, true).withJsonMessageConverter();
    Publisher publisherHost2 = publisher(HOST2, true).withJsonMessageConverter();
    publisherHost1.startAndWait();
    publisherHost2.startAndWait();

    MessageHandler handler = new MessageHandler();
    Receiver receiver = receiverAllHosts(true).withJsonListener(handler).start();

    Map<String, Object> sentMessage = new HashMap<>();
    Map<String, Object> receivedMessage;

    sentMessage.put("data", HOST1);
    publisherHost1.send(sentMessage);
    receivedMessage = handler.get();
    assertNotNull(receivedMessage);
    assertEquals(sentMessage, receivedMessage);

    sentMessage.put("data", HOST2);
    publisherHost2.send(sentMessage);
    receivedMessage = handler.get();
    assertNotNull(receivedMessage);
    assertEquals(sentMessage, receivedMessage);

    publisherHost1.stopAndWait();
    publisherHost2.stopAndWait();
    receiver.shutdown();
  }

  @Test
  public void testDirectionsDeclared() throws InterruptedException {
    Destination destination = new Destination(EXCHANGE, ROUTING_KEY1);

    Publisher publisherHost1 = publisher(HOST1, false).withJsonMessageConverter();
    Publisher publisherHost2 = publisher(HOST2, false).withJsonMessageConverter();
    publisherHost1.startAndWait();
    publisherHost2.startAndWait();

    MessageHandler handler = new MessageHandler();
    Receiver receiver = receiverAllHosts(false).withJsonListener(handler).forQueues(QUEUE1).start();

    Map<String, Object> sentMessage = new HashMap<>();
    Map<String, Object> receivedMessage;

    sentMessage.put("data", HOST1);
    publisherHost1.send(destination, sentMessage);
    receivedMessage = handler.get();
    assertNotNull(receivedMessage);
    assertEquals(sentMessage, receivedMessage);

    sentMessage.put("data", HOST2);
    publisherHost2.send(destination, sentMessage);
    receivedMessage = handler.get();
    assertNotNull(receivedMessage);
    assertEquals(sentMessage, receivedMessage);

    publisherHost1.stopAndWait();
    publisherHost2.stopAndWait();
    receiver.shutdown();
  }

  @Test
  public void testMultipleQueues() throws InterruptedException {
    Destination destination1 = new Destination(EXCHANGE, ROUTING_KEY1);
    Destination destination2 = new Destination(EXCHANGE, ROUTING_KEY2);

    Publisher publisher = publisher(HOST1, false).withJsonMessageConverter();
    publisher.startAndWait();

    MessageHandler handler = new MessageHandler();
    Receiver receiver = receiverAllHosts(false).withJsonListener(handler).forQueues(QUEUE1, QUEUE2).start();

    Map<String, Object> sentMessage = new HashMap<>();
    Map<String, Object> receivedMessage;

    sentMessage.put("data", QUEUE1);
    publisher.send(destination1, sentMessage);
    receivedMessage = handler.get();
    assertNotNull(receivedMessage);
    assertEquals(sentMessage, receivedMessage);

    sentMessage.put("data", QUEUE2);
    publisher.send(destination2, sentMessage);
    receivedMessage = handler.get();
    assertNotNull(receivedMessage);
    assertEquals(sentMessage, receivedMessage);

    publisher.stopAndWait();
    receiver.shutdown();
  }

  @Test
  public void testReceiverRestart() throws InterruptedException {
    Publisher publisher = publisher(HOST1, true).withJsonMessageConverter();
    publisher.startAndWait();

    MessageHandler handler = new MessageHandler();
    Receiver receiver = receiverAllHosts(true).withJsonListener(handler).forQueues(QUEUE1).start();

    Map<String, Object> sentMessage = new HashMap<>();
    Map<String, Object> receivedMessage;

    sentMessage.put("data", "1");
    publisher.send(sentMessage);
    receivedMessage = handler.get();
    assertNotNull(receivedMessage);
    assertEquals(sentMessage, receivedMessage);

    receiver.stop();
    receiver.start();

    sentMessage.put("data", "2");
    publisher.send(sentMessage);
    receivedMessage = handler.get();
    assertNotNull(receivedMessage);
    assertEquals(sentMessage, receivedMessage);

    publisher.stopAndWait();
    receiver.shutdown();
  }

  private static class MessageHandler implements MapMessageListener {
    private ArrayBlockingQueue<Map<String, Object>> queue = new ArrayBlockingQueue<Map<String, Object>>(1);

    public void handleMessage(Map<String, Object> data) {
      queue.add(data);
    }
    public Map<String, Object> get() throws InterruptedException {
      return queue.poll(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }
  }

}
