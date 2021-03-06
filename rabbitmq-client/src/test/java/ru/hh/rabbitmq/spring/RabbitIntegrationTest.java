package ru.hh.rabbitmq.spring;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ConfirmCallback;
import org.springframework.amqp.rabbit.support.CorrelationData;
import ru.hh.rabbitmq.spring.receive.MapMessageListener;
import ru.hh.rabbitmq.spring.send.CorrelatedMessage;
import ru.hh.rabbitmq.spring.send.Destination;
import ru.hh.rabbitmq.spring.send.Publisher;
import ru.hh.rabbitmq.spring.send.QueueIsFullException;

public class RabbitIntegrationTest extends AsyncRabbitIntegrationTestBase {

  public static final long TIMEOUT_MILLIS = 5000;

  @Test
  public void testDirectionsConfigured() throws InterruptedException {
    Publisher publisherHost1 = publisher(HOST1, PORT1, true).withJsonMessageConverter().build();
    Publisher publisherHost2 = publisher(HOST2, PORT2, true).withJsonMessageConverter().build();
    publisherHost1.startSync();
    publisherHost2.startSync();

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

    publisherHost1.stopSync();
    publisherHost2.stopSync();
    receiver.shutdown();
  }

  @Test
  public void testDirectionsDeclared() throws InterruptedException {
    Destination destination = new Destination(EXCHANGE, ROUTING_KEY1);

    Publisher publisherHost1 = publisher(HOST1, PORT1, false).withJsonMessageConverter().build();
    Publisher publisherHost2 = publisher(HOST2, PORT2, false).withJsonMessageConverter().build();
    publisherHost1.startSync();
    publisherHost2.startSync();

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

    publisherHost1.stopSync();
    publisherHost2.stopSync();
    receiver.shutdown();
  }

  @Test
  public void testMultipleQueues() throws InterruptedException {
    Destination destination1 = new Destination(EXCHANGE, ROUTING_KEY1);
    Destination destination2 = new Destination(EXCHANGE, ROUTING_KEY2);

    Publisher publisher = publisher(HOST1, PORT1, false).withJsonMessageConverter().build();
    publisher.startSync();

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

    publisher.stopSync();
    receiver.shutdown();
  }

  @Test
  public void testReceiverRestart() throws InterruptedException {
    Publisher publisher = publisher(HOST1, PORT1, true).withJsonMessageConverter().build();
    publisher.startSync();

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

    publisher.stopSync();
    receiver.shutdown();
  }

  @Test
  public void testStoppedPublisher() {
    Publisher publisher = publisher(HOST1, PORT1, true).withJsonMessageConverter().build();
    publisher.startSync();

    publisher.stopSync();

    Map<String, Object> sentMessage = new HashMap<>();

    sentMessage.put("data", "1");
    assertThrows(IllegalStateException.class, () -> publisher.send(sentMessage));
  }

  @Test
  public void testImmediateFullQueue() {

    Publisher publisher = publisher("unknown_host_for_queue_is_full", PORT1, true, 2).withJsonMessageConverter().build();
    publisher.startSync();
    assertEquals(2, publisher.getInnerQueueRemainingCapacity());
    publisher.send("message1");
    // the task can be: in the queue, or out of the queue in the middle of processing
    Assert.assertTrue(publisher.getInnerQueueRemainingCapacity() <= 2);

    publisher.send("message2");
    Assert.assertTrue(publisher.getInnerQueueRemainingCapacity() <= 1);

    publisher.send("message3");
    Assert.assertEquals(0, publisher.getInnerQueueRemainingCapacity());
    assertThrows(QueueIsFullException.class, () -> publisher.send("queueIsFull"));
  }

  @Test
  public void testTimedOutFullQueue() throws InterruptedException {

    Publisher publisher = publisher("unknown_host_for_queue_is_full", PORT1, true, 2).withJsonMessageConverter().build();
    publisher.startSync();

    publisher.send("message1");
    publisher.send("message2");

    long start = System.currentTimeMillis();
    boolean queueIsFull = false;
    try {
      // must offer at least 2 messages, because one of the previous can be out of the queue in the middle of processing
      publisher.offer(100, "message3");
      publisher.offer(100, "message4");
    }
    catch (QueueIsFullException e) {
      queueIsFull = true;
    }
    assertTrue(queueIsFull);
    assertTrue((System.currentTimeMillis() - start) >= 100);
  }

  @Test
  public void testPublisherConfirms() throws InterruptedException, ExecutionException {
    TestConfirmCallback callback = new TestConfirmCallback();
    Publisher publisher = publisher(HOST2, PORT2, true, true).withJsonMessageConverter().withConfirmCallback(callback).build();
    publisher.startSync();

    Map<String, Object> sentMessage = new HashMap<>();
    CorrelationData correlationData;

    Map<String, String> data = ImmutableMap.of("1", "corr1", "2", "corr2", "3", "corr3");

    for (Entry<String, String> entry : data.entrySet()) {
      sentMessage.put("data", entry.getKey());
      correlationData = new CorrelationData(entry.getValue());
      publisher.send(new CorrelatedMessage(correlationData, sentMessage)).get();
    }

    String callbackValue;
    callbackValue = callback.get();
    assertNotNull(callbackValue, "first callback value must not be null");
    assertTrue(data.containsValue(callbackValue), "first callback value not in original: " + callbackValue);

    callbackValue = callback.get();
    assertNotNull(callbackValue, "second callback value must not be null");
    assertTrue(data.containsValue(callbackValue), "second callback value not in original: " + callbackValue);

    callbackValue = callback.get();
    assertNotNull(callbackValue, "third callback value must not be null");
    assertTrue(data.containsValue(callbackValue), "third callback value not in original: " + callbackValue);

    MessageHandler handler = new MessageHandler();
    Receiver receiver = receiverAllHosts(true).withJsonListener(handler).forQueues(QUEUE1).start();
    handler.get();
    handler.get();
    handler.get();

    publisher.stopSync();
    receiver.shutdown();
  }

  @Test
  public void testMDC() throws InterruptedException {
    String MDCKey = "mdc_test_key";
    String MDCValue = "mdc_test_value";

    MDC.put(MDCKey, MDCValue);
    Publisher publisherHost1 = publisherMDC(HOST1, PORT1).withJsonMessageConverter().build();
    publisherHost1.startSync();

    MessageHandler handler = new MessageHandler(true);
    Receiver receiver = receiverMDC().withJsonListener(handler).start();

    Map<String, Object> sentMessage = new HashMap<>();
    Map<String, Object> receivedMessage;

    sentMessage.put("data", HOST1);
    publisherHost1.send(sentMessage);
    assertEquals(MDCValue, MDC.get(MDCKey));
    MDC.clear();

    receivedMessage = handler.get();
    assertNull(MDC.getCopyOfContextMap()); // listener does not set mdc context to this thread, all happens in one of receiver internal threads
    assertNotNull(receivedMessage);
    assertEquals(sentMessage, receivedMessage);
    Map<String, String> messageMDCContext = handler.getMDC();
    assertNotNull(messageMDCContext);
    assertEquals(MDCValue, messageMDCContext.get(MDCKey));

    publisherHost1.stopSync();
    receiver.shutdown();
  }

  private static class MessageHandler implements MapMessageListener {
    private final ArrayBlockingQueue<Map<String, Object>> queue = new ArrayBlockingQueue<>(1);
    private final ArrayBlockingQueue<Map<String, String>> mdcContextQueue = new ArrayBlockingQueue<>(1);

    private final boolean useMDC;

    public MessageHandler() {
      this.useMDC = false;
    }

    public MessageHandler(boolean useMDC) {
      this.useMDC = useMDC;
    }

    @Override
    public void handleMessage(Map<String, Object> data) {
      queue.add(data);
      if (useMDC) {
        mdcContextQueue.add(MDC.getCopyOfContextMap());
      }
    }

    public Map<String, Object> get() throws InterruptedException {
      return queue.poll(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }

    public Map<String, String> getMDC() throws InterruptedException {
      return mdcContextQueue.poll(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }
  }

  private static class TestConfirmCallback implements ConfirmCallback {
    private final ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(3);

    @Override
    public void confirm(CorrelationData correlationData, boolean ack, String cause) {
      queue.add(correlationData.getId());
    }

    public String get() throws InterruptedException {
      return queue.poll(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }
  }
}
