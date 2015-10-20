package ru.hh.rabbitmq.spring;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.slf4j.MDC;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ConfirmCallback;
import org.springframework.amqp.rabbit.support.CorrelationData;
import com.google.common.collect.ImmutableMap;
import ru.hh.rabbitmq.spring.receive.MapMessageListener;
import ru.hh.rabbitmq.spring.send.CorrelatedMessage;
import ru.hh.rabbitmq.spring.send.Destination;
import ru.hh.rabbitmq.spring.send.Publisher;
import ru.hh.rabbitmq.spring.send.QueueIsFullException;

public class RabbitIntegrationTest extends RabbitIntegrationTestBase {

  public static final long TIMEOUT_MILLIS = 5000;

  @Test
  public void testDirectionsConfigured() throws InterruptedException {
    Publisher publisherHost1 = publisher(HOST1, true).withJsonMessageConverter().build();
    Publisher publisherHost2 = publisher(HOST2, true).withJsonMessageConverter().build();
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

    Publisher publisherHost1 = publisher(HOST1, false).withJsonMessageConverter().build();
    Publisher publisherHost2 = publisher(HOST2, false).withJsonMessageConverter().build();
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

    Publisher publisher = publisher(HOST1, false).withJsonMessageConverter().build();
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
    Publisher publisher = publisher(HOST1, true).withJsonMessageConverter().build();
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

  @Test(expected = IllegalStateException.class)
  public void testStoppedPublisher() throws InterruptedException {
    Publisher publisher = publisher(HOST1, true).withJsonMessageConverter().build();
    publisher.startSync();

    publisher.stopSync();

    Map<String, Object> sentMessage = new HashMap<>();

    sentMessage.put("data", "1");
    publisher.send(sentMessage);
  }

  @Test(expected = QueueIsFullException.class)
  public void testImmediateFullQueue() throws InterruptedException {

    Publisher publisher = publisher("unknownhost_for_queue_is_full", true, 2).withJsonMessageConverter().build();
    publisher.startSync();
    assertEquals(2, publisher.getInnerQueueRemainingCapacity());

    publisher.send("message1");
    // the task can be: in the queue, or out of the queue in the middle of processing
    assertTrue(publisher.getInnerQueueRemainingCapacity() <= 2);

    publisher.send("message2");
    assertTrue(publisher.getInnerQueueRemainingCapacity() <= 1);

    publisher.send("message3");
    assertEquals(0, publisher.getInnerQueueRemainingCapacity());

    publisher.send("message4");
  }

  @Test
  public void testTimedOutFullQueue() throws InterruptedException {

    Publisher publisher = publisher("unknownhost_for_queue_is_full", true, 2).withJsonMessageConverter().build();
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
    Publisher publisher = publisher(HOST2, true, true).withJsonMessageConverter().withConfirmCallback(callback).build();
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
    assertNotNull("first callback value must not be null", callbackValue);
    assertTrue("first callback value not in original: " + callbackValue, data.values().contains(callbackValue));

    callbackValue = callback.get();
    assertNotNull("second callback value must not be null", callbackValue);
    assertTrue("second callback value not in original: " + callbackValue, data.values().contains(callbackValue));

    callbackValue = callback.get();
    assertNotNull("third callback value must not be null", callbackValue);
    assertTrue("third callback value not in original: " + callbackValue, data.values().contains(callbackValue));

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
    String MDCKey = "mdctestkey";
    String MDCValue = "mdctestvalue";

    MDC.put(MDCKey, MDCValue);
    Publisher publisherHost1 = publisherMDC(HOST1).withJsonMessageConverter().build();
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
    private ArrayBlockingQueue<Map<String, Object>> queue = new ArrayBlockingQueue<Map<String, Object>>(1);
    private ArrayBlockingQueue<Map<String, String>> mdcContextQueue = new ArrayBlockingQueue<Map<String, String>>(1);

    private boolean useMDC;

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
    private ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<String>(3);

    @SuppressWarnings("unused")
    @Override
    public void confirm(CorrelationData correlationData, boolean ack, String cause) {
      queue.add(correlationData.getId());
    }
    public String get() throws InterruptedException {
      return queue.poll(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }
  }

}
