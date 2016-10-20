package ru.hh.rabbitmq.spring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import org.junit.Test;
import org.slf4j.MDC;
import org.springframework.amqp.rabbit.support.CorrelationData;
import com.google.common.collect.ImmutableMap;
import ru.hh.rabbitmq.spring.send.CorrelatedMessage;
import ru.hh.rabbitmq.spring.send.Destination;
import ru.hh.rabbitmq.spring.send.SyncPublisher;

public class SyncRabbitIntegrationTest extends SyncRabbitIntegrationTestBase {

  @Test
  public void testDirectionsConfigured() throws InterruptedException {
    SyncPublisher publisherHost1 = publisher(HOST1, true).withJsonMessageConverter().build();
    SyncPublisher publisherHost2 = publisher(HOST2, true).withJsonMessageConverter().build();
    publisherHost1.startAsync();
    publisherHost2.startAsync();

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

    SyncPublisher publisherHost1 = publisher(HOST1, false).withJsonMessageConverter().build();
    SyncPublisher publisherHost2 = publisher(HOST2, false).withJsonMessageConverter().build();
    publisherHost1.startAsync();
    publisherHost2.startAsync();

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

    SyncPublisher publisher = publisher(HOST1, false).withJsonMessageConverter().build();
    publisher.startAsync();

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

  @Test(expected = IllegalStateException.class)
  public void testStoppedPublisher() throws InterruptedException {
    SyncPublisher publisher = publisher(HOST1, true).withJsonMessageConverter().build();
    publisher.startAsync();

    publisher.stopSync();

    Map<String, Object> sentMessage = new HashMap<>();

    sentMessage.put("data", "1");
    publisher.send(sentMessage);
  }

  @Test
  public void testPublisherConfirms() throws InterruptedException, ExecutionException {
    TestConfirmCallback callback = new TestConfirmCallback();
    SyncPublisher publisher = publisher(HOST2, true, true).withJsonMessageConverter().withConfirmCallback(callback).build();
    publisher.startAsync();

    Map<String, Object> sentMessage = new HashMap<>();
    CorrelationData correlationData;

    Map<String, String> data = ImmutableMap.of("1", "corr1", "2", "corr2", "3", "corr3");

    for (Entry<String, String> entry : data.entrySet()) {
      sentMessage.put("data", entry.getKey());
      correlationData = new CorrelationData(entry.getValue());
      publisher.send(new CorrelatedMessage(correlationData, sentMessage));
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
    SyncPublisher publisherHost1 = publisherMDC(HOST1).withJsonMessageConverter().build();
    publisherHost1.startAsync();

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
}
