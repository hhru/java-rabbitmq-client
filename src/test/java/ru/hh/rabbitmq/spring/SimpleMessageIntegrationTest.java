package ru.hh.rabbitmq.spring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.springframework.amqp.support.converter.MessageConverter;

import ru.hh.rabbitmq.spring.send.Publisher;
import ru.hh.rabbitmq.spring.simple.SimpleMessage;
import ru.hh.rabbitmq.spring.simple.SimpleMessageConverter;
import ru.hh.rabbitmq.spring.simple.SimpleMessageListener;

public class SimpleMessageIntegrationTest extends RabbitIntegrationTestBase {

  public static final long TIMEOUT_MILLIS = 1000;

  @Test
  public void testSimpleMessages() throws InterruptedException {
    MessageConverter converter = new SimpleMessageConverter();

    Publisher publisherHost1 = publisher(HOST1, true).withMessageConverter(converter).build();
    publisherHost1.startSync();

    MessageHandler handler = new MessageHandler();
    Receiver receiver = receiverAllHosts(true).withListenerAndConverter(handler, converter).start();

    Map<String, Object> body = new HashMap<>();
    body.put("data1", 1);
    body.put("data2", "2");

    Map<String, Object> headers = new HashMap<>();
    headers.put("header1", 1);
    headers.put("header2", "2");

    SimpleMessage sentMessage = new SimpleMessage(headers, body);
    publisherHost1.send(sentMessage);
    SimpleMessage receivedMessage = handler.get();

    for (Entry<String, Object> entry : headers.entrySet()) {
      assertTrue(receivedMessage.getHeaders().entrySet().contains(entry));
    }
    assertEquals(sentMessage.getBody(), receivedMessage.getBody());

    publisherHost1.stopSync();
    receiver.shutdown();
  }

  private static class MessageHandler implements SimpleMessageListener {
    private ArrayBlockingQueue<SimpleMessage> queue = new ArrayBlockingQueue<SimpleMessage>(1);

    @Override
    public void handleMessage(SimpleMessage message) throws Exception {
      queue.add(message);
    }

    public SimpleMessage get() throws InterruptedException {
      return queue.poll(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }

  }

}
