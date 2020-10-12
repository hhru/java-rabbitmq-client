package ru.hh.rabbitmq.spring;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.fail;
import static org.rnorth.visibleassertions.VisibleAssertions.assertNotNull;
import static org.rnorth.visibleassertions.VisibleAssertions.assertSame;
import static org.rnorth.visibleassertions.VisibleAssertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.support.converter.MessageConverter;
import ru.hh.rabbitmq.spring.send.Publisher;
import ru.hh.rabbitmq.spring.simple.SimpleMessage;
import ru.hh.rabbitmq.spring.simple.SimpleMessageConverter;
import ru.hh.rabbitmq.spring.simple.SimpleMessageListener;

public class SimpleMessageIntegrationTest extends AsyncRabbitIntegrationTestBase {

  public static final long TIMEOUT_MILLIS = 1000;

  @Test
  public void testSimpleMessages() throws InterruptedException {
    MessageConverter converter = new SimpleMessageConverter();

    Publisher publisherHost = publisher(HOST1, PORT1, true).withMessageConverter(converter).build();
    publisherHost.startSync();

    MessageHandler handler = new MessageHandler();
    Receiver receiver = receiverAllHosts(true).withListenerAndConverter(handler, converter).start();

    SimpleMessage sentMessage1 = new SimpleMessage(
      Map.of(
        "id", 1,
        "header11", 123,
        "header21", "321"
      ),
      Map.of(
        "data11", 456,
        "data21", "789"
      )
    );

    SimpleMessage sentMessage2 = new SimpleMessage(
      Map.of(
        "id", 2,
        "header12", 321,
        "header22", "123"
      ),
      Map.of(
        "data12", 654,
        "data22", "987"
      )
    );

    List<SimpleMessage> sentMessages = List.of(sentMessage1, sentMessage2);
    publisherHost.send(sentMessages);

    SimpleMessage receivedMessage1 = handler.get();
    int id1 = getSimpleMessageId(receivedMessage1);

    SimpleMessage receivedMessage2 = handler.get();
    int id2 = getSimpleMessageId(receivedMessage2);

    // there is no guarantee that messages will be received at the same order
    if (id1 == 1) {
      assertSimpleMessageEquals(sentMessage1, receivedMessage1);
      assertSame("Second message hasn't been received", id2, 2);
      assertSimpleMessageEquals(sentMessage2, receivedMessage2);
    } else if (id1 == 2) {
      assertSimpleMessageEquals(sentMessage2, receivedMessage1);
      assertSame("First message hasn't been received", id2, 1);
      assertSimpleMessageEquals(sentMessage1, receivedMessage2);
    } else {
      fail("Unexpected 'id' header value: " + id1);
    }

    publisherHost.stopSync();
    receiver.shutdown();
  }

  private void assertSimpleMessageEquals(SimpleMessage expected, SimpleMessage actual) {
    var actualHeadersSet = actual.getHeaders().entrySet();
    var expectedHeadersSet = expected.getHeaders().entrySet();
    var actualBodySet = actual.getBody().entrySet();
    var expectedBodySet = expected.getBody().entrySet();

    for (var entry : expectedHeadersSet) {
      assertTrue(
        String.format("Header '%s:%s' hasn't received", entry.getKey(), entry.getValue()),
        actualHeadersSet.contains(entry)
      );
    }

    for (var entry : expectedBodySet) {
      assertTrue(
        String.format("Body entry '%s:%s' hasn't received", entry.getKey(), entry.getValue()),
        actualBodySet.contains(entry)
      );
    }
  }

  private int getSimpleMessageId(SimpleMessage receivedMessage) {
    assertNotNull("Message hasn't been received", receivedMessage);
    Object index = receivedMessage.getHeaders().get("id");
    assertNotNull("Header 'id' hasn't been received", index);
    assertSame("Received 'id' header isn't integer", index.getClass(), Integer.class);
    return (Integer) index;
  }

  private static class MessageHandler implements SimpleMessageListener {
    private final ArrayBlockingQueue<SimpleMessage> queue = new ArrayBlockingQueue<>(2);

    @Override
    public void handleMessage(SimpleMessage message) {
      queue.add(message);
    }

    public SimpleMessage get() throws InterruptedException {
      return queue.poll(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }

  }

}
