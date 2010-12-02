package ru.hh.rabbitmq.send;

import com.headhunter.test.Mocks;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Test;
import ru.hh.rabbitmq.simple.BodilessMessage;
import ru.hh.rabbitmq.simple.Message;

public class PublisherTest {
  @Test
  public void send() throws ExecutionException, InterruptedException, TimeoutException {
    Mocks mocks = new Mocks();
    com.rabbitmq.client.ConnectionFactory connectionFactory = mocks.createMock(com.rabbitmq.client.ConnectionFactory.class);
    mocks.replay();
    Publisher publisher = new Publisher(connectionFactory, TimeUnit.SECONDS, 1, 1, 3, "localhost, anotherhost", 5672);
    Destination destination = new Destination("", "trash", true, false);
    Message message = new BodilessMessage(new HashMap<String, Object>());
    List<Message> messages = Arrays.asList(message, message, message);
    List<Future<Void>> futures = new LinkedList<Future<Void>>();
    for (int i = 0; i < 3; i++) {
      futures.add(publisher.send(destination, messages));
    }
    for (Future<Void> future : futures) {
      future.get(100, TimeUnit.MILLISECONDS);
    }
    publisher.close();
    mocks.verify();
  }
}
