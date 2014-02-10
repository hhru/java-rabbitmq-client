package ru.hh.rabbitmq.spring;

import static ru.hh.rabbitmq.spring.ConfigKeys.HEARTBIT;
import static ru.hh.rabbitmq.spring.ConfigKeys.HOSTS;
import static ru.hh.rabbitmq.spring.ConfigKeys.PASSWORD;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_EXCHANGE;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_ROUTING_KEY;
import static ru.hh.rabbitmq.spring.ConfigKeys.RECEIVER_QUEUES;
import static ru.hh.rabbitmq.spring.ConfigKeys.USERNAME;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.util.ErrorHandler;

import ru.hh.rabbitmq.spring.ClientFactory;
import ru.hh.rabbitmq.spring.Publisher;
import ru.hh.rabbitmq.spring.Receiver;

// not a unit test, just some manually-runned test with working environment
public class AmqpTest {
  
  public static void main(String[] args) throws InterruptedException {
    // create receiver
    Properties properties = new Properties();
    properties.setProperty(HOSTS, "voznesenskiy.pyn.ru,dev");
    properties.setProperty(USERNAME, "guest");
    properties.setProperty(PASSWORD, "guest");
    properties.setProperty(RECEIVER_QUEUES, "springq");
    properties.setProperty(HEARTBIT, "1");

    ClientFactory factory = new ClientFactory(properties);
    Receiver receiver = factory.createReceiver();

    // set up the listener and receiver
    @SuppressWarnings("unused")
    // listener for debug
    MessageListener listener = new MessageListener() {
      @Override
      public void onMessage(Message message) {
        System.out.println(message);
      }
    };

    Object jsonListener = new ErrorHandler() {
      @Override
      public void handleError(Throwable t) {
        throw new AmqpRejectAndDontRequeueException(t.getMessage());
      }

      @SuppressWarnings("unused")
      public void handleMessage(Map<String, Object> object) {
        System.out.println(object);
      }
    };

    receiver.withJsonListener(jsonListener).start();
    // receiver.withListener(listener).start();

    // create publishers
    properties.setProperty(PUBLISHER_EXCHANGE, "spring");
    properties.setProperty(PUBLISHER_ROUTING_KEY, "do");

    properties.setProperty(HOSTS, "voznesenskiy.pyn.ru");
    factory = new ClientFactory(properties);
    Publisher publisher1 = factory.createPublisher().withJsonMessageConverter();

    properties.setProperty(HOSTS, "dev");
    factory = new ClientFactory(properties);
    Publisher publisher2 = factory.createPublisher().withJsonMessageConverter();

    publisher1.start();
    publisher2.start();

    // send something
    for (int i = 0; i < 100; i++) {
      send(publisher1, "loc", i);
      send(publisher2, "dev", i);
      Thread.sleep(100);
    }

    // shutdown
    Thread.sleep(5000);

    publisher1.stop();
    publisher2.stop();
    receiver.stop();
  }

  private static void send(Publisher publisher, String id, int counter) {
    Map<String, Object> body = new HashMap<String, Object>();
    body.put("counter", counter);
    body.put("id", id);
    publisher.send(body);
  }
}
