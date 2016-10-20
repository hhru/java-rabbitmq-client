package ru.hh.rabbitmq.spring.send;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.springframework.amqp.rabbit.transaction.RabbitTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import ru.hh.rabbitmq.spring.Receiver;
import ru.hh.rabbitmq.spring.SyncRabbitIntegrationTestBase;

public class SyncPublisherTransactionTest extends SyncRabbitIntegrationTestBase {

  @Test
  public void testTransaction() throws InterruptedException {
    final SyncPublisher publisher = publisher(HOST1, true).withJsonMessageConverter().build();
    RabbitTransactionManager transactionManager = new RabbitTransactionManager(publisher.getTemplate().getConnectionFactory());
    TransactionTemplate transaction = new TransactionTemplate(transactionManager);
    publisher.startAsync();

    MessageHandler handler = new MessageHandler();
    Receiver receiver = receiverAllHosts(false).withJsonListener(handler).forQueues(QUEUE1).start();

    final Map<String, Object> sentMessage = new HashMap<>();
    sentMessage.put("data", QUEUE1);

    transaction.execute(new TransactionCallback<Void>() {
      @Override
      public Void doInTransaction(@SuppressWarnings("unused") TransactionStatus status) {
        publisher.send(sentMessage);
        return null;
      }
    });

    Map<String, Object> receivedMessage = handler.get();
    assertNotNull(receivedMessage);
    assertEquals(sentMessage, receivedMessage);

    publisher.stopSync();
    receiver.shutdown();
  }

  @Test
  public void testTransactionWithRollback() throws InterruptedException {
    final SyncPublisher publisher = publisher(HOST1, true).withJsonMessageConverter().build();
    RabbitTransactionManager transactionManager = new RabbitTransactionManager(publisher.getTemplate().getConnectionFactory());
    TransactionTemplate transaction = new TransactionTemplate(transactionManager);
    publisher.startAsync();

    MessageHandler handler = new MessageHandler();
    Receiver receiver = receiverAllHosts(false).withJsonListener(handler).forQueues(QUEUE1).start();

    final Map<String, Object> sentMessage = new HashMap<>();
    sentMessage.put("data", QUEUE1);


    try {
      transaction.execute(new TransactionCallback<Void>() {
        @Override
        public Void doInTransaction(@SuppressWarnings("unused") TransactionStatus status) {
          publisher.send(sentMessage);
          throw new PlannedException();
        }
      });
    }
    catch (PlannedException e) {
    }

    Map<String, Object> receivedMessage  = handler.get();
    assertNull(receivedMessage);

    publisher.stopSync();
    receiver.shutdown();
  }

  private static class PlannedException extends RuntimeException {
  }
}
