package ru.hh.rabbitmq.spring.send;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.springframework.amqp.rabbit.transaction.RabbitTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import ru.hh.rabbitmq.spring.Receiver;
import ru.hh.rabbitmq.spring.SyncRabbitIntegrationTestBase;

public class SyncPublisherTransactionTest extends SyncRabbitIntegrationTestBase {

  @Test
  public void testTransaction() throws InterruptedException {
    final SyncPublisher publisher = publisher(HOST1, PORT1, true).withJsonMessageConverter().setTransactional(true).build();
    RabbitTransactionManager transactionManager = new RabbitTransactionManager(publisher.getTemplate().getConnectionFactory());
    TransactionTemplate transaction = new TransactionTemplate(transactionManager);
    publisher.startAsync();

    assertTrue(publisher.getTemplate().isChannelTransacted());

    MessageHandler handler = new MessageHandler();
    Receiver receiver = receiverAllHosts(false).withJsonListener(handler).forQueues(QUEUE1).start();

    final Map<String, Object> sentMessage = new HashMap<>();
    sentMessage.put("data", QUEUE1);

    transaction.execute(status -> {
      publisher.send(sentMessage);
      return null;
    });

    Map<String, Object> receivedMessage = handler.get();
    assertNotNull(receivedMessage);
    assertEquals(sentMessage, receivedMessage);

    publisher.stopSync();
    receiver.shutdown();
  }

  @Test
  public void testTransactionWithRollback() throws InterruptedException {
    final SyncPublisher publisher = publisher(HOST1, PORT1, true).withJsonMessageConverter().setTransactional(true).build();
    RabbitTransactionManager transactionManager = new RabbitTransactionManager(publisher.getTemplate().getConnectionFactory());
    TransactionTemplate transaction = new TransactionTemplate(transactionManager);
    publisher.startAsync();

    assertTrue(publisher.getTemplate().isChannelTransacted());

    MessageHandler handler = new MessageHandler();
    Receiver receiver = receiverAllHosts(false).withJsonListener(handler).forQueues(QUEUE1).start();

    final Map<String, Object> sentMessage = new HashMap<>();
    sentMessage.put("data", QUEUE1);

    try {
      transaction.execute(status -> {
        publisher.send(sentMessage);
        throw new PlannedException();
      });
    } catch (PlannedException e) {
    }

    Map<String, Object> receivedMessage = handler.get();
    assertNull(receivedMessage);

    publisher.stopSync();
    receiver.shutdown();
  }

  // XXX there should be also testTransactionDisabledWithRollback() method to test that builder.setTransactional(false) will forbid
  // transaction even if running in transaction context, but RabbitTransactionManager ignores that setting on RabbitTemplate and
  // enforces tx anyway.
  // It is not how regular transactions (i.e. in hh.ru) work - usually that setting is propagated and processed properly.
  // This was manually tested in hh.ru environment.

  private static class PlannedException extends RuntimeException {
  }
}
