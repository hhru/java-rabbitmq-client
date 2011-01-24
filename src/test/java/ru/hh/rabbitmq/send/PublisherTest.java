package ru.hh.rabbitmq.send;

import com.headhunter.test.Mocks;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.ShutdownListener;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.easymock.EasyMock;
import org.junit.Test;
import ru.hh.rabbitmq.simple.BodilessMessage;
import ru.hh.rabbitmq.simple.Message;
import static org.easymock.EasyMock.anyBoolean;
import static org.easymock.EasyMock.aryEq;

public class PublisherTest {
  @Test
  public void send() throws ExecutionException, InterruptedException, TimeoutException, IOException {
    Mocks mocks = new Mocks();
    
    com.rabbitmq.client.ConnectionFactory connectionFactory = mocks.createMock(com.rabbitmq.client.ConnectionFactory.class);
    mockConnectionTo(new Address("localhost", 5672), connectionFactory, mocks);
    
    mocks.replay();
    Publisher publisher = new Publisher(connectionFactory, TimeUnit.SECONDS, 1, 1, 3, "localhost", 5672);
    Destination destination = new Destination("", "trash", true, false);
    Message message = new BodilessMessage(new HashMap<String, Object>());
    List<Message> messages = Arrays.asList(message, message, message);
    List<Future<Void>> futures = new LinkedList<Future<Void>>();
    for (int i = 0; i < 3; i++) {
      futures.add(publisher.send(destination, messages));
    }
    for (Future<Void> future : futures) {
      future.get(1000, TimeUnit.MILLISECONDS);
    }
    publisher.close();
    mocks.verify();
  }

  private void mockConnectionTo(Address address, ConnectionFactory connectionFactory, Mocks mocks) throws IOException {
    Connection connection = mocks.createMock(Connection.class);
    connection.isOpen();
    mocks.expectLastCall().andReturn(true).anyTimes();
    connectionFactory.newConnection(aryEq(new Address[] {address}));
    mocks.expectLastCall().andReturn(connection).once();
    connection.addShutdownListener(EasyMock.<ShutdownListener>anyObject());
    mocks.expectLastCall().once();
    Channel channel = mocks.createMock(Channel.class);
    channel.isOpen();
    mocks.expectLastCall().andReturn(true).anyTimes();
    connection.createChannel();
    mocks.expectLastCall().andReturn(channel).anyTimes();
    channel.setReturnListener(EasyMock.<ReturnListener>anyObject());
    mocks.expectLastCall().anyTimes();
    channel.txSelect();
    mocks.expectLastCall().andReturn(null).anyTimes();
    channel.close();
    mocks.expectLastCall().anyTimes();
    channel.getConnection();
    mocks.expectLastCall().andReturn(connection).anyTimes();
    channel.basicPublish(EasyMock.<String>anyObject(), EasyMock.<String>anyObject(), anyBoolean(), anyBoolean(), EasyMock.<AMQP.BasicProperties>anyObject(), EasyMock.<byte[]>anyObject());
    mocks.expectLastCall().anyTimes();
    connection.close();
    mocks.expectLastCall().once();
  }
}
