package ru.hh.rabbitmq.receive;

import com.google.common.collect.Maps;
import com.headhunter.test.Mocks;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.ShutdownListener;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import junit.framework.Assert;
import org.easymock.EasyMock;
import static org.easymock.EasyMock.aryEq;
import org.junit.Test;
import ru.hh.rabbitmq.simple.Message;
import ru.hh.rabbitmq.simple.MessageReceiver;

public class ReceiverTest {
  @Test
  public void receive() throws ExecutionException, InterruptedException, TimeoutException, IOException {
    Mocks mocks = new Mocks();

    com.rabbitmq.client.ConnectionFactory connectionFactory = mocks.createMock(com.rabbitmq.client.ConnectionFactory.class);

    Envelope envelope = mocks.createMock(Envelope.class);
    envelope.getDeliveryTag();
    mocks.expectLastCall().andReturn(1).anyTimes();

    Map<String, Object> body = Maps.newHashMap();
    body.put("one", "two");
    Message message = new Message(new byte[1], body, envelope);
    Channel channel = mockConnectionTo(new Address("localhost", 5672), connectionFactory, mocks);
    mockReceive(channel, "myq", message, mocks, 1);

    mocks.replay();

    Receiver receiver = new Receiver(connectionFactory, TimeUnit.SECONDS, 1, 1, 1, new Address("localhost", 5672));
    DummyReceiver dr = new DummyReceiver();
    receiver.receiveSingle(dr, "myq");

    receiver.close();
    mocks.verify();

    Message received = dr.getMessage();
    Assert.assertTrue(received.getProperties().getHeaders().containsKey("one"));
    Assert.assertTrue(received.getProperties().getHeaders().containsValue("two"));
    Assert.assertEquals(received.getProperties().getHeaders().size(), 1);
    Assert.assertEquals(received.getBody().length, 1);
  }

  private Channel mockConnectionTo(Address address, ConnectionFactory connectionFactory, Mocks mocks) throws IOException {
    Connection connection = mocks.createMock(Connection.class);
    connection.isOpen();
    mocks.expectLastCall().andReturn(true).anyTimes();
    connectionFactory.newConnection(aryEq(new Address[] { address }));
    mocks.expectLastCall().andReturn(connection).once();
    connection.addShutdownListener(EasyMock.<ShutdownListener>anyObject());
    mocks.expectLastCall().once();

    Channel channel = mocks.createMock(Channel.class);
    channel.isOpen();
    mocks.expectLastCall().andReturn(true).anyTimes();
    channel.basicQos(EasyMock.eq(1));
    mocks.expectLastCall().anyTimes();
    channel.setReturnListener(EasyMock.<ReturnListener>anyObject());
    mocks.expectLastCall().anyTimes();
    channel.close();
    mocks.expectLastCall().anyTimes();
    channel.getConnection();
    mocks.expectLastCall().andReturn(connection).anyTimes();

    connection.createChannel();
    mocks.expectLastCall().andReturn(channel).anyTimes();
    connection.close();
    mocks.expectLastCall().once();
    return channel;
  }

  private void mockReceive(Channel channel, String queue, Message message, Mocks mocks, long deliveryTag) throws IOException {
    GetResponse response = mocks.createMock(GetResponse.class);

    channel.basicGet(EasyMock.eq(queue), EasyMock.eq(false));
    mocks.expectLastCall().andReturn(response).anyTimes();
    response.getEnvelope();
    mocks.expectLastCall().andReturn(message.getEnvelope()).anyTimes();
    response.getProps();
    mocks.expectLastCall().andReturn(message.getProperties()).anyTimes();
    response.getBody();
    mocks.expectLastCall().andReturn(message.getBody());
    channel.basicAck(EasyMock.eq(deliveryTag), EasyMock.eq(false));
    mocks.expectLastCall().anyTimes();
  }

  private class DummyReceiver implements MessageReceiver {
    private Message message;

    @Override
    public void receive(Message message) throws InterruptedException {
      this.message = message;
    }

    public Message getMessage() {
      return message;
    }
  }
}
