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
import static org.easymock.EasyMock.eq;

import org.junit.Test;
import ru.hh.rabbitmq.NackException;
import ru.hh.rabbitmq.simple.Message;
import ru.hh.rabbitmq.simple.MessageReceiver;

public class ReceiverTest {

  public static final String QUEUE_NAME = "myq";
  public static final Address ADDRESS = new Address("localhost", 5672);
  public static final long DELIVERY_TAG = 1L;

  @Test
  public void receive() throws ExecutionException, InterruptedException, TimeoutException, IOException {
    Mocks mocks = new Mocks();
    com.rabbitmq.client.ConnectionFactory connectionFactory = mocks.createMock(com.rabbitmq.client.ConnectionFactory.class);
    Envelope envelope = mockEnvelope(mocks);
    Message message = mockMessage(envelope);
    Channel channel = mockConnectionTo(ADDRESS, connectionFactory, mocks);
    mockReceive(mocks, channel, QUEUE_NAME, message, DELIVERY_TAG);

    mocks.replay();

    Receiver receiver = new Receiver(connectionFactory, TimeUnit.SECONDS, 1, 1, 1, ADDRESS);
    DummyReceiver dr = new DummyReceiver();
    receiver.receiveSingle(dr, QUEUE_NAME);

    receiver.close();
    mocks.verify();

    Message received = dr.getMessage();
    Assert.assertTrue(received.getProperties().getHeaders().containsKey("one"));
    Assert.assertTrue(received.getProperties().getHeaders().containsValue("two"));
    Assert.assertEquals(received.getProperties().getHeaders().size(), 1);
    Assert.assertEquals(received.getBody().length, 1);
  }

  @Test
  public void nackReQueue() throws IOException, InterruptedException {
    Mocks mocks = new Mocks();
    com.rabbitmq.client.ConnectionFactory connectionFactory = mocks.createMock(com.rabbitmq.client.ConnectionFactory.class);
    Envelope envelope = mockEnvelope(mocks);
    Message message = mockMessage(envelope);
    Channel channel = mockConnectionTo(ADDRESS, connectionFactory, mocks);
    mockReceive(mocks, channel, QUEUE_NAME, message, DELIVERY_TAG);

    Receiver receiver = new Receiver(connectionFactory, TimeUnit.SECONDS, 1, 1, 1, ADDRESS);
    boolean reQueue;

    reQueue = true;
    NackReceiver nr1 = new NackReceiver(reQueue, false);
    mockNack(mocks, channel, DELIVERY_TAG, reQueue);

    reQueue = false;
    NackReceiver nr2 = new NackReceiver(reQueue, false);
    mockNack(mocks, channel, DELIVERY_TAG, reQueue);

    mocks.replay();

    receiver.receiveSingle(nr1, QUEUE_NAME);
    receiver.receiveSingle(nr2, QUEUE_NAME);

    receiver.close();
    mocks.verify();
  }

  @Test(expected = NullPointerException.class)
  public void nackThrow() throws IOException, InterruptedException {
    Mocks mocks = new Mocks();
    com.rabbitmq.client.ConnectionFactory connectionFactory = mocks.createMock(com.rabbitmq.client.ConnectionFactory.class);
    Envelope envelope = mockEnvelope(mocks);
    Message message = mockMessage(envelope);
    Channel channel = mockConnectionTo(ADDRESS, connectionFactory, mocks);
    mockReceive(mocks, channel, QUEUE_NAME, message, DELIVERY_TAG);

    Receiver receiver = new Receiver(connectionFactory, TimeUnit.SECONDS, 1, 1, 1, ADDRESS);

    boolean reQueue = false;
    NackReceiver nr = new NackReceiver(reQueue, true);
    mockNack(mocks, channel, DELIVERY_TAG, reQueue);

    mocks.replay();

    receiver.receiveSingle(nr, QUEUE_NAME);
  }


  private void mockNack(Mocks mocks, Channel channel, long deliveryTag, boolean reQueue) throws IOException {
    channel.basicNack(eq(deliveryTag), eq(false), eq(reQueue));
    mocks.expectLastCall().once();
  }

  private Message mockMessage(Envelope envelope) {
    Map<String, Object> body = Maps.newHashMap();
    body.put("one", "two");
    return new Message(new byte[1], body, envelope);
  }

  private Envelope mockEnvelope(Mocks mocks) {
    Envelope envelope = mocks.createMock(Envelope.class);
    envelope.getDeliveryTag();
    mocks.expectLastCall().andReturn(DELIVERY_TAG).anyTimes();
    return envelope;
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
    channel.basicQos(eq(1));
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

  private void mockReceive(Mocks mocks, Channel channel, String queue, Message message, long deliveryTag) throws IOException {
    GetResponse response = mocks.createMock(GetResponse.class);

    channel.basicGet(eq(queue), eq(false));
    mocks.expectLastCall().andReturn(response).anyTimes();
    response.getEnvelope();
    mocks.expectLastCall().andReturn(message.getEnvelope()).anyTimes();
    response.getProps();
    mocks.expectLastCall().andReturn(message.getProperties()).anyTimes();
    response.getBody();
    mocks.expectLastCall().andReturn(message.getBody()).anyTimes();
    channel.basicAck(eq(deliveryTag), eq(false));
    mocks.expectLastCall().anyTimes();
  }

  private static class DummyReceiver implements MessageReceiver {
    private Message message;

    @Override
    public void receive(Message message) throws InterruptedException {
      this.message = message;
    }

    public Message getMessage() {
      return message;
    }
  }

  private static class NackReceiver implements MessageReceiver {
    private boolean reQueue;
    private boolean shouldThrow;

    private NackReceiver(boolean reQueue, boolean shouldThrow) {
      this.reQueue = reQueue;
      this.shouldThrow = shouldThrow;
    }

    @Override
    public void receive(Message message) throws InterruptedException, NackException {
      if (shouldThrow) {
        throw new NackException(reQueue, new NullPointerException());
      } else {
        throw new NackException(reQueue);
      }
    }
  }
}
