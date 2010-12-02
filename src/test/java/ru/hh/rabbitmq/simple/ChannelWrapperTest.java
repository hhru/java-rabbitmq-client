package ru.hh.rabbitmq.simple;

import com.headhunter.test.Mocks;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import junit.framework.Assert;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import ru.hh.rabbitmq.ChannelFactory;

public class ChannelWrapperTest {
  private static final String QUEUE_NAME = "one";
  private Mocks mm;
  private Channel channel;
  private ChannelWrapper wrapper;

  @Before
  public void setUp() throws IOException {
    mm = new Mocks();
    ChannelFactory factory = mm.createMock(ChannelFactory.class);
    wrapper = new ChannelWrapper();
    wrapper.setQueueName(QUEUE_NAME);
    wrapper.setFactory(factory);

    channel = mm.createMock(Channel.class);
    factory.getChannel();
    mm.expectLastCall().andReturn(channel).anyTimes();

    channel.isOpen();
    mm.expectLastCall().andReturn(true).anyTimes();
  }

  @Test
  public void testReceiveSingle() throws IOException, InterruptedException {
    DummyMessageReceiver receiver = new DummyMessageReceiver();

    Envelope envelope = new Envelope(1, false, "", QUEUE_NAME);
    GetResponse response = new GetResponse(envelope, null, null, 0);

    channel.basicGet(EasyMock.eq(QUEUE_NAME), EasyMock.eq(false));
    mm.expectLastCall().andReturn(response).anyTimes();

    channel.basicAck(EasyMock.eq(1L), EasyMock.eq(false));
    mm.expectLastCall().anyTimes();

    mm.replay();

    wrapper.receiveSingle(receiver);

    mm.verify();

    Assert.assertNotNull(receiver.message);
    Assert.assertEquals(receiver.message.getEnvelope(), envelope);
  }

  @Test
  public void testWaitAndReceiveMany() throws IOException, ShutdownSignalException, InterruptedException {
    DummyMessageReceiver receiver = new DummyMessageReceiver();

    channel.basicConsume(EasyMock.eq(QUEUE_NAME), EasyMock.eq(false), EasyMock.isA(QueueingConsumer.class));
    mm.expectLastCall().andReturn("zxc").anyTimes();

    channel.basicCancel(EasyMock.eq("zxc"));
    mm.expectLastCall().anyTimes();

    mm.replay();

    // wait for timeout (there are no messages in queue)
    wrapper.waitAndReceiveMany(receiver, 5L);

    mm.verify();

    Assert.assertNull(receiver.message);
    Assert.assertTrue(receiver.finishCalled);
    Assert.assertFalse(receiver.isEnoughCalled);
    Assert.assertTrue(receiver.startCalled);
  }
}
