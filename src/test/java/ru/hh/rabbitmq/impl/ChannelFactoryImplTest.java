package ru.hh.rabbitmq.impl;

import com.headhunter.test.Mocks;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import junit.framework.Assert;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import ru.hh.rabbitmq.ConnectionFactory;

public class ChannelFactoryImplTest {
  private static final int QUEUE_QOS = 1;
  private static final boolean QUEUE_DURABLE = true;
  private static final String QUEUE_NAME = "one";
  private Mocks mm;
  private ConnectionFactory factory;
  private ChannelFactoryImpl impl;

  @Before
  public void setUp() throws Exception {
    mm = new Mocks();
    factory = mm.createMock(ConnectionFactory.class);
    AutoreconnectProperties auto = new AutoreconnectProperties(0);
    impl = new ChannelFactoryImpl(factory, QUEUE_QOS, auto);
  }

  @Test
  public void testOpenChannel() throws IOException {
    mockOpenChannel();
    mm.replay();
    impl.getChannel();
    mm.verify();
  }

  @Test
  public void testOpenAfterClose() throws IOException {
    mockClose(null);
    mm.replay();
    impl.close();
    mm.verify();
    try {
      impl.getChannel();
      Assert.fail();
    } catch (IllegalStateException e) { }
  }

  @Test
  public void testReturnChannel() throws IOException {
    Channel channel = mm.createMock(Channel.class);

    channel.isOpen();
    mm.expectLastCall().andReturn(true).anyTimes();

    channel.close();
    mm.expectLastCall().anyTimes();

    mm.replay();
    impl.returnChannel(channel);
    mm.verify();
  }

  private void mockOpenChannel() throws IOException {
    Connection connection = mm.createMock(Connection.class);

    connection.isOpen();
    mm.expectLastCall().andReturn(true).anyTimes();

    factory.getConnection();
    mm.expectLastCall().andReturn(connection).anyTimes();

    Channel channel = mm.createMock(Channel.class);
    connection.createChannel();
    mm.expectLastCall().andReturn(channel).anyTimes();

    channel.basicQos(QUEUE_QOS);
    mm.expectLastCall().anyTimes();
  }

  private void mockClose(Connection connection) {
    factory.returnConnection(EasyMock.eq(connection));
    mm.expectLastCall().anyTimes();

    if (connection != null) {
      connection.isOpen();
      mm.expectLastCall().andReturn(false).anyTimes();
    }
  }
}
