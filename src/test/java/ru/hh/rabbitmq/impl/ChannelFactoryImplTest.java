package ru.hh.rabbitmq.impl;

import com.headhunter.test.Mocks;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import ru.hh.rabbitmq.ConnectionFactory;

public class ChannelFactoryImplTest {
  private static final int QUEUE_QOS = 1;
  private Mocks mm;
  private ConnectionFactory factory;
  private ChannelFactoryImpl impl;

  @Before
  public void setUp() throws Exception {
    mm = new Mocks();
    factory = mm.createMock(ConnectionFactory.class);
    impl = new ChannelFactoryImpl(factory, QUEUE_QOS);
  }

  @Test
  public void testOpenChannel() throws IOException {
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
    mm.replay();
    impl.getChannel();
    mm.verify();
  }

  @Test
  public void testReturnChannel() throws IOException {
    Channel channel = mm.createMock(Channel.class);
    Connection connection = mm.createMock(Connection.class);
    
    channel.isOpen();
    mm.expectLastCall().andReturn(true).anyTimes();
    
    channel.getConnection();
    mm.expectLastCall().andReturn(connection).anyTimes();

    channel.close();
    mm.expectLastCall().once();
    
    factory.returnConnection(connection);
    mm.expectLastCall().once();
      
    mm.replay();
    impl.returnChannel(channel);
    mm.verify();
  }

}
