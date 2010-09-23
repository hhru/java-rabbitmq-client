package ru.hh.rabbitmq.impl;

import com.headhunter.test.Mocks;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import junit.framework.Assert;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class SingleConnectionFactoryTest {
  private static final String HOST = "localhost";
  private static final int PORT = 1;

  private SingleConnectionFactory impl;
  private ConnectionFactory connectionFactory;
  private Address[] addresses;

  private Mocks mm;

  @Before
  public void setUp() throws Exception {
    mm = new Mocks();
    addresses = new Address[] { new Address(HOST, PORT) };
    connectionFactory = mm.createMock(ConnectionFactory.class);
    impl = new SingleConnectionFactory(connectionFactory, TimeUnit.SECONDS, 1, 3, addresses);
  }

  @Test
  public void testOpenConnection() throws IOException {
    Connection connection = mm.createMock(Connection.class);
    connectionFactory.newConnection(EasyMock.eq(addresses));
    mm.expectLastCall().andReturn(connection).anyTimes();
    connection.isOpen();
    mm.expectLastCall().andReturn(true).anyTimes();

    connection.addShutdownListener(impl);
    mm.expectLastCall().anyTimes();

    mm.replay();

    impl.getConnection();

    mm.verify();
    mm.reset();
    
    connection.close();
    mm.expectLastCall().once();
    connection.isOpen();
    mm.expectLastCall().andReturn(true).anyTimes();
    mm.replay();
    
    impl.close();
    
    mm.verify();
    
    try {
      impl.getConnection();
      Assert.fail();
    } catch (IllegalStateException e) { }
  }
}
