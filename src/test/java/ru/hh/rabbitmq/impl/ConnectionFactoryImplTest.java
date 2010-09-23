package ru.hh.rabbitmq.impl;

import com.headhunter.test.Mocks;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConnectionParameters;
import java.io.IOException;
import javax.net.SocketFactory;
import junit.framework.Assert;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import ru.hh.rabbitmq.BeanUtils;

public class ConnectionFactoryImplTest {
  private static final String HOST = "localhost";
  private static final int PORT = 1;
  private static final int CLOSE_TIMEOUT = 1;

  private SocketFactory socketFactory;
  private SingleConnectionFactory impl;
  private ConnectionParameters params;
  private ConnectionFactory connectionFactory;
  private Address[] addresses;

  private Mocks mm;

  @Before
  public void setUp() throws Exception {
    mm = new Mocks();
    socketFactory = mm.createMock(SocketFactory.class);
    params = new ConnectionParameters();
    addresses = new Address[] { new Address(HOST, PORT) };

    impl = new SingleConnectionFactory(params, socketFactory, CLOSE_TIMEOUT, addresses);
    impl.init();

    connectionFactory = mm.createMock(ConnectionFactory.class);

    BeanUtils.setField(impl, "connectionFactory", connectionFactory);
  }

  @Test
  public void testOpenConnection() throws IOException {
    testOpenConnectionInternal();

    impl.close();

    try {
      testOpenConnectionInternal();
      Assert.fail();
    } catch (IllegalStateException e) { }
  }

  private void testOpenConnectionInternal() throws IOException {
    Connection connection = EasyMock.createMock(Connection.class);
    connectionFactory.newConnection(EasyMock.eq(addresses));
    EasyMock.expectLastCall().andReturn(connection).anyTimes();

    connection.addShutdownListener(impl);
    EasyMock.expectLastCall().anyTimes();

    mm.replay();

    impl.openConnection();

    mm.verify();
    mm.reset();
  }

  public void testReturnConnection() throws IOException {
    Connection connection = EasyMock.createMock(Connection.class);
    connection.isOpen();
    mm.expectLastCall().andReturn(true).anyTimes();
    connection.close(EasyMock.eq(CLOSE_TIMEOUT));
    mm.expectLastCall().anyTimes();

    mm.replay();

    impl.returnConnection(connection);

    mm.verify();
  }
}
