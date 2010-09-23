package ru.hh.rabbitmq.impl;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionParameters;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import javax.net.SocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.hh.rabbitmq.ConnectionFactory;
import ru.hh.rabbitmq.NotInitializedException;

public class SingleConnectionFactory implements ConnectionFactory, ShutdownListener {
  private static final Logger logger = LoggerFactory.getLogger(SingleConnectionFactory.class);

  private ConnectionParameters connectionParameters;
  private Address[] addresses;
  private SocketFactory socketFactory;
  private Integer closeTimeout;

  private com.rabbitmq.client.ConnectionFactory connectionFactory;
  private volatile boolean initialized;
  private volatile boolean shuttingDown;

  public SingleConnectionFactory(ConnectionParameters connectionParameters, Address... addresses) {
    this.connectionParameters = connectionParameters;
    this.addresses = addresses;
  }

  public SingleConnectionFactory(ConnectionParameters connectionParameters, SocketFactory socketFactory, Address... addresses) {
    this.connectionParameters = connectionParameters;
    this.addresses = addresses;
    this.socketFactory = socketFactory;
  }

  public SingleConnectionFactory(
      ConnectionParameters connectionParameters, SocketFactory socketFactory, Integer closeTimeout, Address... addresses) {
    this.connectionParameters = connectionParameters;
    this.addresses = addresses;
    this.socketFactory = socketFactory;
    this.closeTimeout = closeTimeout;
  }

  public void init() {
    logger.debug("Started initializing");
    if (initialized) {
      throw new IllegalStateException("Already initialized!");
    }

    connectionFactory = new com.rabbitmq.client.ConnectionFactory();
    if (socketFactory != null) {
      connectionFactory.setSocketFactory(socketFactory);
    }
    connectionFactory.setUsername(connectionParameters.getUserName());
    connectionFactory.setPassword(connectionParameters.getPassword());
    connectionFactory.setVirtualHost(connectionParameters.getVirtualHost());
    connectionFactory.setRequestedChannelMax(connectionParameters.getRequestedChannelMax());
    connectionFactory.setRequestedFrameMax(connectionParameters.getRequestedFrameMax());
    connectionFactory.setRequestedHeartbeat(connectionParameters.getRequestedHeartbeat());
    initialized = true;
    logger.debug("Finished initializing");
  }

  public Connection openConnection() throws IOException {
    logger.debug("Openning new connection");
    ensureRunning();
    Connection connection = connectionFactory.newConnection(addresses);
    connection.addShutdownListener(this);
    return connection;
  }

  public void returnConnection(Connection connection) {
    logger.debug("Closing connection");
    if (connection == null) {
      return;
    }

    if (!connection.isOpen()) {
      logger.warn("Connection is already closing, ignoring");
      return;
    }

    try {
      if (closeTimeout != null) {
        connection.close(closeTimeout);
        return;
      }
      connection.close();
    } catch (IOException e) {
      logger.warn("Error while closing connection, ignoring", e);
    }
  }

  @Override
  public void close() {
    logger.debug("Closing");
    shuttingDown = true;
  }

  private void ensureRunning() {
    if (!initialized) {
      throw new NotInitializedException();
    }
    if (shuttingDown) {
      throw new IllegalStateException("Shutting down");
    }
  }

  @Override
  public void shutdownCompleted(ShutdownSignalException cause) {
    String description =
      (cause.isHardError() ? "connection" : "channel") + " shutdown, "
      + "reason: " + cause.getReason() + " reference:" + cause.getReference();
    if (!shuttingDown) {
      logger.warn(description, cause);
    } else {
      logger.info(description);
    }
  }
}
