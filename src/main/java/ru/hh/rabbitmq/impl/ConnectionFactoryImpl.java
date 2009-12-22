package ru.hh.rabbitmq.impl;

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

public class ConnectionFactoryImpl implements ConnectionFactory, ShutdownListener {
  private static final Logger logger = LoggerFactory.getLogger(ConnectionFactoryImpl.class);

  private ConnectionParameters connectionParameters;
  private SocketFactory socketFactory;
  private String hostName;
  private Integer portNumber;
  private Integer closeTimeout;

  private com.rabbitmq.client.ConnectionFactory connectionFactory;
  private volatile boolean initialized;
  private volatile boolean shuttingDown;

  public void setConnectionParameters(ConnectionParameters connectionParameters) {
    this.connectionParameters = connectionParameters;
  }

  public void setSocketFactory(SocketFactory socketFactory) {
    this.socketFactory = socketFactory;
  }

  public void setHostName(String hostName) {
    this.hostName = hostName;
  }

  public void setPortNumber(Integer portNumber) {
    this.portNumber = portNumber;
  }

  public void setCloseTimeout(Integer closeTimeout) {
    this.closeTimeout = closeTimeout;
  }

  public void init() {
    if (initialized) {
      throw new IllegalStateException("Already initialized!");
    }

    connectionFactory = new com.rabbitmq.client.ConnectionFactory(connectionParameters);
    if (socketFactory != null) {
      connectionFactory.setSocketFactory(socketFactory);
    }
    initialized = true;
    logger.debug("Finished initializing");
  }

  public Connection openConnection() throws IOException {
    logger.debug("Openning new connection");
    ensureRunning();
    Connection connection = connectionFactory.newConnection(hostName, portNumber);
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
