package ru.hh.rabbitmq.impl;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.hh.rabbitmq.ConnectionFactory;

/**
 * Maintains single open connection, reconnects if necessary
 */
public class SingleConnectionFactory implements ConnectionFactory, ShutdownListener {
  private static final Logger logger = LoggerFactory.getLogger(SingleConnectionFactory.class);

  private final Address[] addresses;
  private final com.rabbitmq.client.ConnectionFactory connectionFactory;
  
  // guarded by synchronized(this)
  private Connection connection;
  
  private volatile boolean shuttingDown;

  /**
   * @see com.rabbitmq.client.ConnectionFactory#newConnection(com.rabbitmq.client.Address[])
   */
  public SingleConnectionFactory(com.rabbitmq.client.ConnectionFactory connectionFactory, Address... addresses) {
    this.connectionFactory = connectionFactory;
    this.addresses = addresses;
  }

  public synchronized Connection openConnection() throws IOException {
    logger.debug("Opening new connection");
    if (shuttingDown) {
      throw new IllegalStateException("Shutting down");
    }
    if (connection == null) {
      connection = connectionFactory.newConnection(addresses);
      connection.addShutdownListener(this);
    }
    // TODO check connection availability
    return connection;
  }

  public void returnConnection(Connection connection) {
    // nothing to do here
  }

  @Override
  public synchronized void close() {
    logger.debug("Closing factory");
    
    shuttingDown = true;
    
    if (connection == null) {
      return;
    }
    if (!connection.isOpen()) {
      logger.warn("Connection is already closing, ignoring");
      return;
    }
    try {
      logger.debug("Closing connection");
      connection.close();
    } catch (IOException e) {
      logger.warn("Error while closing connection, ignoring", e);
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
