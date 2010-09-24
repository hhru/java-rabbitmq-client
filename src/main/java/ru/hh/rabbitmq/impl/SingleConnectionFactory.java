package ru.hh.rabbitmq.impl;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.hh.rabbitmq.ConnectionFactory;
import ru.hh.rabbitmq.ConnectionFailedException;

/**
 * Maintains single open connection, reconnects if necessary
 */
public class SingleConnectionFactory implements ConnectionFactory, ShutdownListener {
  private static final Logger logger = LoggerFactory.getLogger(SingleConnectionFactory.class);

  private final Address[] addresses;
  private final com.rabbitmq.client.ConnectionFactory connectionFactory;
  private final TimeUnit retryUnit;
  private final long retryDelay;
  private final int attempts;
  
  // guarded by synchronized(this)
  private Connection connection;
  
  private volatile boolean shuttingDown;

  /**
   * @see com.rabbitmq.client.ConnectionFactory#newConnection(com.rabbitmq.client.Address[])
   */
  public SingleConnectionFactory(com.rabbitmq.client.ConnectionFactory connectionFactory, TimeUnit retryUnit, long retryDelay,
                                 int attempts, Address... addresses) {
    this.connectionFactory = connectionFactory;
    this.addresses = addresses;
    this.retryUnit = retryUnit;
    this.retryDelay = retryDelay;
    this.attempts = attempts;
  }

  public synchronized Connection getConnection() {
    logger.debug("Opening new connection");
    int remains = attempts;
    while ((connection == null || !connection.isOpen()) && !shuttingDown && (remains > 0 || attempts == 0)) {
      try {
        remains--;
        connection = connectionFactory.newConnection(addresses);
        connection.addShutdownListener(this);
      } catch (IOException connectionError) {
        if (shuttingDown) {
          // nothing to do, will bail out later
        } else if (remains > 0) {
          logger.warn("connection attempt failed, retrying", connectionError);
          try {
            retryUnit.sleep(retryDelay);
          } catch (InterruptedException interrupt) {
            throw new ConnectionFailedException("retry sleep was interrupted", interrupt);
          }
        } else {
          throw new ConnectionFailedException("Can't connect to queue server", connectionError);
        }
      }
    }
    if (shuttingDown) {
      throw new IllegalStateException("Shutting down");
    }
    return connection;
  }

  public void returnConnection(Connection connection) {
    // nothing to do here
  }

  @Override
  public void close() {
    logger.debug("Closing factory");
    
    shuttingDown = true;
    
    synchronized (this) {
      if (connection == null) {
        return;
      }
      if (!connection.isOpen()) {
        logger.warn("Connection is already closed, ignoring");
        return;
      }
      try {
        logger.debug("Closing connection");
        connection.close();
      } catch (IOException e) {
        logger.warn("Error while closing connection, ignoring", e);
      }
    }
  }

  @Override
  public void shutdownCompleted(ShutdownSignalException cause) {
    String description =
      (cause.isHardError() ? "connection" : "channel") + " shutdown, "
      + "reason: " + cause.getReason() + " reference:" + cause.getReference();
    if (!shuttingDown) {
      logger.error(description, cause);
    } else {
      logger.info(description);
    }
  }
}
