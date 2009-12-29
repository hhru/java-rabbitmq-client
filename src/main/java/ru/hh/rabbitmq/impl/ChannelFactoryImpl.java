package ru.hh.rabbitmq.impl;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.hh.rabbitmq.ChannelFactory;
import ru.hh.rabbitmq.ConnectionFactory;

public class ChannelFactoryImpl implements ChannelFactory {
  private static final Logger logger = LoggerFactory.getLogger(ChannelFactoryImpl.class);

  private ConnectionFactory connectionFactory;
  private Integer prefetchCount;
  private AutoreconnectProperties autoreconnect = new AutoreconnectProperties(0);

  private volatile Connection connection;
  private volatile boolean shuttingDown = false;

  public ChannelFactoryImpl(ConnectionFactory connectionFactory, Integer prefetchCount, AutoreconnectProperties autoreconnect) {
    this.connectionFactory = connectionFactory;
    this.prefetchCount = prefetchCount;
    this.autoreconnect = autoreconnect;
  }

  public Channel openChannel(String queueName, boolean durableQueue) throws IOException {
    logger.debug("Openning channel");
    Channel channel = openChannel();
    channel.queueDeclare(queueName, durableQueue);
    return channel;
  }

  public Channel openChannel() throws IOException {
    ensureConnectedAndRunning();
    Channel channel = connection.createChannel();
    if (prefetchCount != null) {
      channel.basicQos(prefetchCount);
    }
    return channel;
  }

  public void returnChannel(Channel channel) {
    logger.debug("Closing channel");
    if (channel == null) {
      return;
    }

    if (!channel.isOpen()) {
      logger.warn("Channel is already closing, ignoring");
      return;
    }

    try {
      channel.close();
    } catch (IOException e) {
      logger.warn("Error while closing channel, ignoring", e);
    }
  }

  private void ensureConnectedAndRunning() {
    if (shuttingDown) {
      throw new IllegalStateException("Shutting down");
    }
    ensureConnected();
  }

  private void ensureConnected() {
    int attempt = 0;
    while (connection == null || !connection.isOpen()) {
      attempt++;
      try {
        logger.debug("Connecting");
        connection = connectionFactory.openConnection();
        logger.debug("Connection is ready");
      } catch (IOException e) {
        if (attempt > autoreconnect.getAttempts()) {
          throw new RuntimeException("Can't connect to queue server", e);
        }
        logger.warn(
          String.format(
            "Attempt %d out of %d to reconnect to server has failed, sleeping then retrying", attempt,
            autoreconnect.getAttempts()), e);
        try {
          autoreconnect.getSleeper().sleep();
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Sleep between autoreconnection attempts has been interrupted", e1);
        }
      }
    }
  }

  public void close() {
    logger.debug("Closing");
    shuttingDown = true;
    connectionFactory.returnConnection(connection);
  }
}
