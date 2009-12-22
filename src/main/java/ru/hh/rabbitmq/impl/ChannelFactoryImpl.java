package ru.hh.rabbitmq.impl;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.hh.rabbitmq.ChannelFactory;
import ru.hh.rabbitmq.ConnectionFactory;

public class ChannelFactoryImpl implements ChannelFactory {
  private static final Logger logger = LoggerFactory.getLogger(ChannelFactoryImpl.class);

  private ConnectionFactory connectionFactory;
  private String queueName;
  private boolean durableQueue = true;
  private Integer prefetchCount;
  private AutoreconnectProperties autoreconnect = new AutoreconnectProperties(false);

  private Connection connection;
  private volatile boolean shuttingDown = false;

  public ChannelFactoryImpl(
      ConnectionFactory connectionFactory, String queueName, boolean durableQueue, Integer prefetchCount,
      AutoreconnectProperties autoreconnect) {
    this.connectionFactory = connectionFactory;
    this.queueName = queueName;
    this.durableQueue = durableQueue;
    this.prefetchCount = prefetchCount;
    this.autoreconnect = autoreconnect;
  }

  public String getQueueName() {
    return queueName;
  }

  public Channel openChannel() throws IOException {
    return openChannel(queueName);
  }

  public Channel openChannel(String queueName) throws IOException {
    logger.debug("Openning channel");
    ensureConnectedAndRunning();
    Channel channel = connection.createChannel();
    channel.queueDeclare(queueName, durableQueue);
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

  private void ensureConnectedAndRunning() throws IOException {
    if (shuttingDown) {
      throw new IllegalStateException("Shutting down");
    }
    ensureConnected();
  }

  private void ensureConnected() throws IOException {
    if (connection != null && connection.isOpen()) {
      return;
    }

    if (connection == null) {
      connection = connectionFactory.openConnection();
      return;
    }
    autoreconnect();
  }

  private void autoreconnect() {
    if (!autoreconnect.isEnabled() || autoreconnect.getAttempts() == 0) {
      throw new IllegalStateException("No connection is available and autoreconnect is disabled");
    }

    int attempt = 0;
    while (attempt <= autoreconnect.getAttempts()) {
      try {
        connection = connectionFactory.openConnection();
        break;
      } catch (IOException e) {
        logger.warn(String.format("Attempt %d out of %d to reconnect has failed", attempt, autoreconnect.getAttempts()), e);
        try {
          TimeUnit.MILLISECONDS.sleep(autoreconnect.getDelay());
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
          logger.warn("Sleep between autoreconnection attempts has been interrupted, ignoring", e1);
        }
      }
      attempt++;
    }
    if (connection == null || !connection.isOpen()) {
      throw new IllegalStateException("Failed to automatically reconnect to MQ");
    }
  }

  public void close() {
    shuttingDown = true;
    connectionFactory.returnConnection(connection);
  }
}
