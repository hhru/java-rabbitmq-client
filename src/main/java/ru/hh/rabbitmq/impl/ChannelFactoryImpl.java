package ru.hh.rabbitmq.impl;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.hh.rabbitmq.ChannelFactory;
import ru.hh.rabbitmq.ConnectionFactory;
import ru.hh.rabbitmq.ConnectionFailedException;

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
    return openChannel(null, null, false, queueName, durableQueue, null);
  }

  public Channel openChannel(String exchangeName, String exchangeType, boolean durableExchange) throws IOException {
    return openChannel(exchangeName, exchangeType, durableExchange, null, false, null);
  }

  @Override
  public Channel openChannel(
      String exchangeName, String exchangeType, boolean durableExchange, String queueName, boolean durableQueue,
      String routingKey) throws IOException {
    Channel channel = openChannel();

    if (exchangeName != null && exchangeType != null) {
      logger.debug("Declaring exchange: {} / {} / {}", new Object[] { exchangeName, exchangeType, durableExchange });
      channel.exchangeDeclare(exchangeName, exchangeType, durableExchange);
    }

    if (queueName != null) {
      logger.debug("Declaring queue: {} / {}", queueName, durableQueue);
      channel.queueDeclare(queueName, durableQueue);
      if (routingKey != null && exchangeName != null) {
        logger.debug("Binding queue {} to exchange {} with routing key {}", new Object[] { queueName, exchangeName, routingKey });
        channel.queueBind(queueName, exchangeName, routingKey);
      }
    }
    return channel;
  }

  public Channel openChannel() throws IOException {
    logger.debug("Openning channel");
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
          throw new ConnectionFailedException("Can't connect to queue server", e);
        }
        logger.warn(
          String.format(
            "Attempt %d out of %d to reconnect to server has failed, sleeping then retrying", attempt,
            autoreconnect.getAttempts()), e);
        try {
          autoreconnect.getSleeper().sleep();
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
          throw new ConnectionFailedException("Sleep between autoreconnection attempts has been interrupted", e1);
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
