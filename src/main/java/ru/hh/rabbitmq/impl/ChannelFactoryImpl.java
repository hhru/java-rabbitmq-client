package ru.hh.rabbitmq.impl;

import com.rabbitmq.client.Channel;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.hh.rabbitmq.ChannelFactory;
import ru.hh.rabbitmq.ConnectionFactory;

public class ChannelFactoryImpl implements ChannelFactory {
  private static final Logger logger = LoggerFactory.getLogger(ChannelFactoryImpl.class);

  private ConnectionFactory connectionFactory;
  
  public ChannelFactoryImpl(ConnectionFactory connectionFactory) {
    this.connectionFactory = connectionFactory;
  }

  public Channel getChannel() throws IOException {
    logger.debug("Opening channel");
    return connectionFactory.getConnection().createChannel();
  }

  public void returnChannel(Channel channel) {
    if (channel == null) {
      return;
    }
    
    if (!channel.isOpen()) {
      logger.warn("Channel is already closed, ignoring");
      return;
    }
    try {
      logger.debug("Closing channel");
      channel.close();
    } catch (IOException e) {
      logger.warn("Error while closing channel, ignoring", e);
    }
    connectionFactory.returnConnection(channel.getConnection());
  }

  public void close() {
    // nothing to do
  }
}
