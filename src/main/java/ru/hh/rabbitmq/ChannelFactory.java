package ru.hh.rabbitmq;

import com.rabbitmq.client.Channel;
import java.io.IOException;

//TODO remove completely, use connectionFactory.getConnection() for raw access, or ChannelWrapper for high level operations
public interface ChannelFactory {
  Channel getChannel() throws IOException;

  void returnChannel(Channel channel);

  void close();
}
