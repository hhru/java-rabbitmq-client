package ru.hh.rabbitmq;

import com.rabbitmq.client.Channel;
import java.io.IOException;

public interface ChannelFactory {
  Channel openChannel() throws IOException;

  Channel openChannel(String queueName) throws IOException;

  void returnChannel(Channel channel);

  String getQueueName();

  void close();
}
