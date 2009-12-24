package ru.hh.rabbitmq;

import com.rabbitmq.client.Channel;
import java.io.IOException;

public interface ChannelFactory {
  // TODO: remove queueName or add openChannel() without paramethers
  Channel openChannel(String queueName, boolean durableQueue) throws IOException;

  void returnChannel(Channel channel);

  void close();
}
