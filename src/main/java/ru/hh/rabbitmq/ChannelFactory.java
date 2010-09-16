package ru.hh.rabbitmq;

import com.rabbitmq.client.Channel;
import java.io.IOException;

public interface ChannelFactory {
  Channel openChannel() throws IOException;

  void returnChannel(Channel channel);

  void close();
}
