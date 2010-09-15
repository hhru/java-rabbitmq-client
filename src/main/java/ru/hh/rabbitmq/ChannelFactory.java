package ru.hh.rabbitmq;

import com.rabbitmq.client.Channel;
import java.io.IOException;

public interface ChannelFactory {
  Channel openChannel(String queueName, boolean durableQueue) throws IOException;

  Channel openChannel(String exchangeName, String exchangeType, boolean durableExchange) throws IOException;

  Channel openChannel(
      String exchangeName, String exchangeType, boolean durableExchange, String queueName, boolean durableQueue,
      String routingKey) throws IOException;

  Channel openChannel() throws IOException;

  void returnChannel(Channel channel);

  void close();
}
