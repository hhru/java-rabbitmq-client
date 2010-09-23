package ru.hh.rabbitmq;

import com.rabbitmq.client.Channel;

public interface ChannelFactory {
  Channel getChannel();

  void returnChannel(Channel channel);

  void close();
}
