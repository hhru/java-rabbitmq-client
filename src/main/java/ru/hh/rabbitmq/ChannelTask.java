package ru.hh.rabbitmq;

import com.rabbitmq.client.Channel;

public interface ChannelTask {
  void run(Channel channel);
}
