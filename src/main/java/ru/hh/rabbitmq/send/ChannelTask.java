package ru.hh.rabbitmq.send;

import com.rabbitmq.client.Channel;

interface ChannelTask {
  void run(Channel channel);
}
