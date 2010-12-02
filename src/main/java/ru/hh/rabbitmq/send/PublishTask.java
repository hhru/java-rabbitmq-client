package ru.hh.rabbitmq.send;

import com.rabbitmq.client.Channel;
import java.util.Collection;
import ru.hh.rabbitmq.simple.Message;

// TODO kill me, send only data, all code should live in ChannelWorker
class PublishTask implements ChannelTask {
  private final Collection<Message> messages;
  private final Destination destination;
  private final boolean transactional;

  public PublishTask(Destination destination, Collection<Message> messages, boolean transactional) {
    this.messages = messages;
    this.destination = destination;
    this.transactional = transactional;
  }

  @Override
  public void run(Channel channel) {
    for (Message message : messages) {
      channel.basicPublish(destination.getExchange(), destination.getRoutingKey(), destination.isMandatory(), 
        destination.isImmediate(), message.getProperties(), message.getBody());
    }
    if (transactional) {
      channel.txCommit();
    }
  }

  @Override
  public boolean isTransactional() {
    return transactional;
  }
}
