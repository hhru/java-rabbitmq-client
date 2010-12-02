package ru.hh.rabbitmq.send;

import com.google.common.util.concurrent.AbstractFuture;
import com.rabbitmq.client.Channel;

class ChannelTaskFuture extends AbstractFuture<Void> implements ChannelTask {
  private final ChannelTask task;

  ChannelTaskFuture(ChannelTask task) {
    this.task = task;
  }

  @Override
  public void run(Channel channel) {
    try {
      if (!isCancelled()) {
        task.run(channel);
        set(null);
      }
    } catch (RuntimeException e) {
      setException(e);
      throw e;
    }
  }

  @Override
  public boolean isTransactional() {
    return task.isTransactional();
  }
}
