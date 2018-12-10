package ru.hh.rabbitmq.spring.persistent;

import javax.annotation.Nullable;
import ru.hh.rabbitmq.spring.send.Destination;
import ru.hh.rabbitmq.spring.send.MessageSender;

public interface DatabaseQueueSender {
  String getDatabaseQueueName();
  String getConsumerName();
  MessageSender getMessageSender();
  void onAmpqException(Exception e, long eventId, long batchId, Destination type, Object data);
  @Nullable
  <T> T onConvertationException(Exception e, long eventId, String type, String data);

  /**
   * must be idempotent
   */
  void start();
}
