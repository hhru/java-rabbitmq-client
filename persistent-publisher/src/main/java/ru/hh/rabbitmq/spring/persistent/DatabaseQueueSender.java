package ru.hh.rabbitmq.spring.persistent;

import java.util.Optional;
import javax.annotation.Nullable;
import ru.hh.rabbitmq.spring.persistent.dto.TargetedDestination;
import ru.hh.rabbitmq.spring.send.MessageSender;

public interface DatabaseQueueSender {
  String getDatabaseQueueName();
  Optional<String> getErrorTableName();
  String getConsumerName();
  MessageSender getMessageSender();
  DbQueueProcessor getConverter(String converterKey);
  void onAmpqException(Exception e, long eventId, long batchId, TargetedDestination destination, Object message);
  @Nullable
  <T> T onConvertationException(Exception e, long eventId, String destinationContent, String messageContent);
}
