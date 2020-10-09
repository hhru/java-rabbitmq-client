package ru.hh.rabbitmq.spring.persistent;

import java.util.Map;
import static java.util.stream.Collectors.toMap;

public interface DbQueueProcessor {
  String getKey();
  String convertToDb(Object message);
  <T> T convertFromDb(String messageData, Class<T> clazz);
  default <T> Map<Long, T> batchConvertFromDb(Map<Long, String> messagesData, Class<T> clazz) {
    return messagesData.entrySet().stream().collect(toMap(Map.Entry::getKey, e -> convertFromDb(e.getValue(), clazz)));
  }
}
