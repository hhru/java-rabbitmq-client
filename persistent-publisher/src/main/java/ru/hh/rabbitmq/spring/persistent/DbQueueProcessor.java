package ru.hh.rabbitmq.spring.persistent;

public interface DbQueueProcessor {
  String getKey();
  String convertToDb(Object message);
  <T> T convertFromDb(String messageData, Class<T> clazz);
}
