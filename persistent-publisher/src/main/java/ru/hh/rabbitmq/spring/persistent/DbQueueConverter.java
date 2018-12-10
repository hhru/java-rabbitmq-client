package ru.hh.rabbitmq.spring.persistent;

public interface DbQueueConverter {
  String getKey();
  String convertToDb(Object message);
  <T> T convertFromDb(String messageData, Class<T> clazz);
}
