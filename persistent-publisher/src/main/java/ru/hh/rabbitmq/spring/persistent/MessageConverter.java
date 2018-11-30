package ru.hh.rabbitmq.spring.persistent;

public interface MessageConverter {
  String getKey();
  String convertToDb(Object message);
  <T> T convertFromDb(String messageData, Class<T> clazz);
}
