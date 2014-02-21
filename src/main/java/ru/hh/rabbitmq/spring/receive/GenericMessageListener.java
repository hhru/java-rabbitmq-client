package ru.hh.rabbitmq.spring.receive;

public interface GenericMessageListener<T> {

  void handleMessage(T t) throws Exception;

}
