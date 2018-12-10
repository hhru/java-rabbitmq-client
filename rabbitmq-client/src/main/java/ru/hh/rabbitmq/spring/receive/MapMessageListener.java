package ru.hh.rabbitmq.spring.receive;

import java.util.Map;

public interface MapMessageListener extends GenericMessageListener<Map<String, Object>> {

  void handleMessage(Map<String, Object> message) throws Exception;

}
