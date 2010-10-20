package ru.hh.rabbitmq.simple;

import java.util.Map;

public class BodilessMessage extends Message {
  private static final byte[] EMTPY_BODY = new byte[0];

  public BodilessMessage(Map<String, Object> headers) {
    super(EMTPY_BODY, headers);
  }
}
