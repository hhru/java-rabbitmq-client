package ru.hh.rabbitmq.simple;

import com.rabbitmq.client.AMQP;
import java.util.Date;
import java.util.Map;

public class BodilessMessage extends Message {
  private static final byte[] EMTPY_BODY = new byte[0];

  public BodilessMessage(Map<String, Object> headers) {
    super(EMTPY_BODY, null);
    this.properties =
      new AMQP.BasicProperties(
        "application/octet-stream", null, headers, 2, null, null, null, null, null, new Date(), null, null, null, null);
  }
}
