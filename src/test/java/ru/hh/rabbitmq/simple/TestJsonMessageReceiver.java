package ru.hh.rabbitmq.simple;

import com.rabbitmq.client.Envelope;
import java.util.Map;

public class TestJsonMessageReceiver extends JsonMessageReceiver<Map<String, Object>> {
  private Map<String, Object> body;
  private Map<String, Object> headers;

  public TestJsonMessageReceiver() {
    super(Map.class);
  }

  @Override
  public void receive(Map<String, Object> body, Map<String, Object> headers, Envelope envelope) throws InterruptedException {
    this.body = body;
    this.headers = headers;
  }

  public Map<String, Object> getBody() {
    return body;
  }

  public Map<String, Object> getHeaders() {
    return headers;
  }
}
