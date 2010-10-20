package ru.hh.rabbitmq.simple;

import com.rabbitmq.client.Envelope;
import java.util.Map;

public class TestJsonObjectReceiver extends JsonMessageReceiver<DummyJsonObject> {
  private DummyJsonObject body;
  private Map<String, Object> headers;

  public TestJsonObjectReceiver() {
    super(DummyJsonObject.class);
  }

  @Override
  public void receive(DummyJsonObject body, Map<String, Object> headers, Envelope envelope) throws InterruptedException {
    this.body = body;
    this.headers = headers;
  }

  public DummyJsonObject getBody() {
    return body;
  }

  public Map<String, Object> getHeaders() {
    return headers;
  }
}
