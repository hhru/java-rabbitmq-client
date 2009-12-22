package ru.hh.rabbitmq.simple;

import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.util.Map;

public abstract class BodilessMessageReceiver implements MessageReceiver {
  @Override
  public void receive(Message message) throws IOException {
    receive(message.getProperties().getHeaders(), message.getEnvelope());
  }

  public abstract void receive(Map<String, Object> headers, Envelope envelope) throws IOException;
}
