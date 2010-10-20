package ru.hh.rabbitmq.simple;

import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.util.Map;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import ru.hh.rabbitmq.util.ObjectMapperHolder;

public abstract class JsonMessageReceiver<T> implements MessageReceiver {
  private Class<?> objectClass;

  public JsonMessageReceiver(Class<?> objectClass) {
    this.objectClass = objectClass;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void receive(Message message) throws InterruptedException {
    byte[] body = message.getBody();

    T parsed;
    try {
      parsed = (T) ObjectMapperHolder.get().readValue(message.getBody(), 0, body.length, objectClass);
    } catch (JsonParseException e) {
      throw new IllegalArgumentException("Can't parse json body of message", e);
    } catch (JsonMappingException e) {
      throw new IllegalArgumentException("Can't parse json body of message", e);
    } catch (IOException e) {
      throw new IllegalArgumentException("Can't parse json body of message", e);
    }
    receive(parsed, message.getProperties().getHeaders(), message.getEnvelope());
  }

  public abstract void receive(T body, Map<String, Object> headers, Envelope envelope) throws InterruptedException;
}
