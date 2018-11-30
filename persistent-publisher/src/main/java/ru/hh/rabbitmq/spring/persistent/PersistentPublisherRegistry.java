package ru.hh.rabbitmq.spring.persistent;

import java.util.Collections;
import java.util.Map;

public class PersistentPublisherRegistry {
  private final Map<String, MessageConverter> converters;
  private final Map<String, DatabaseQueueSender> senders;

  public PersistentPublisherRegistry(Map<String, DatabaseQueueSender> senders) {
    converters = Collections.singletonMap(JacksonMessageConverter.INSTANCE.getKey(), JacksonMessageConverter.INSTANCE);
    this.senders = senders;
  }

  public void registerConverter(String key, MessageConverter converter) {
    MessageConverter previous = converters.put(key, converter);
    if (previous != null) {
      throw new IllegalStateException("Key " + key + "is not unique");
    }
  }

  public void registerSender(String key, DatabaseQueueSender sender) {
    DatabaseQueueSender previous = senders.put(key, sender);
    if (previous != null) {
      throw new IllegalStateException("Key " + key + "is not unique");
    }
  }

  public MessageConverter getMessageConverter(String key) {
    return converters.get(key);
  }

  public DatabaseQueueSender getSender(String key) {
    return senders.get(key);
  }
}
