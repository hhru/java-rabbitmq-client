package ru.hh.rabbitmq.spring.persistent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PersistentPublisherRegistry {
  private final Map<String, DatabaseQueueSender> senders;

  public PersistentPublisherRegistry() {
    senders = new ConcurrentHashMap<>();
  }

  public void registerSender(String key, DatabaseQueueSender sender) {
    DatabaseQueueSender previous = senders.put(key, sender);
    if (previous != null) {
      throw new IllegalStateException("Key " + key + "is not unique");
    }
  }

  public DatabaseQueueSender getSender(String key) {
    return senders.get(key);
  }
}
