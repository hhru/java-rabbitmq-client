package ru.hh.rabbitmq.spring.persistent;

import java.util.Collection;
import java.util.Map;

public class PersistentPublisherRegistry {
  private final Map<String, DbQueueConverter> converters;
  private final Map<String, DatabaseQueueSender> senders;

  public PersistentPublisherRegistry(Map<String, DatabaseQueueSender> senders, Map<String, DbQueueConverter> converters) {
    this.converters = converters;
    this.senders = senders;
  }

  public void registerConverter(String key, DbQueueConverter converter) {
    DbQueueConverter previous = converters.put(key, converter);
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

  public DbQueueConverter getMessageConverter(String key) {
    return converters.get(key);
  }

  public DatabaseQueueSender getSender(String key) {
    return senders.get(key);
  }

  Collection<DatabaseQueueSender> getSenders() {
    return senders.values();
  }
}
