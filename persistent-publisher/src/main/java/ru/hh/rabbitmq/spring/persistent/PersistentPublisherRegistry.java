package ru.hh.rabbitmq.spring.persistent;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PersistentPublisherRegistry {
  private final Map<String, DbQueueConverter> converters;
  private final Map<String, DatabaseQueueSender> senders;

  public PersistentPublisherRegistry() {
    this.converters = new ConcurrentHashMap<>();
    this.senders = new ConcurrentHashMap<>();
  }

  public void registerConverter(DbQueueConverter converter) {
    DbQueueConverter previous = converters.put(converter.getKey(), converter);
    if (previous != null) {
      throw new IllegalStateException("Key " + converter.getKey() + "is not unique");
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
