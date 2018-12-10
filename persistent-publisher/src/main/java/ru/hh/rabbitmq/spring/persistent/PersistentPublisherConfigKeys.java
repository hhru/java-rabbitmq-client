package ru.hh.rabbitmq.spring.persistent;

public final class PersistentPublisherConfigKeys {

  public static final String PERSISTENT_PUBLISHER_PREFIX = "persistent.publisher.mq";
  public static final String JERSEY_BASE_PATH_PROPERTY = "jerseyBasePath";
  public static final String UPSTREAM_PROPERTY = "upstream";
  public static final String DB_QUEUE_NAME_PROPERTY = "databaseQueueName";
  public static final String POLLING_INTERVAL_SEC_PROPERTY = "pollingIntervalSec";
  public static final String RETRY_DELAY_SEC_PROPERTY = "retryDelaySec";

  private PersistentPublisherConfigKeys() {
  }
}
