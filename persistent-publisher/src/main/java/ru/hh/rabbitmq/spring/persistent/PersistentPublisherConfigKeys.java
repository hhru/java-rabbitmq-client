package ru.hh.rabbitmq.spring.persistent;

public final class PersistentPublisherConfigKeys {

  public static final String PERSISTENT_PUBLISHER_PREFIX = "persistent.publisher.mq";
  public static final String DB_QUEUE_NAME_PROPERTY = "databaseQueueName";
  public static final String DB_QUEUE_CONSUMER_NAME_PROPERTY = "databaseQueueConsumerName";
  public static final String RABBIT_ERROR_QUEUE_PROPERTY = "errorLogTableName";
  public static final String RETRY_DELAY_SEC_PROPERTY = "retryDelaySec";

  private PersistentPublisherConfigKeys() {
  }
}
