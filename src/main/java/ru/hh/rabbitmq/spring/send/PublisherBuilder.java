package ru.hh.rabbitmq.spring.send;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ConfirmCallback;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ReturnCallback;
import org.springframework.amqp.support.converter.MessageConverter;
import ru.hh.rabbitmq.spring.PropertiesHelper;

import java.util.Collection;
import java.util.Properties;

import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_INNER_QUEUE_SHUTDOWN_MS;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_INNER_QUEUE_SIZE;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_RETRY_DELAY_MS;

public class PublisherBuilder extends AbstractPublisherBuilder {

  private final int innerQueueSize;
  private final int innerQueueShutdownMs;
  private final int retryDelayMs;

  public PublisherBuilder(Collection<ConnectionFactory> connectionFactories, Properties properties) {
    super(connectionFactories, properties);
    PropertiesHelper props = new PropertiesHelper(properties);
    innerQueueSize = props.getInteger(PUBLISHER_INNER_QUEUE_SIZE, 1000);
    innerQueueShutdownMs = props.getInteger(PUBLISHER_INNER_QUEUE_SHUTDOWN_MS, 3000);
    retryDelayMs = props.getInteger(PUBLISHER_RETRY_DELAY_MS, 2000);
  }

  public PublisherBuilder withMessageConverter(MessageConverter converter) {
    withMessageConverterInternal(converter);
    return this;
  }

  public PublisherBuilder withJsonMessageConverter() {
    withJsonMessageConverterInternal();
    return this;
  }

  public PublisherBuilder withConfirmCallback(ConfirmCallback callback) {
    withConfirmCallbackInternal(callback);
    return this;
  }

  public PublisherBuilder withReturnCallback(ReturnCallback callback) {
    withReturnCallbackInternal(callback);
    return this;
  }

  public Publisher build() {
    return new Publisher(commonName, innerQueueSize, templates, retryDelayMs, useMDC, innerQueueShutdownMs);
  }
}
