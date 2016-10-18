package ru.hh.rabbitmq.spring.send;

import java.util.Collection;
import java.util.Properties;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ConfirmCallback;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ReturnCallback;
import org.springframework.amqp.support.converter.MessageConverter;

public class SyncPublisherBuilder extends AbstractPublisherBuilder {

  public SyncPublisherBuilder(Collection<ConnectionFactory> connectionFactories, Properties properties) {
    super(connectionFactories, properties);
    if (templates.size() > 1) {
      throw new IllegalArgumentException("Specified multiple hosts for sync publisher");
    }

  }

  public SyncPublisherBuilder setTransactional(boolean transactional) {
    setTransactionalInternal(transactional);
    return this;
  }

  public SyncPublisherBuilder withMessageConverter(MessageConverter converter) {
    withMessageConverterInternal(converter);
    return this;
  }

  public SyncPublisherBuilder withJsonMessageConverter() {
    withJsonMessageConverterInternal();
    return this;
  }

  public SyncPublisherBuilder withConfirmCallback(ConfirmCallback callback) {
    withConfirmCallbackInternal(callback);
    return this;
  }

  public SyncPublisherBuilder withReturnCallback(ReturnCallback callback) {
    withReturnCallbackInternal(callback);
    return this;
  }

  public SyncPublisher build() {
    return new SyncPublisher(commonName, templates.iterator().next());
  }

}
