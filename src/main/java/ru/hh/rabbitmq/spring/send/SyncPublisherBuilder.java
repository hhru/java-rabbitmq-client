package ru.hh.rabbitmq.spring.send;

import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_HOSTS;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_TRANSACTIONAL;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ConfirmCallback;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ReturnCallback;
import org.springframework.amqp.support.converter.MessageConverter;
import ru.hh.rabbitmq.spring.ClientFactory;

public class SyncPublisherBuilder extends AbstractPublisherBuilder {

  public SyncPublisherBuilder(ClientFactory clientFactory) {
    super(clientFactory.createConnectionFactories(PUBLISHER_HOSTS), clientFactory.getProperties().getProperties());
    if (templates.size() > 1) {
      throw new IllegalArgumentException("Specified multiple hosts for sync publisher");
    }
    Boolean transactional = clientFactory.getProperties().getBoolean(PUBLISHER_TRANSACTIONAL);
    if (transactional != null) {
      for (RabbitTemplate template : templates) {
        template.setChannelTransacted(transactional);
      }
    }
  }

  public SyncPublisherBuilder setTransactional(boolean transactional) {
    for (RabbitTemplate template : templates) {
      template.setChannelTransacted(transactional);
    }
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
