package ru.hh.rabbitmq.spring.send;

import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_TRANSACTIONAL;
import java.util.Collection;
import java.util.Properties;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ConfirmCallback;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ReturnCallback;
import org.springframework.amqp.support.converter.MessageConverter;
import ru.hh.rabbitmq.spring.PropertiesHelper;

public class SyncPublisherBuilder extends AbstractPublisherBuilder {

  public SyncPublisherBuilder(Collection<ConnectionFactory> connectionFactories, Properties properties) {
    super(connectionFactories, properties);
    if (templates.size() > 1) {
      throw new IllegalArgumentException("Specified multiple hosts for sync publisher");
    }
    PropertiesHelper props = new PropertiesHelper(properties);
    Boolean transactional = props.getBoolean(PUBLISHER_TRANSACTIONAL);
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
