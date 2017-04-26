package ru.hh.rabbitmq.spring.send;

import java.util.Collection;
import java.util.Properties;
import javax.annotation.Nullable;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ConfirmCallback;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ReturnCallback;
import org.springframework.amqp.support.converter.MessageConverter;
import ru.hh.metrics.StatsDSender;
import ru.hh.rabbitmq.spring.ConfigException;

public class SyncPublisherBuilder extends AbstractPublisherBuilder {
  @Nullable
  private final String serviceName;
  @Nullable
  private final StatsDSender statsDSender;

  public SyncPublisherBuilder(Collection<ConnectionFactory> connectionFactories,
                              Properties properties,
                              @Nullable
                              String serviceName,
                              @Nullable
                              StatsDSender statsDSender) {
    super(connectionFactories, properties);
    this.serviceName = serviceName;
    this.statsDSender = statsDSender;
    if (templates.size() > 1) {
      throw new ConfigException("Multiple hosts are not allowed for sync publisher");
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
    RabbitTemplate template = templates.iterator().next();
    MessageSender messageSender = new MessageSender(template, serviceName, statsDSender);
    return new SyncPublisher(commonName, template, messageSender);
  }

}
