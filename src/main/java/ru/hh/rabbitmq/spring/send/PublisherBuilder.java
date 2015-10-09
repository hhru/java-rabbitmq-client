package ru.hh.rabbitmq.spring.send;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ConfirmCallback;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ReturnCallback;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import ru.hh.rabbitmq.spring.MDCMessagePropertiesConverter;
import ru.hh.rabbitmq.spring.PropertiesHelper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static java.util.Collections.unmodifiableList;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_EXCHANGE;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_INNER_QUEUE_SHUTDOWN_MS;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_INNER_QUEUE_SIZE;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_MANDATORY;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_NAME;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_RETRY_DELAY_MS;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_ROUTING_KEY;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_TRANSACTIONAL;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_USE_MDC;

public class PublisherBuilder {

  private final String commonName;
  private final int innerQueueSize;
  private final Collection<RabbitTemplate> templates;
  private final int retryDelayMs;
  private final boolean useMDC;
  private final int innerQueueShutdownMs;

  public PublisherBuilder(Collection<ConnectionFactory> connectionFactories, Properties properties) {
    PropertiesHelper props = new PropertiesHelper(properties);

    commonName = props.getString(PUBLISHER_NAME, "");
    innerQueueSize = props.getInteger(PUBLISHER_INNER_QUEUE_SIZE, 1000);

    String exchange = props.getString(PUBLISHER_EXCHANGE);
    String routingKey = props.getString(PUBLISHER_ROUTING_KEY);
    Boolean mandatory = props.getBoolean(PUBLISHER_MANDATORY);
    Boolean transactional = props.getBoolean(PUBLISHER_TRANSACTIONAL);
    useMDC = props.getBoolean(PUBLISHER_USE_MDC, false);
    retryDelayMs = props.getInteger(PUBLISHER_RETRY_DELAY_MS, 2000);

    List<RabbitTemplate> templates = new ArrayList<>(connectionFactories.size());
    for (ConnectionFactory factory : connectionFactories) {
      RabbitTemplate template = new RabbitTemplate(factory);

      if (exchange != null) {
        template.setExchange(exchange);
      }

      if (routingKey != null) {
        template.setRoutingKey(routingKey);
      }

      if (mandatory != null) {
        template.setMandatory(mandatory);
      }

      if (transactional != null) {
        template.setChannelTransacted(transactional);
      }

      if (useMDC) {
        template.setMessagePropertiesConverter(new MDCMessagePropertiesConverter());
      }

      templates.add(template);
    }
    this.templates = unmodifiableList(templates);

    innerQueueShutdownMs = props.getInteger(PUBLISHER_INNER_QUEUE_SHUTDOWN_MS, 3000);
  }

  /** @return Immutable collection of all rabbit templates for additional configuration */
  public Collection<RabbitTemplate> getRabbitTemplates() {
    return templates;
  }

  public PublisherBuilder setTransactional(boolean transactional) {
    for (RabbitTemplate template : templates) {
      template.setChannelTransacted(transactional);
    }
    return this;
  }

  public PublisherBuilder withMessageConverter(MessageConverter converter) {
    for (RabbitTemplate template : templates) {
      template.setMessageConverter(converter);
    }
    return this;
  }

  public PublisherBuilder withJsonMessageConverter() {
    Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter();
    return withMessageConverter(converter);
  }

  public PublisherBuilder withConfirmCallback(ConfirmCallback callback) {
    for (RabbitTemplate template : templates) {
      template.setConfirmCallback(callback);
    }
    return this;
  }

  public PublisherBuilder withReturnCallback(ReturnCallback callback) {
    for (RabbitTemplate template : templates) {
      template.setReturnCallback(callback);
    }
    return this;
  }

  public Publisher build() {
    return new Publisher(commonName, innerQueueSize, templates, retryDelayMs, useMDC, innerQueueShutdownMs);
  }
}
