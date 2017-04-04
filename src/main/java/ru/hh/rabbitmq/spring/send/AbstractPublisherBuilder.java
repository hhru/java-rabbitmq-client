package ru.hh.rabbitmq.spring.send;

import java.util.ArrayList;
import java.util.Collection;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import java.util.Properties;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ConfirmCallback;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ReturnCallback;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import ru.hh.rabbitmq.spring.ConfigException;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_EXCHANGE;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_MANDATORY;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_NAME;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_ROUTING_KEY;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_TRANSACTIONAL;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_USE_MDC;
import ru.hh.rabbitmq.spring.MDCMessagePropertiesConverter;
import ru.hh.rabbitmq.spring.PropertiesHelper;

public abstract class AbstractPublisherBuilder {

  protected final String commonName;
  protected final Collection<RabbitTemplate> templates;
  protected final boolean useMDC;

  AbstractPublisherBuilder(Collection<ConnectionFactory> connectionFactories, Properties properties) {
    PropertiesHelper props = new PropertiesHelper(properties);

    Boolean transactional = props.getBoolean(PUBLISHER_TRANSACTIONAL);
    if (transactional != null && transactional) {
      throw new ConfigException(
          PUBLISHER_TRANSACTIONAL + " is not allowed to be specified via properties. Use corresponding method on builder if available");
    }

    commonName = props.getString(PUBLISHER_NAME, "");

    String exchange = props.getString(PUBLISHER_EXCHANGE);
    String routingKey = props.getString(PUBLISHER_ROUTING_KEY);
    Boolean mandatory = props.getBoolean(PUBLISHER_MANDATORY);
    useMDC = props.getBoolean(PUBLISHER_USE_MDC, false);

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

      if (useMDC) {
        template.setMessagePropertiesConverter(new MDCMessagePropertiesConverter());
      }

      templates.add(template);
    }
    this.templates = unmodifiableList(templates);
  }

  /** @return Immutable collection of all rabbit templates for additional configuration */
  public Collection<RabbitTemplate> getRabbitTemplates() {
    return templates;
  }

  protected void withMessageConverterInternal(MessageConverter converter) {
    for (RabbitTemplate template : templates) {
      template.setMessageConverter(converter);
    }
  }

  protected void withJsonMessageConverterInternal() {
    Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter();
    withMessageConverterInternal(converter);
  }

  protected void withConfirmCallbackInternal(ConfirmCallback callback) {
    for (RabbitTemplate template : templates) {
      template.setConfirmCallback(callback);
    }
  }

  protected void withReturnCallbackInternal(ReturnCallback callback) {
    for (RabbitTemplate template : templates) {
      template.setReturnCallback(callback);
    }
  }
}
