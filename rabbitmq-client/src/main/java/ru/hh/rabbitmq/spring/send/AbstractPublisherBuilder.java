package ru.hh.rabbitmq.spring.send;

import java.util.Collection;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ConfirmCallback;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ReturnCallback;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import ru.hh.rabbitmq.spring.ConfigException;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_NAME;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_TRANSACTIONAL;
import ru.hh.rabbitmq.spring.PropertiesHelper;

public abstract class AbstractPublisherBuilder {

  protected final String commonName;
  protected final Collection<RabbitTemplate> templates;

  protected AbstractPublisherBuilder(Collection<ConnectionFactory> connectionFactories, Properties properties) {
    PropertiesHelper props = new PropertiesHelper(properties);

    Boolean transactional = props.getBoolean(PUBLISHER_TRANSACTIONAL);
    if (transactional != null && transactional) {
      throw new ConfigException(
          PUBLISHER_TRANSACTIONAL + " is not allowed to be specified via properties. Use corresponding method on builder if available");
    }

    commonName = props.getString(PUBLISHER_NAME, "");
    RabbitTemplateFactory templateFactory = new RabbitTemplateFactory(properties);
    List<RabbitTemplate> templates = connectionFactories.stream().map(templateFactory::createTemplate).collect(Collectors.toList());
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
