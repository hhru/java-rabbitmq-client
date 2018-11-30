package ru.hh.rabbitmq.spring.send;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Properties;
import javax.annotation.Nullable;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ConfirmCallback;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ReturnCallback;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.util.ReflectionUtils;
import ru.hh.metrics.StatsDSender;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_INNER_QUEUE_SHUTDOWN_MS;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_INNER_QUEUE_SIZE;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_RETRY_DELAY_MS;
import ru.hh.rabbitmq.spring.MDCMessagePropertiesConverter;
import ru.hh.rabbitmq.spring.PropertiesHelper;

public class PublisherBuilder extends AbstractPublisherBuilder {

  private final int innerQueueSize;
  private final int innerQueueShutdownMs;
  private final int retryDelayMs;
  private final boolean useMDC;
  @Nullable
  private final StatsDSender statsDSender;
  @Nullable
  private final String serviceName;

  public PublisherBuilder(Collection<ConnectionFactory> connectionFactories,
                          Properties properties,
                          @Nullable
                          String serviceName,
                          @Nullable
                          StatsDSender statsDSender) {
    super(connectionFactories, properties);
    this.serviceName = serviceName;
    this.statsDSender = statsDSender;
    PropertiesHelper props = new PropertiesHelper(properties);
    innerQueueSize = props.getInteger(PUBLISHER_INNER_QUEUE_SIZE, 1000);
    innerQueueShutdownMs = props.getInteger(PUBLISHER_INNER_QUEUE_SHUTDOWN_MS, 3000);
    retryDelayMs = props.getInteger(PUBLISHER_RETRY_DELAY_MS, 2000);
    useMDC = checkIsUsingMdc(templates);
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
    return new Publisher(commonName, innerQueueSize, templates, retryDelayMs, useMDC, innerQueueShutdownMs, serviceName, statsDSender);
  }

  //TODO make a better way to check if useMDC, but respecting template creation procedure encapsulation
  private static boolean checkIsUsingMdc(Collection<RabbitTemplate> templates) {
    RabbitTemplate template = templates.iterator().next();
    Field messagePropertiesConverterField = ReflectionUtils.findField(RabbitTemplate.class, "messagePropertiesConverter");
    if (messagePropertiesConverterField == null) {
      return false;
    }
    return ReflectionUtils.getField(messagePropertiesConverterField, template) instanceof MDCMessagePropertiesConverter;
  }
}
