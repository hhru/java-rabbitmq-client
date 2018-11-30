package ru.hh.rabbitmq.spring.send;

import java.util.Properties;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import ru.hh.rabbitmq.spring.MDCMessagePropertiesConverter;
import ru.hh.rabbitmq.spring.PropertiesHelper;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_EXCHANGE;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_MANDATORY;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_ROUTING_KEY;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_USE_MDC;

public class RabbitTemplateFactory {
  private Destination destination;
  private Boolean mandatory;
  private Boolean useMDC;

  public RabbitTemplateFactory(Properties properties) {
    PropertiesHelper props = new PropertiesHelper(properties);
    destination = createDestination(props);
    mandatory = props.getBoolean(PUBLISHER_MANDATORY);
    useMDC = props.getBoolean(PUBLISHER_USE_MDC, false);
  }

  public RabbitTemplate createTemplate(ConnectionFactory factory) {
    RabbitTemplate template = new RabbitTemplate(factory);

    if (destination.getExchange() != null) {
      template.setExchange(destination.getExchange());
    }

    if (destination.getRoutingKey() != null) {
      template.setRoutingKey(destination.getRoutingKey());
    }

    if (mandatory != null) {
      template.setMandatory(mandatory);
    }

    if (Boolean.TRUE.equals(useMDC)) {
      template.setMessagePropertiesConverter(new MDCMessagePropertiesConverter());
    }
    return template;
  }


  private static Destination createDestination(PropertiesHelper props) {
    String exchange = props.getString(PUBLISHER_EXCHANGE);
    String routingKey = props.getString(PUBLISHER_ROUTING_KEY);
    return new Destination(exchange, routingKey);
  }
}
