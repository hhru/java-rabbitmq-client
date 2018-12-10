package ru.hh.rabbitmq.spring.send;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;

public class HhRabbitTemplate extends RabbitTemplate {

  public HhRabbitTemplate(ConnectionFactory connectionFactory) {
    super(connectionFactory);
  }

  @Override
  public MessagePropertiesConverter getMessagePropertiesConverter() {
    return super.getMessagePropertiesConverter();
  }
}
