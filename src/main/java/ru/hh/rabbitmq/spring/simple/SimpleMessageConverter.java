package ru.hh.rabbitmq.spring.simple;

import java.io.IOException;
import java.util.Map;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;

import ru.hh.rabbitmq.spring.util.ObjectMapperHolder;

public class SimpleMessageConverter implements MessageConverter {
  
  private Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter();

  @Override
  public Message toMessage(Object object, MessageProperties messageProperties) throws MessageConversionException {
    if (object == null) {
      throw new MessageConversionException("Null object");
    }
    SimpleMessage jsonMessage = (SimpleMessage) object;
    Message message = converter.toMessage(jsonMessage.getBody(), messageProperties);
    // put to headers
    messageProperties.getHeaders().putAll(jsonMessage.getHeaders());
    messageProperties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
    return message;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object fromMessage(Message message) throws MessageConversionException {
    byte[] body = message.getBody();

    Map<String, Object> parsed;
    try {
      parsed = ObjectMapperHolder.get().readValue(body, 0, body.length, Map.class);
    } catch (IOException ex) {
      throw new MessageConversionException("Failed to convert body to map", ex);
    }
    return new SimpleMessage(message.getMessageProperties().getHeaders(), parsed);
  }
  
  

}
