package ru.hh.rabbitmq.spring;

import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.MDC;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;

import com.google.common.collect.Maps;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP.BasicProperties;

/**
 * <p>
 * Put everything from MDC context to message headers when serializing message.
 * </p>
 * <p>
 * Put all MDC-related headers from message to MDC context when deserializing message.
 * </p>
 * <p>
 * WARNING: when deserializing message, MDC context will always be CLEARED.
 * </p>
 */
public class MDCMessagePropertiesConverter extends DefaultMessagePropertiesConverter {

  private static final String MDC_PREFIX = "_MDC_";

  @Override
  public BasicProperties fromMessageProperties(MessageProperties source, String charset) {
    BasicProperties properties = super.fromMessageProperties(source, charset);
    Map<String, String> mdcContext = MDC.getCopyOfContextMap();
    if (mdcContext != null && !mdcContext.isEmpty()) {
      Map<String, Object> headers = Maps.newHashMap(properties.getHeaders());
      for (Entry<String, String> entry : mdcContext.entrySet()) {
        headers.put(MDC_PREFIX + entry.getKey(), entry.getValue());
      }
      properties = properties.builder().headers(headers).build();
    }
    return properties;
  }

  @Override
  public MessageProperties toMessageProperties(BasicProperties source, Envelope envelope, String charset) {
    // put to MDC before calling super so we have MDC filled id as soon as possible
    MDC.clear();
    if (source != null) {
      Map<String, Object> headers = source.getHeaders();
      if (headers != null) {
        for (Entry<String, Object> header : headers.entrySet()) {
          if (header.getKey().startsWith(MDC_PREFIX) && header.getValue() != null) {
            String key = header.getKey().substring(MDC_PREFIX.length());
            MDC.put(key, header.getValue().toString());
          }
        }
      }
    }
    return super.toMessageProperties(source, envelope, charset);
  }
}
