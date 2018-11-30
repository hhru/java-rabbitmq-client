package ru.hh.rabbitmq.spring.persistent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

public class JacksonMessageConverter implements MessageConverter {

  public static final String JACKSON_CONVERTER_KEY = "JacksonMessageConverter";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final JacksonMessageConverter INSTANCE = new JacksonMessageConverter();

  private JacksonMessageConverter() {
  }

  @Override
  public String getKey() {
    return JACKSON_CONVERTER_KEY;
  }

  @Override
  public String convertToDb(Object message) {
    try {
      return OBJECT_MAPPER.writeValueAsString(message);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <T> T convertFromDb(String messageData, Class<T> clazz) {
    try {
      return OBJECT_MAPPER.readValue(messageData, clazz);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
