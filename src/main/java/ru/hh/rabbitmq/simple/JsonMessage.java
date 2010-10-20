package ru.hh.rabbitmq.simple;

import java.io.IOException;
import java.util.Map;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import ru.hh.rabbitmq.util.ObjectMapperHolder;

public class JsonMessage extends Message {
  public JsonMessage(Map<String, Object> body, Map<String, Object> headers) throws JsonGenerationException, JsonMappingException,
    IOException {
    super(ObjectMapperHolder.get().writeValueAsBytes(body), headers);
  }

  public JsonMessage(Object body, Map<String, Object> headers) throws JsonGenerationException, JsonMappingException, IOException {
    super(ObjectMapperHolder.get().writeValueAsBytes(body), headers);
  }
}
