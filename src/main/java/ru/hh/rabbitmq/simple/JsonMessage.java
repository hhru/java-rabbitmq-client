package ru.hh.rabbitmq.simple;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import java.io.IOException;
import java.util.Map;
import ru.hh.rabbitmq.util.ObjectMapperHolder;

public class JsonMessage extends Message {
  public JsonMessage(Map<String, Object> body, Map<String, Object> headers) throws JsonGenerationException, JsonMappingException, IOException {
    super(ObjectMapperHolder.get().writeValueAsBytes(body), headers);
  }

  public JsonMessage(Object body, Map<String, Object> headers) throws JsonGenerationException, JsonMappingException, IOException {
    super(ObjectMapperHolder.get().writeValueAsBytes(body), headers);
  }
}
