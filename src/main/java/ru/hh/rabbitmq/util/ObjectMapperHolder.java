package ru.hh.rabbitmq.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;

public class ObjectMapperHolder {
  private static final ObjectMapperHolder INSTANCE = new ObjectMapperHolder();
  private final ObjectMapper mapper;

  private ObjectMapperHolder() {
    this.mapper = new ObjectMapper();
    mapper.setAnnotationIntrospector(
      new AnnotationIntrospectorPair(new JacksonAnnotationIntrospector(), new JaxbAnnotationIntrospector(mapper.getTypeFactory())));
  }

  public static ObjectMapper get() {
    return INSTANCE.mapper;
  }
}
