package ru.hh.rabbitmq.util;

import org.codehaus.jackson.map.AnnotationIntrospector;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.introspect.JacksonAnnotationIntrospector;
import org.codehaus.jackson.xc.JaxbAnnotationIntrospector;

public class ObjectMapperHolder {
  private static final ObjectMapperHolder INSTANCE = new ObjectMapperHolder();
  private ObjectMapper mapper;

  private ObjectMapperHolder() {
    this.mapper = new ObjectMapper();
    AnnotationIntrospector jacksonNative = new JacksonAnnotationIntrospector();
    AnnotationIntrospector jaxb = new JaxbAnnotationIntrospector();
    AnnotationIntrospector introspectors = new AnnotationIntrospector.Pair(jacksonNative, jaxb);
    mapper.getDeserializationConfig().setAnnotationIntrospector(introspectors);
    mapper.getSerializationConfig().setAnnotationIntrospector(introspectors);
  }

  public static ObjectMapper get() {
    return INSTANCE.mapper;
  }
}
