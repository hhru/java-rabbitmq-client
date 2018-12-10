package ru.hh.rabbitmq.spring.util;

import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;

public class ObjectMapperHolder {

  private static final ObjectMapperHolder INSTANCE = new ObjectMapperHolder();
  private ObjectMapper mapper;

  private ObjectMapperHolder() {
    this.mapper = new ObjectMapper();
    AnnotationIntrospector jacksonNative = new JacksonAnnotationIntrospector();
    AnnotationIntrospector jaxb = new JaxbAnnotationIntrospector(TypeFactory.defaultInstance());
    AnnotationIntrospector introspectors = new AnnotationIntrospectorPair(jacksonNative, jaxb);
    mapper.setAnnotationIntrospector(introspectors);
  }

  public static ObjectMapper get() {
    return INSTANCE.mapper;
  }
}
