package ru.hh.rabbitmq.spring;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Properties;

import com.google.common.primitives.Ints;

public class PropertiesHelper {

  private Properties properties;

  public PropertiesHelper(Properties properties) {
    this.properties = properties;
  }

  public Properties getProperties() {
    return properties;
  }

  public String notNullString(String name) {
    return checkNotNull(properties.getProperty(name), "Property '" + name + "' must be set");
  }

  public String string(String name) {
    return properties.getProperty(name);
  }

  public Integer integer(String name) {
    String value = string(name);
    if (value == null) {
      return null;
    }
    return Ints.tryParse(value);
  }

  public Integer integer(String name, int defaultValue) {
    Integer value = integer(name);
    return value == null ? defaultValue : value;
  }

  public Boolean bool(String name) {
    String value = string(name);
    if (value == null) {
      return null;
    }
    return Boolean.valueOf(value);
  }
}
