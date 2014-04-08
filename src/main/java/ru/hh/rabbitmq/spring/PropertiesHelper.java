package ru.hh.rabbitmq.spring;

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
    String value = properties.getProperty(name);
    if (value == null || value.trim().length() == 0) {
      throw new NullPointerException("Property '" + name + "' must be set and not empty");
    }
    return value;
  }

  public String string(String name) {
    return properties.getProperty(name);
  }

  public String string(String name, String defaultName) {
    String someName = string(name);
    return someName == null ? defaultName : someName;
  }

  public Integer integer(String name) {
    String value = string(name);
    if (value == null) {
      return null;
    }
    return Ints.tryParse(value);
  }

  public int integer(String name, int defaultValue) {
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

  public boolean bool(String name, boolean defaultValue) {
    Boolean value = bool(name);
    return value == null ? defaultValue : value;
  }
}
