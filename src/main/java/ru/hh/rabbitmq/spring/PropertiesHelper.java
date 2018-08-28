package ru.hh.rabbitmq.spring;

import java.util.Properties;
import java.util.function.Function;

public class PropertiesHelper {

  private Properties properties;

  public PropertiesHelper(Properties properties) {
    this.properties = properties;
  }

  public Properties getProperties() {
    return properties;
  }

  public String getNotNullString(String name) {
    String value = properties.getProperty(name);
    if (value == null || value.trim().length() == 0) {
      throw new NullPointerException("Property '" + name + "' must be set and not empty");
    }
    return value;
  }

  public String getString(String name) {
    return properties.getProperty(name);
  }

  public String getString(String name, String defaultName) {
    String someName = getString(name);
    return someName == null ? defaultName : someName;
  }

  public Integer getInteger(String name) {
    return getInteger(name, null);
  }

  public Integer getInteger(String name, Integer defaultValue) {
    String value = getString(name);
    return safeParseNumber(value, Integer::valueOf, defaultValue);
  }

  public Long getLong(String name) {
    return getLong(name, null);
  }

  public Long getLong(String name, Long defaultValue) {
    String value = getString(name);
    return safeParseNumber(value, Long::valueOf, defaultValue);
  }

  public Boolean getBoolean(String name) {
    return getBoolean(name, null);
  }

  public Boolean getBoolean(String name, Boolean defaultValue) {
    String value = getString(name);
    return value == null ? defaultValue : Boolean.valueOf(value);
  }

  private static <T> T safeParseNumber(String value, Function<String, T> parser, T defaultValue) {
    if (value == null) {
      return defaultValue;
    }
    try {
      return parser.apply(value);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }
}
