package ru.hh.rabbitmq.spring;

import java.util.Properties;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

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
    String someName = PropertiesHelper.this.getString(name);
    return someName == null ? defaultName : someName;
  }

  public Integer getInteger(String name) {
    String value = PropertiesHelper.this.getString(name);
    if (value == null) {
      return null;
    }
    return Ints.tryParse(value);
  }

  public int getInteger(String name, int defaultValue) {
    Integer value = PropertiesHelper.this.getInteger(name);
    return value == null ? defaultValue : value;
  }

  public Long getLong(String name) {
    String value = PropertiesHelper.this.getString(name);
    if (value == null) {
      return null;
    }
    return Longs.tryParse(value);
  }

  public long getLong(String name, long defaultValue) {
    Long value = PropertiesHelper.this.getLong(name);
    return value == null ? defaultValue : value;
  }

  public Boolean getBoolean(String name) {
    String value = PropertiesHelper.this.getString(name);
    if (value == null) {
      return null;
    }
    return Boolean.valueOf(value);
  }

  public boolean getBoolean(String name, boolean defaultValue) {
    Boolean value = PropertiesHelper.this.getBoolean(name);
    return value == null ? defaultValue : value;
  }
}
