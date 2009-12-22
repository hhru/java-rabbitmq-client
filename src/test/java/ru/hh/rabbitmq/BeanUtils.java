package ru.hh.rabbitmq;
import java.lang.reflect.Field;

public class BeanUtils {
  public static void setField(Object bean, String fieldName, Object value) {
    boolean staticField = bean instanceof Class;
    if (!staticField) {
      setField(bean.getClass(), bean, fieldName, value);
      return;
    }
    try {
      Class clazz = (Class) bean;
      Field field = clazz.getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(null, value);
    } catch (NoSuchFieldException e) {
      throw new IllegalStateException(e);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  public static void setField(Class clazz, Object bean, String fieldName, Object value) {
    try {
      Field field = clazz.getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(bean, value);
    } catch (NoSuchFieldException e) {
      if (clazz.getSuperclass() == null || clazz.getSuperclass().getSuperclass() == null) {
        throw new IllegalStateException(e);
      }
      setField(clazz.getSuperclass(), bean, fieldName, value);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }
}
