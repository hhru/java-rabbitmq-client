package ru.hh.rabbitmq.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;

/**
 * copypaste from com.headhunter.core.utils.ExceptionUtils
 * */
public class ExceptionUtils {
  private ExceptionUtils() { }

  public static RuntimeException unchecked(Throwable t) {
    if (t instanceof RuntimeException) {
      return (RuntimeException) t;
    } else {
      return new RuntimeException(t);
    }
  }
}
