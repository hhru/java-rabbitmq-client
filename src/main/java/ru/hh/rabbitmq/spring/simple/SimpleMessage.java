package ru.hh.rabbitmq.spring.simple;

import java.util.Map;

public class SimpleMessage {

  private Map<String, Object> headers;
  private Map<String, Object> body;

  public SimpleMessage(Map<String, Object> headers, Map<String, Object> body) {
    this.headers = headers;
    this.body = body;
  }

  public Map<String, Object> getHeaders() {
    return headers;
  }

  public Map<String, Object> getBody() {
    return body;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((body == null) ? 0 : body.hashCode());
    result = prime * result + ((headers == null) ? 0 : headers.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    SimpleMessage other = (SimpleMessage) obj;
    if (body == null) {
      if (other.body != null)
        return false;
    }
    else if (!body.equals(other.body))
      return false;
    if (headers == null) {
      if (other.headers != null)
        return false;
    }
    else if (!headers.equals(other.headers))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "SimpleMessage{" +
      "headers=" + headers +
      ", body=" + body +
      '}';
  }
}
