package ru.hh.rabbitmq.spring.persistent.http;

import java.util.Collections;
import java.util.concurrent.Callable;
import ru.hh.jclient.common.HttpClientContextThreadLocalSupplier;

public class EmptyHttpContext {
  private final HttpClientContextThreadLocalSupplier httpClientContextSupplier;

  public EmptyHttpContext(HttpClientContextThreadLocalSupplier httpClientContextSupplier) {
    this.httpClientContextSupplier = httpClientContextSupplier;
  }

  public <T> T executeAsServerRequest(Callable<T> operation) throws Exception {
    httpClientContextSupplier.addContext(Collections.emptyMap(), Collections.emptyMap());
    try {
      return operation.call();
    } finally {
      httpClientContextSupplier.clear();
    }
  }
}
