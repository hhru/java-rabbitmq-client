package ru.hh.rabbitmq.spring.persistent.http;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.Callable;
import ru.hh.jclient.common.HttpClientContextThreadLocalSupplier;
import ru.hh.jclient.common.util.storage.ThreadLocalStorage;

public class EmptyHttpContext {
  private final Optional<HttpClientContextThreadLocalSupplier> httpClientContextSupplier;

  public EmptyHttpContext(Optional<HttpClientContextThreadLocalSupplier> httpClientContextSupplier) {
    this.httpClientContextSupplier = httpClientContextSupplier;
  }

  public <T> T executeAsServerRequest(Callable<T> operation) throws Exception {
    httpClientContextSupplier.ifPresent(supplier -> supplier.addContext(Collections.emptyMap(), Collections.emptyMap()));
    try {
      return operation.call();
    } finally {
      httpClientContextSupplier.ifPresent(ThreadLocalStorage::clear);
    }
  }
}
