package ru.hh.rabbitmq.spring.util;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static java.lang.Thread.currentThread;

public final class ConnectionFactoryTester {

  private static final Logger log = LoggerFactory.getLogger(ConnectionFactoryTester.class);

  public static void test(final ConnectionFactory connectionFactory)
          throws AmqpException, IOException, TimeoutException {

    final Connection connection = connectionFactory.createConnection();
    try {
      final Channel channel = connection.createChannel(false);
      channel.close();
    } finally {
      connection.close();
    }
  }

  public static void testUntilSuccess(final ConnectionFactory connectionFactory, final int retryDelayMs)
          throws InterruptedException {

    while(!currentThread().isInterrupted()) {
      try {
        test(connectionFactory);
        return;

      } catch (AmqpException | IOException | TimeoutException e) {
        log.warn("test of {}:{} failed: {}, sleeping {} ms until next test",
                connectionFactory.getHost(), connectionFactory.getPort(), e.toString(), retryDelayMs, e);

      } catch (RuntimeException e) {
        log.error("test of {}:{} failed: {}, sleeping {} ms until next test",
                connectionFactory.getHost(), connectionFactory.getPort(), e.toString(), retryDelayMs, e);
      }

      Thread.sleep(retryDelayMs);
    }
  }

  private ConnectionFactoryTester() {
  }
}
