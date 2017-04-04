package ru.hh.rabbitmq.spring.send;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;

public class SyncPublisher extends AbstractService {

  private final String name;
  private final MessageSender messageSender;

  SyncPublisher(String commonName, RabbitTemplate template, MessageSender messageSender) {
    ConnectionFactory connectionFactory = template.getConnectionFactory();
    String connectionFactoryName = connectionFactory.getHost() + ':' + connectionFactory.getPort();
    name = getClass().getSimpleName() + '{' + commonName + ',' + connectionFactoryName + '}';
    this.messageSender = messageSender;
  }

  @VisibleForTesting
  RabbitTemplate getTemplate() {
    return messageSender.getTemplate();
  }

  public void stopSync() {
    stopAsync();
    awaitTerminated();
  }

  public void stopSync(long timeout, TimeUnit timeUnit) throws TimeoutException {
    stopAsync();
    awaitTerminated(timeout, timeUnit);
  }

  @Override
  protected void doStart() {
    notifyStarted();
  }

  @Override
  protected void doStop() {
    ConnectionFactory connectionFactory = messageSender.getTemplate().getConnectionFactory();
    if (connectionFactory instanceof CachingConnectionFactory) {
      ((CachingConnectionFactory) connectionFactory).destroy();
    }
    notifyStopped();
  }

  /**
   * Blocking method, enqueues message or throws {@link AmqpException}
   * <p>
   * Wrap message with {@link CorrelatedMessage} to attach {@link CorrelationData} for publisher confirms.
   * </p>
   */
  public void send(Destination destination, Object message) throws AmqpException {
    checkStarted();
    messageSender.publishMessage(message, destination);
  }

  /**
   * Blocking method, enqueues message or throws {@link AmqpException}
   * <p>
   * Wrap message with {@link CorrelatedMessage} to attach {@link CorrelationData} for publisher confirms.
   * </p>
   */
  public void send(Object message) throws AmqpException {
    send(null, message);
  }

  @Override
  public String toString() {
    return name;
  }

  private void checkStarted() {
    if (!isRunning()) {
      throw new IllegalStateException("Publisher was not started for " + toString());
    }
  }

}
