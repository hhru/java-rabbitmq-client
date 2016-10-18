package ru.hh.rabbitmq.spring.send;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;
import com.google.common.util.concurrent.AbstractService;

public class SyncPublisher extends AbstractService {

  private final String name;
  private RabbitTemplate template;

  SyncPublisher(String commonName, RabbitTemplate template) {
    this.template = template;

    ConnectionFactory connectionFactory = template.getConnectionFactory();
    String connectionFactoryName = connectionFactory.getHost() + ':' + connectionFactory.getPort();
    name = getClass().getSimpleName() + '{' + commonName + ',' + connectionFactoryName + '}';
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
    ConnectionFactory connectionFactory = template.getConnectionFactory();
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
    ChannelWorker.publishMessage(message, destination, template);
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
