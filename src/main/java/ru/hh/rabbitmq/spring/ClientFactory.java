package ru.hh.rabbitmq.spring;

import java.util.List;
import java.util.Properties;
import javax.annotation.Nullable;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import ru.hh.metrics.StatsDSender;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_HOSTS;
import static ru.hh.rabbitmq.spring.ConfigKeys.RECEIVER_HOSTS;
import ru.hh.rabbitmq.spring.send.PublisherBuilder;
import ru.hh.rabbitmq.spring.send.SyncPublisherBuilder;

/**
 * <p>
 * Create and configure {@link Receiver} and/or {@link PublisherBuilder}.
 * </p>
 * <p>
 * See {@link ConfigKeys} constants for configuration options.
 * </p>
 */
public class ClientFactory extends ConnectionsFactory {

  public ClientFactory(Properties properties, @Nullable String serviceName, @Nullable StatsDSender statsDSender, boolean sendStats) {
    super(properties, serviceName, statsDSender, sendStats);
  }

  public ClientFactory(Properties properties) {
    super(properties);
  }

  public Receiver createReceiver() {
    List<ConnectionFactory> factories = createConnectionFactories(RECEIVER_HOSTS);
    return new Receiver(factories, properties.getProperties(), serviceName, statsDSender);
  }

  public PublisherBuilder createPublisherBuilder() {
    List<ConnectionFactory> factories = createConnectionFactories(PUBLISHER_HOSTS);
    return new PublisherBuilder(factories, properties.getProperties(), serviceName, statsDSender);
  }

  public SyncPublisherBuilder createSyncPublisherBuilder() {
    List<ConnectionFactory> factories = createConnectionFactories(PUBLISHER_HOSTS);
    return new SyncPublisherBuilder(factories, properties.getProperties(), serviceName, statsDSender);
  }

}
