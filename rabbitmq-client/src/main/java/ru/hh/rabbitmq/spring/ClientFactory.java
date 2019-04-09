package ru.hh.rabbitmq.spring;

import java.util.List;
import java.util.Properties;
import javax.annotation.Nullable;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import ru.hh.nab.metrics.StatsDSender;
import static ru.hh.rabbitmq.spring.ConfigKeys.HOST;
import static ru.hh.rabbitmq.spring.ConfigKeys.HOSTS;
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

  @Nullable
  protected final StatsDSender statsDSender;
  @Nullable
  protected final String serviceName;

  public ClientFactory(Properties properties, @Nullable String serviceName, @Nullable StatsDSender statsDSender) {
    super(properties);
    this.serviceName = serviceName;
    this.statsDSender = statsDSender;
  }

  public ClientFactory(Properties properties, @Nullable String serviceName, @Nullable StatsDSender statsDSender, boolean sendStats) {
    this(properties, serviceName, sendStats ? statsDSender : null);
  }

  public ClientFactory(Properties properties) {
    this(properties, null, null);
  }

  public Receiver createReceiver() {
    List<ConnectionFactory> factories = createConnectionFactories(true, RECEIVER_HOSTS, HOSTS, HOST);
    return new Receiver(factories, properties.getProperties(), serviceName, statsDSender);
  }

  public PublisherBuilder createPublisherBuilder() {
    List<ConnectionFactory> factories = createConnectionFactories(true, PUBLISHER_HOSTS, HOSTS, HOST);
    return new PublisherBuilder(factories, properties.getProperties(), serviceName, statsDSender);
  }

  public SyncPublisherBuilder createSyncPublisherBuilder() {
    List<ConnectionFactory> factories = createConnectionFactories(true, PUBLISHER_HOSTS, HOSTS, HOST);
    return new SyncPublisherBuilder(factories, properties.getProperties(), serviceName, statsDSender);
  }

}
