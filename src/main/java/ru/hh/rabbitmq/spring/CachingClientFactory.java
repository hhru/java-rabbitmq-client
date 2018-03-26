package ru.hh.rabbitmq.spring;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.annotation.Nullable;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import ru.hh.metrics.StatsDSender;
import ru.hh.rabbitmq.spring.send.PublisherBuilder;
import ru.hh.rabbitmq.spring.send.SyncPublisherBuilder;
import static ru.hh.rabbitmq.spring.ConfigKeys.HOST;
import static ru.hh.rabbitmq.spring.ConfigKeys.HOSTS;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_HOSTS;
import static ru.hh.rabbitmq.spring.ConfigKeys.RECEIVER_HOSTS;

/**
 * <p>
 * Create and configure {@link Receiver} and/or {@link PublisherBuilder}, reusing connection factories.
 * </p>
 * <p>
 * See {@link ConfigKeys} constants for configuration options.
 * </p>
 */
public class CachingClientFactory extends ConnectionsFactory {

  private final List<ConnectionFactory> receiverFactories;
  private final List<ConnectionFactory> publisherFactories;

  public CachingClientFactory(Properties properties, @Nullable String serviceName, @Nullable StatsDSender statsDSender, boolean sendStats) {
    super(properties, serviceName, statsDSender, sendStats);
    List<ConnectionFactory> generic = createConnectionFactories(false, HOSTS, HOST);

    receiverFactories = new ArrayList<>(generic);
    receiverFactories.addAll(createConnectionFactories(false, RECEIVER_HOSTS));

    publisherFactories = new ArrayList<>(generic);
    publisherFactories.addAll(createConnectionFactories(false, PUBLISHER_HOSTS));
  }

  public Receiver createReceiver(Properties properties) {
    return new Receiver(receiverFactories, properties, serviceName, statsDSender);
  }

  public PublisherBuilder createPublisherBuilder(Properties properties) {
    return new PublisherBuilder(publisherFactories, properties, serviceName, statsDSender);
  }

  public SyncPublisherBuilder createSyncPublisherBuilder(Properties properties) {
    return new SyncPublisherBuilder(publisherFactories, properties, serviceName, statsDSender);
  }
}
