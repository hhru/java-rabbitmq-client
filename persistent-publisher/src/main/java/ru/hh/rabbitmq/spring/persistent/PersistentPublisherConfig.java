package ru.hh.rabbitmq.spring.persistent;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.hibernate.SessionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.hh.hhinvoker.client.InvokerClient;
import ru.hh.metrics.StatsDSender;
import ru.hh.nab.common.properties.FileSettings;
import ru.hh.rabbitmq.spring.ClientFactory;
import ru.hh.rabbitmq.spring.PropertiesHelper;
import ru.hh.rabbitmq.spring.send.MessageSender;
import ru.hh.rabbitmq.spring.persistent.send.PersistentPublisher;
import static ru.hh.rabbitmq.spring.ConfigKeys.HOST;
import static ru.hh.rabbitmq.spring.ConfigKeys.HOSTS;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_HOSTS;

@Configuration
public class PersistentPublisherConfig {
  @Bean
  DatabaseQueueService databaseQueueService(String serviceName, FileSettings fileSettings, SessionFactory sessionFactory,
      ObjectMapper persistentPublisherObjectMapper, StatsDSender statsDSender) {
    Properties subSettings = fileSettings.getSubProperties("persistent.publisher.mq");
    List<ConnectionFactory> connectionFactories
      = ClientFactory.createConnectionFactories(new PropertiesHelper(subSettings), true, PUBLISHER_HOSTS, HOSTS, HOST);
    if (connectionFactories.size() != 1) {
      throw new BeanCreationException("Only one " + ConnectionFactory.class + " allowed for " + DatabaseQueueService.class);
    }
    RabbitTemplate template = new RabbitTemplate(connectionFactories.iterator().next());
    template.setMessageConverter(new Jackson2JsonMessageConverter(persistentPublisherObjectMapper));
    MessageSender messageSender = new MessageSender(template, serviceName, statsDSender);
    return new DatabaseQueueService(serviceName, messageSender, sessionFactory, persistentPublisherObjectMapper);
  }

  @Bean
  PersistentPublisherResource persistentPublisherResource(InvokerClient invokerClient, DatabaseQueueService databaseQueueService) {
    return new PersistentPublisherResource(invokerClient, databaseQueueService);
  }

  @Bean
  PersistentPublisher persistentPublisher(DatabaseQueueService databaseQueueService, String serviceName, FileSettings fileSettings) {
    FileSettings subSettings = fileSettings.getSubSettings("persistent.publisher.mq");
    return new PersistentPublisher(databaseQueueService, serviceName, subSettings.getString("upstreamName"),
      subSettings.getString("jerseyBasePath"), Duration.ofSeconds(subSettings.getLong("retryEventSec")),
      Duration.ofSeconds(subSettings.getLong("pollingIntervalSec")));
  }

  @Bean
  ObjectMapper persistentPublisherObjectMapper() {
    return new ObjectMapper();
  }
}
