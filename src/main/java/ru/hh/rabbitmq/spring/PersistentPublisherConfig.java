package ru.hh.rabbitmq.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import org.hibernate.SessionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.hh.hhinvoker.client.InvokerClient;
import ru.hh.metrics.StatsDSender;
import ru.hh.nab.common.properties.FileSettings;
import ru.hh.rabbitmq.spring.send.MessageSender;
import ru.hh.rabbitmq.spring.send.PersistentPublisher;
import ru.hh.rabbitmq.spring.simple.SimpleMessageConverter;

@Configuration
public class PersistentPublisherConfig {
  @Bean
  DatabaseQueueService databaseQueueService(String serviceName, ConnectionFactory connectionFactory, SessionFactory sessionFactory,
      ObjectMapper objectMapper, StatsDSender statsDSender) {
    RabbitTemplate template = new RabbitTemplate(connectionFactory);
    template.setMessageConverter(new SimpleMessageConverter());
    MessageSender messageSender = new MessageSender(template, serviceName, statsDSender);
    return new DatabaseQueueService(serviceName, messageSender, sessionFactory, objectMapper);
  }

  @Bean
  PersistentPublisherResource persistentPublisherResource(InvokerClient invokerClient, DatabaseQueueService databaseQueueService) {
    return new PersistentPublisherResource(invokerClient, databaseQueueService);
  }

  @Bean
  PersistentPublisher persistentPublisher(DatabaseQueueService databaseQueueService, String serviceName, FileSettings fileSettings) {
    FileSettings subSettings = fileSettings.getSubSettings("persistent.publisher.mq");
    return new PersistentPublisher(databaseQueueService, serviceName, fileSettings.getString("upstreamName"),
      Duration.ofSeconds(fileSettings.getLong("retryEventSec")), Duration.ofSeconds(fileSettings.getLong("pollingIntervalSec")));
  }
}
