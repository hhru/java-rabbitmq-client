package ru.hh.rabbitmq.spring.send;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.Lifecycle;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.hh.hhinvoker.client.InvokerClient;
import ru.hh.metrics.StatsDSender;
import ru.hh.rabbitmq.spring.PersistentPublisherResource;
import ru.hh.rabbitmq.spring.PgqService;
import ru.hh.rabbitmq.spring.simple.SimpleMessage;
import ru.hh.rabbitmq.spring.simple.SimpleMessageConverter;

public class PersistentPublisher implements Lifecycle {

  private final ConfigurableBeanFactory beanFactory;
  private final String pgqQueueName;
  private final String upstreamName;
  private final MessageSender messageSender;
  private final ObjectMapper objectMapper;
  private final JdbcTemplate jdbcTemplate;
  private final Duration pollingInterval;
  private final Duration retryEventDelay;
  private PgqService pgqService;

  public PersistentPublisher(ConfigurableBeanFactory beanFactory, String serviceName, String upstreamName, ConnectionFactory connectionFactory,
      ObjectMapper objectMapper, JdbcTemplate jdbcTemplate, Duration retryEventDelay, Duration pollingInterval,
      @Nullable StatsDSender statsDSender) {
    this.beanFactory = beanFactory;
    pgqQueueName = serviceName;
    this.upstreamName = upstreamName;
    RabbitTemplate template = new RabbitTemplate(connectionFactory);
    template.setMessageConverter(new SimpleMessageConverter());
    messageSender = new MessageSender(template, serviceName, statsDSender);
    this.objectMapper = objectMapper;
    this.jdbcTemplate = jdbcTemplate;
    this.retryEventDelay = retryEventDelay;
    this.pollingInterval = pollingInterval;
  }

  public void send(SimpleMessage simpleMessage, Destination destination) throws JsonProcessingException {
    pgqService.publish(simpleMessage, destination);
  }

  @PostConstruct
  private void afterPropertiesSet() throws Exception {
    if (!beanFactory.containsSingleton("pgqService")) {
      beanFactory.registerSingleton("pgqService", new PgqService(pgqQueueName, messageSender, objectMapper, jdbcTemplate));
    }
    pgqService = beanFactory.getBean(PgqService.class);
    if (!beanFactory.containsSingleton("publicationResource")) {
      InvokerClient invokerClient = beanFactory.getBean(InvokerClient.class);
      beanFactory.registerSingleton("publicationResource", new PersistentPublisherResource(invokerClient, pgqService));
    }
  }

  @Override
  public void start() {
    pgqService.registerConsumerIfPossible();
    pgqService.registerHhInvokerJob(pgqQueueName, upstreamName, pollingInterval, retryEventDelay);
  }

  @Override
  public void stop() { }

  @Override
  public boolean isRunning() {
    return !pgqService.registerConsumerIfPossible();
  }
}
