package ru.hh.rabbitmq.spring;

import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;


public class RabbitIntegrationTestBase {

  public static final String HOST1 = "localhost";
  public static final String HOST2 = "dev";
  public static final String[] HOSTS = new String[] { HOST1, HOST2 };
  public static final String USERNAME = "guest";
  public static final String PASSWORD = "guest";

  public static final String ROUTING_KEY1 = "routingkey1";
  public static final String ROUTING_KEY2 = "routingkey2";
  public static final String QUEUE1 = "hh-rabbit-client-spring-queue-1";
  public static final String QUEUE2 = "hh-rabbit-client-spring-queue-2";
  public static final String EXCHANGE = "hh-rabbit-client-spring-exchange";

  @BeforeClass
  public static void beforeClass() {

    for (String host : HOSTS) {
      setUp(getConnectionFactory(host));
    }
  }

  @AfterClass
  public static void afterClass() {
    for (String host : HOSTS) {
      tearDown(getConnectionFactory(host));
    }
  }

  private static void setUp(CachingConnectionFactory connectionFactory) {
    RabbitAdmin admin = new RabbitAdmin(connectionFactory);
    admin.afterPropertiesSet();
    admin.declareExchange(getExchange());
    // q1
    admin.declareQueue(getQueue(QUEUE1));
    admin.declareBinding(getBinding(QUEUE1, ROUTING_KEY1));
    // q2
    admin.declareQueue(getQueue(QUEUE2));
    admin.declareBinding(getBinding(QUEUE2, ROUTING_KEY2));

    connectionFactory.destroy();
  }

  private static void tearDown(CachingConnectionFactory connectionFactory) {
    RabbitAdmin admin = new RabbitAdmin(connectionFactory);
    admin.afterPropertiesSet();
    // q1
    admin.removeBinding(getBinding(QUEUE1, ROUTING_KEY1));
    admin.deleteQueue(getQueue(QUEUE1).getName());
    // q2
    admin.removeBinding(getBinding(QUEUE2, ROUTING_KEY2));
    admin.deleteQueue(getQueue(QUEUE2).getName());

    admin.deleteExchange(getExchange().getName());
    connectionFactory.destroy();
  }

  private static CachingConnectionFactory getConnectionFactory(String host) {
    CachingConnectionFactory factory = new CachingConnectionFactory();
    factory.setHost(host);
    factory.setUsername(USERNAME);
    factory.setPassword(PASSWORD);
    return factory;
  }

  private static DirectExchange getExchange() {
    return new DirectExchange(EXCHANGE);
  }

  private static Queue getQueue(String queue) {
    return new Queue(queue);
  }

  private static Binding getBinding(String queue, String routingKey) {
    return BindingBuilder.bind(getQueue(queue)).to(getExchange()).with(routingKey);
  }

  protected static Properties baseProperties() {
    Properties properties = new Properties();
    properties.setProperty(ConfigKeys.USERNAME, USERNAME);
    properties.setProperty(ConfigKeys.PASSWORD, PASSWORD);
    return properties;
  }

  protected static Properties propertiesAllHosts() {
    Properties properties = baseProperties();
    properties.setProperty(ConfigKeys.HOSTS, HOST1 + ConfigKeys.HOSTS_SEPARATOR + HOST2);
    return properties;
  }

  protected static Properties properties(String host) {
    Properties properties = baseProperties();
    properties.setProperty(ConfigKeys.HOSTS, host);
    return properties;
  }

  protected static Properties appendDirections(Properties properties) {
    properties.setProperty(ConfigKeys.RECEIVER_QUEUES, QUEUE1);
    properties.setProperty(ConfigKeys.PUBLISHER_EXCHANGE, EXCHANGE);
    properties.setProperty(ConfigKeys.PUBLISHER_ROUTING_KEY, ROUTING_KEY1);
    return properties;
  }

  protected static Publisher publisher(String host, boolean withDirections) {
    Properties properties = properties(host);
    if (withDirections) {
      appendDirections(properties);
    }
    ClientFactory factory = new ClientFactory(properties);
    return factory.createPublisher();
  }

  protected static Receiver receiverAllHosts(boolean withDirections) {
    Properties properties = propertiesAllHosts();
    if (withDirections) {
      appendDirections(properties);
    }
    ClientFactory factory = new ClientFactory(properties);
    return factory.createReceiver();
  }
}
