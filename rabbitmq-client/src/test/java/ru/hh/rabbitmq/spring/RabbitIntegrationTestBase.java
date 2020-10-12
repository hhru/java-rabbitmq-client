package ru.hh.rabbitmq.spring;

import com.rabbitmq.client.AMQP;
import static com.rabbitmq.client.ConnectionFactory.DEFAULT_PASS;
import static com.rabbitmq.client.ConnectionFactory.DEFAULT_USER;
import java.util.Properties;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static ru.hh.rabbitmq.spring.ConfigKeys.HOSTS_PORT_SEPARATOR;
import static ru.hh.rabbitmq.spring.ConfigKeys.HOSTS_SEPARATOR;
@Testcontainers
public class RabbitIntegrationTestBase {

  public static String HOST1 = "localhost";
  public static int PORT1 = AMQP.PROTOCOL.PORT;

  public static String HOST2 = "dev";
  public static int PORT2 = AMQP.PROTOCOL.PORT;

  public static final String USERNAME = DEFAULT_USER;
  public static final String PASSWORD = DEFAULT_PASS;

  public static final String ROUTING_KEY1 = "routing_key1";
  public static final String ROUTING_KEY2 = "routing_key2";
  public static final String QUEUE1 = "hh-rabbit-client-spring-queue-1";
  public static final String QUEUE2 = "hh-rabbit-client-spring-queue-2";
  public static final String EXCHANGE = "hh-rabbit-client-spring-exchange";
  @Container
  public GenericContainer<?> rabbit1 = new GenericContainer<>("rabbitmq:3-management").withExposedPorts(5672);
  @Container
  public GenericContainer<?> rabbit2 = new GenericContainer<>("rabbitmq:3-management").withExposedPorts(5672);

  @BeforeEach
  public void beforeClass() {

    String host1 = rabbit1.getHost();
    String port1 = String.valueOf(rabbit1.getFirstMappedPort());
    String host2 = rabbit2.getHost();
    String port2 = String.valueOf(rabbit2.getFirstMappedPort());

    if (host1 != null) {
      HOST1 = host1;
    }
    if (port1 != null) {
      PORT1 = Integer.parseInt(port1);
    }
    if (host2 != null) {
      HOST2 = host2;
    }
    if (port2 != null) {
      PORT2 = Integer.parseInt(port2);
    }
    setUp(getConnectionFactory(HOST1, PORT1));
    setUp(getConnectionFactory(HOST2, PORT2));
  }

  @AfterEach
  public void afterClass() {
    tearDown(getConnectionFactory(HOST1, PORT1));
    tearDown(getConnectionFactory(HOST2, PORT2));
  }

  @BeforeEach
  public void purgeQueues() {
    purgeQueue(HOST1, PORT1);
    purgeQueue(HOST2, PORT2);
  }

  private void purgeQueue(String host, int port) {
    CachingConnectionFactory connectionFactory = getConnectionFactory(host, port);
    RabbitAdmin admin = new RabbitAdmin(connectionFactory);
    admin.afterPropertiesSet();
    if (admin.getQueueProperties(QUEUE1) != null) {
      admin.purgeQueue(QUEUE1, false);
    }
    if (admin.getQueueProperties(QUEUE2) != null) {
      admin.purgeQueue(QUEUE2, false);
    }
    connectionFactory.destroy();
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

  private static CachingConnectionFactory getConnectionFactory(String host, int port) {
    CachingConnectionFactory factory = new CachingConnectionFactory();
    factory.setHost(host);
    factory.setPort(port);
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
    properties.setProperty(ConfigKeys.HOSTS, HOST1 + HOSTS_PORT_SEPARATOR + PORT1 + HOSTS_SEPARATOR + HOST2 + HOSTS_PORT_SEPARATOR + PORT2);
    return properties;
  }

  protected static Properties properties(String host, int port) {
    Properties properties = baseProperties();
    properties.setProperty(ConfigKeys.HOSTS, host + HOSTS_PORT_SEPARATOR + port);
    properties.setProperty(ConfigKeys.PORT, String.valueOf(port));
    return properties;
  }

  protected static void appendDirections(Properties properties) {
    properties.setProperty(ConfigKeys.RECEIVER_QUEUES, QUEUE1);
    properties.setProperty(ConfigKeys.PUBLISHER_EXCHANGE, EXCHANGE);
    properties.setProperty(ConfigKeys.PUBLISHER_ROUTING_KEY, ROUTING_KEY1);
  }

  protected static Receiver receiverAllHosts(boolean withDirections) {
    Properties properties = propertiesAllHosts();
    if (withDirections) {
      appendDirections(properties);
    }
    ClientFactory factory = new ClientFactory(properties);
    return factory.createReceiver();
  }

  protected static Receiver receiverMDC() {
    Properties properties = propertiesAllHosts();
    appendDirections(properties);
    properties.setProperty(ConfigKeys.RECEIVER_USE_MDC, "true");
    ClientFactory factory = new ClientFactory(properties);
    return factory.createReceiver();
  }
}
