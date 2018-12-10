package ru.hh.rabbitmq.spring;

import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;

import static org.junit.Assert.assertEquals;
import static ru.hh.rabbitmq.spring.ConfigKeys.HOSTS;


public class ClientFactoryIntegrationTest {

  @Test(expected = ConfigException.class)
  public void testEmptyProperties() {
    publisher(new Properties());
  }

  @Test(expected = ConfigException.class)
  public void testEmptyHost() {
    Properties properties = new Properties();
    properties.setProperty(ConfigKeys.HOST, "");
    publisher(properties);
  }

  @Test(expected = ConfigException.class)
  public void testEmptyHosts() {
    Properties properties = new Properties();
    properties.setProperty(ConfigKeys.HOSTS, "");
    publisher(properties);
  }

  @Test(expected = ConfigException.class)
  public void testNoUsername() {
    Properties properties = new Properties();
    properties.setProperty(ConfigKeys.HOSTS, "localhost");
    publisher(properties);
  }

  @Test(expected = ConfigException.class)
  public void testNoPassword() {
    Properties properties = new Properties();
    properties.setProperty(ConfigKeys.HOSTS, "localhost");
    properties.setProperty(ConfigKeys.USERNAME, "guest");
    publisher(properties);
  }

  @Test
  public void testMinimal() {
    Properties properties = new Properties();
    properties.setProperty(ConfigKeys.HOSTS, "localhost");
    properties.setProperty(ConfigKeys.USERNAME, "guest");
    properties.setProperty(ConfigKeys.PASSWORD, "guest");
    publisher(properties);

    properties.remove(ConfigKeys.HOSTS);
    properties.setProperty(ConfigKeys.HOST, "localhost");
    publisher(properties);
  }

  @Test
  public void testParseHostNoPort() {
    Properties properties = new Properties();
    String testHost = "localhost";
    properties.setProperty(ConfigKeys.HOSTS, testHost);
    properties.setProperty(ConfigKeys.USERNAME, "guest");
    properties.setProperty(ConfigKeys.PASSWORD, "guest");
    ClientFactory clientFactory = new ClientFactory(properties);
    List<ConnectionFactory> connectionFactories = clientFactory.createConnectionFactories(true, HOSTS);
    assertEquals(1, connectionFactories.size());
    assertEquals(testHost, connectionFactories.get(0).getHost());
  }
  @Test
  public void testParseHost() {
    Properties properties = new Properties();
    String testHost = "localhost";
    int testPort = 123;
    properties.setProperty(ConfigKeys.HOSTS, testHost);
    properties.setProperty(ConfigKeys.PORT, String.valueOf(testPort));
    properties.setProperty(ConfigKeys.USERNAME, "guest");
    properties.setProperty(ConfigKeys.PASSWORD, "guest");
    ClientFactory clientFactory = new ClientFactory(properties);
    List<ConnectionFactory> connectionFactories = clientFactory.createConnectionFactories(true, HOSTS);
    assertEquals(1, connectionFactories.size());
    assertEquals(testHost, connectionFactories.get(0).getHost());
    assertEquals(testPort, connectionFactories.get(0).getPort());
  }

  private void publisher(Properties properties) {
    ClientFactory factory = new ClientFactory(properties);
    factory.createPublisherBuilder();
  }
}
