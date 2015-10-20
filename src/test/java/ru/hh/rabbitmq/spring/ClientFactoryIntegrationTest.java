package ru.hh.rabbitmq.spring;

import java.util.Properties;

import org.junit.Test;


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

  private void publisher(Properties properties) {
    ClientFactory factory = new ClientFactory(properties);
    factory.createPublisherBuilder();
  }
}
