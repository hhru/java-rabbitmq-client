package ru.hh.rabbitmq;

import com.rabbitmq.client.Connection;

public interface ConnectionFactory {
  Connection getConnection();

  void returnConnection(Connection connection);

  void close();
}
