package ru.hh.rabbitmq;

import com.rabbitmq.client.Connection;
import java.io.IOException;

public interface ConnectionFactory {
  Connection getConnection() throws IOException;

  void returnConnection(Connection connection);

  void close();
}
