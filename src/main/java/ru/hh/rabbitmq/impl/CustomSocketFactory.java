package ru.hh.rabbitmq.impl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import javax.net.SocketFactory;

public class CustomSocketFactory extends SocketFactory {
  private SocketFactory socketFactory = SocketFactory.getDefault();
  private Integer soTimeout;

  public void setSocketFactory(SocketFactory socketFactory) {
    this.socketFactory = socketFactory;
  }

  public void setSoTimeout(Integer soTimeout) {
    this.soTimeout = soTimeout;
  }

  @Override
  public Socket createSocket(String host, int port) throws IOException, UnknownHostException {
    Socket socket = socketFactory.createSocket(host, port);
    applyCustomParameters(socket);
    return socket;
  }

  @Override
  public Socket createSocket(InetAddress host, int port) throws IOException {
    Socket socket = socketFactory.createSocket(host, port);
    applyCustomParameters(socket);
    return socket;
  }

  @Override
  public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException,
    UnknownHostException {
    Socket socket = socketFactory.createSocket(host, port, localHost, localPort);
    applyCustomParameters(socket);
    return socket;
  }

  @Override
  public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
    Socket socket = socketFactory.createSocket(address, port, localAddress, localPort);
    applyCustomParameters(socket);
    return socket;
  }

  private void applyCustomParameters(Socket socket) throws SocketException {
    if (soTimeout != null) {
      socket.setSoTimeout(soTimeout);
    }
  }
}
