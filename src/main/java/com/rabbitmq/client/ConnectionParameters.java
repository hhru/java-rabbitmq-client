package com.rabbitmq.client;

import java.security.AccessControlException;

public class ConnectionParameters {
  public static final String DEFAULT_USER = "guest";
  public static final String DEFAULT_PASS = "guest";
  public static final String DEFAULT_VHOST = "/";
  public static final int DEFAULT_CHANNEL_MAX = 0;
  public static final int DEFAULT_FRAME_MAX = 0;
  public static final int DEFAULT_HEARTBEAT = 0;

  private String _userName = DEFAULT_USER;
  private String _password = DEFAULT_PASS;
  private String _virtualHost = DEFAULT_VHOST;
  private int _requestedChannelMax = DEFAULT_CHANNEL_MAX;
  private int _requestedFrameMax = DEFAULT_FRAME_MAX;
  private int _requestedHeartbeat = DEFAULT_HEARTBEAT;

  public ConnectionParameters() { }

  private static String safeGetProperty(String key, String def) {
    try {
      return System.getProperty(key, def);
    } catch (AccessControlException ex) {
      return def;
    }
  }

  public String getUserName() {
    return _userName;
  }

  public void setUsername(String userName) {
    _userName = userName;
  }

  public String getPassword() {
    return _password;
  }

  public void setPassword(String password) {
    _password = password;
  }

  public String getVirtualHost() {
    return _virtualHost;
  }

  public void setVirtualHost(String virtualHost) {
    _virtualHost = virtualHost;
  }

  public int getRequestedChannelMax() {
    return _requestedChannelMax;
  }

  public void setRequestedFrameMax(int requestedFrameMax) {
    _requestedFrameMax = requestedFrameMax;
  }

  public int getRequestedFrameMax() {
    return _requestedFrameMax;
  }

  public int getRequestedHeartbeat() {
    return _requestedHeartbeat;
  }

  public void setRequestedHeartbeat(int requestedHeartbeat) {
    _requestedHeartbeat = requestedHeartbeat;
  }

  public void setRequestedChannelMax(int requestedChannelMax) {
    _requestedChannelMax = requestedChannelMax;
  }
}
