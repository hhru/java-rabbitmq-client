package ru.hh.rabbitmq.spring;

import com.rabbitmq.client.AddressResolver;
import com.rabbitmq.client.BlockedCallback;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.UnblockedCallback;
import java.io.IOException;
import java.net.InetAddress;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClosingTooOldConnectionFactory extends ConnectionFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClosingTooOldConnectionFactory.class);

  private final long maxAgeMillis;

  public ClosingTooOldConnectionFactory(Duration maxAge) {
    maxAgeMillis = maxAge.toMillis();
  }

  @Override
  public Connection newConnection(ExecutorService executor, AddressResolver addressResolver, String clientProvidedName)
      throws IOException, TimeoutException {
    Connection connection = super.newConnection(executor, addressResolver, clientProvidedName);
    return new ClosingProxy(connection, System.currentTimeMillis(), maxAgeMillis);
  }

  protected static class ClosingProxy implements Connection {

    private final Connection target;
    private final long creationTimestamp;
    private final long maxAgeMillis;

    public ClosingProxy(Connection target, long creationTimestamp, long maxAgeMillis) {
      this.target = target;
      this.creationTimestamp = creationTimestamp;
      this.maxAgeMillis = maxAgeMillis;
    }

    @Override
    public boolean isOpen() {
      if (System.currentTimeMillis() - creationTimestamp >= maxAgeMillis) {
          if (target.isOpen()) {
            try {
              close();
            } catch (IOException e) {
              LOGGER.error("Failed to close connection {}", target);
            }
          }
      }
      return target.isOpen();
    }

    @Override
    public InetAddress getAddress() {
      return target.getAddress();
    }

    @Override
    public int getPort() {
      return target.getPort();
    }

    @Override
    public int getChannelMax() {
      return target.getChannelMax();
    }

    @Override
    public int getFrameMax() {
      return target.getFrameMax();
    }

    @Override
    public int getHeartbeat() {
      return target.getHeartbeat();
    }

    @Override
    public Map<String, Object> getClientProperties() {
      return target.getClientProperties();
    }

    @Override
    public String getClientProvidedName() {
      return target.getClientProvidedName();
    }

    @Override
    public Map<String, Object> getServerProperties() {
      return target.getServerProperties();
    }

    @Override
    public Channel createChannel() throws IOException {
      return target.createChannel();
    }

    @Override
    public Channel createChannel(int channelNumber) throws IOException {
      return target.createChannel(channelNumber);
    }

    @Override
    public void close() throws IOException {
      target.close();
    }

    @Override
    public void close(int closeCode, String closeMessage) throws IOException {
      target.close(closeCode, closeMessage);
    }

    @Override
    public void close(int timeout) throws IOException {
      target.close(timeout);
    }

    @Override
    public void close(int closeCode, String closeMessage, int timeout) throws IOException {
      target.close(closeCode, closeMessage, timeout);
    }

    @Override
    public void abort() {
      target.abort();
    }

    @Override
    public void abort(int closeCode, String closeMessage) {
      target.abort(closeCode, closeMessage);
    }

    @Override
    public void abort(int timeout) {
      target.abort(timeout);
    }

    @Override
    public void abort(int closeCode, String closeMessage, int timeout) {
      target.abort(closeCode, closeMessage, timeout);
    }

    @Override
    public void addBlockedListener(BlockedListener listener) {
      target.addBlockedListener(listener);
    }

    @Override
    public BlockedListener addBlockedListener(BlockedCallback blockedCallback, UnblockedCallback unblockedCallback) {
      return target.addBlockedListener(blockedCallback, unblockedCallback);
    }

    @Override
    public boolean removeBlockedListener(BlockedListener listener) {
      return target.removeBlockedListener(listener);
    }

    @Override
    public void clearBlockedListeners() {
      target.clearBlockedListeners();
    }

    @Override
    public ExceptionHandler getExceptionHandler() {
      return target.getExceptionHandler();
    }

    @Override
    public String getId() {
      return target.getId();
    }

    @Override
    public void setId(String id) {
      target.setId(id);
    }

    @Override
    public void addShutdownListener(ShutdownListener listener) {
      target.addShutdownListener(listener);
    }

    @Override
    public void removeShutdownListener(ShutdownListener listener) {
      target.removeShutdownListener(listener);
    }

    @Override
    public ShutdownSignalException getCloseReason() {
      return target.getCloseReason();
    }

    @Override
    public void notifyListeners() {
      target.notifyListeners();
    }
  }
}
