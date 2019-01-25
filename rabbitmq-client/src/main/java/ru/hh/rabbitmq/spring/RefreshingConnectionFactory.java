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

public class RefreshingConnectionFactory extends ConnectionFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(RefreshingConnectionFactory.class);

  private final long maxAgeMillis;

  public RefreshingConnectionFactory(Duration maxAge) {
    maxAgeMillis = maxAge.toMillis();
  }

  @Override
  public Connection newConnection(ExecutorService executor, AddressResolver addressResolver, String clientProvidedName)
      throws IOException, TimeoutException {
    Connection connection = super.newConnection(executor, addressResolver, clientProvidedName);
    return new ClosingProxy(connection, System.currentTimeMillis(), maxAgeMillis);
  }

  protected static class ClosingProxy implements Connection {
    private final Connection delegate;
    private final long creationTimestamp;
    private final long maxAgeMillis;

    public ClosingProxy(Connection delegate, long creationTimestamp, long maxAgeMillis) {
      this.delegate = delegate;
      this.creationTimestamp = creationTimestamp;
      this.maxAgeMillis = maxAgeMillis;
    }

    @Override
    public boolean isOpen() {
      if (System.currentTimeMillis() - creationTimestamp >= maxAgeMillis) {
          if (delegate.isOpen()) {
            try {
              close();
            } catch (IOException e) {
              LOGGER.error("Failed to close connection {}", delegate);
            }
          }
      }
      return delegate.isOpen();
    }

    @Override
    public InetAddress getAddress() {
      return delegate.getAddress();
    }

    @Override
    public int getPort() {
      return delegate.getPort();
    }

    @Override
    public int getChannelMax() {
      return delegate.getChannelMax();
    }

    @Override
    public int getFrameMax() {
      return delegate.getFrameMax();
    }

    @Override
    public int getHeartbeat() {
      return delegate.getHeartbeat();
    }

    @Override
    public Map<String, Object> getClientProperties() {
      return delegate.getClientProperties();
    }

    @Override
    public String getClientProvidedName() {
      return delegate.getClientProvidedName();
    }

    @Override
    public Map<String, Object> getServerProperties() {
      return delegate.getServerProperties();
    }

    @Override
    public Channel createChannel() throws IOException {
      return delegate.createChannel();
    }

    @Override
    public Channel createChannel(int channelNumber) throws IOException {
      return delegate.createChannel(channelNumber);
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }

    @Override
    public void close(int closeCode, String closeMessage) throws IOException {
      delegate.close(closeCode, closeMessage);
    }

    @Override
    public void close(int timeout) throws IOException {
      delegate.close(timeout);
    }

    @Override
    public void close(int closeCode, String closeMessage, int timeout) throws IOException {
      delegate.close(closeCode, closeMessage, timeout);
    }

    @Override
    public void abort() {
      delegate.abort();
    }

    @Override
    public void abort(int closeCode, String closeMessage) {
      delegate.abort(closeCode, closeMessage);
    }

    @Override
    public void abort(int timeout) {
      delegate.abort(timeout);
    }

    @Override
    public void abort(int closeCode, String closeMessage, int timeout) {
      delegate.abort(closeCode, closeMessage, timeout);
    }

    @Override
    public void addBlockedListener(BlockedListener listener) {
      delegate.addBlockedListener(listener);
    }

    @Override
    public BlockedListener addBlockedListener(BlockedCallback blockedCallback, UnblockedCallback unblockedCallback) {
      return delegate.addBlockedListener(blockedCallback, unblockedCallback);
    }

    @Override
    public boolean removeBlockedListener(BlockedListener listener) {
      return delegate.removeBlockedListener(listener);
    }

    @Override
    public void clearBlockedListeners() {
      delegate.clearBlockedListeners();
    }

    @Override
    public ExceptionHandler getExceptionHandler() {
      return delegate.getExceptionHandler();
    }

    @Override
    public String getId() {
      return delegate.getId();
    }

    @Override
    public void setId(String id) {
      delegate.setId(id);
    }

    @Override
    public void addShutdownListener(ShutdownListener listener) {
      delegate.addShutdownListener(listener);
    }

    @Override
    public void removeShutdownListener(ShutdownListener listener) {
      delegate.removeShutdownListener(listener);
    }

    @Override
    public ShutdownSignalException getCloseReason() {
      return delegate.getCloseReason();
    }

    @Override
    public void notifyListeners() {
      delegate.notifyListeners();
    }

    @Override
    public String toString() {
      return "ClosingProxy{delegate=" + delegate + ", creationTimestamp=" + creationTimestamp + ", maxAgeMillis=" + maxAgeMillis + '}';
    }
  }
}
