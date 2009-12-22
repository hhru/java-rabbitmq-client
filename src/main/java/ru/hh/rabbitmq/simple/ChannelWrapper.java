package ru.hh.rabbitmq.simple;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.hh.rabbitmq.ChannelFactory;
import ru.hh.rabbitmq.impl.AutoreconnectProperties;

public class ChannelWrapper {
  private static final Logger logger = LoggerFactory.getLogger(ChannelWrapper.class);

  private String queue;
  private ChannelFactory factory;
  private AutoreconnectProperties autoreconnect;

  private volatile boolean closed;
  private volatile boolean inTransaction;

  private Channel channel;

  public ChannelWrapper(String queue, ChannelFactory factory) {
    this.queue = queue;
    this.factory = factory;
    this.autoreconnect = new AutoreconnectProperties(false);
  }

  public ChannelWrapper(String queue, ChannelFactory factory, AutoreconnectProperties autoreconnect) {
    this.queue = queue;
    this.factory = factory;
    this.autoreconnect = autoreconnect;
  }

  public void begin() throws IOException {
    if (inTransaction) {
      throw new IllegalStateException("Already in transaction");
    }
    ensureConnectedAndRunning();
    channel.txSelect();
    inTransaction = true;
  }

  public void commit() throws IOException {
    if (!inTransaction) {
      throw new IllegalStateException("Not in transaction");
    }
    ensureConnectedAndRunning();
    channel.txCommit();
    inTransaction = false;
  }

  public void rollback() throws IOException {
    if (!inTransaction) {
      return;
    }
    ensureConnectedAndRunning();
    channel.txRollback();
    inTransaction = false;
  }

  public void send(ReturnListener returnListener, Message... message) throws IOException {
    setReturnListener(returnListener);
    send(message);
  }

  public void send(Message... messages) throws IOException {
    ensureConnectedAndRunning();
    for (Message message : messages) {
      channel.basicPublish("", queue, true, false, message.getProperties(), message.getBody());
    }
  }

  public void send(Collection<Message> messages) throws IOException {
    for (Message message : messages) {
      send(message);
    }
  }

  public void send(ReturnListener returnListener, Collection<Message> messages) throws IOException {
    setReturnListener(returnListener);
    send(messages);
  }

  public void setReturnListener(ReturnListener returnListener) {
    if (returnListener != null) {
      channel.setReturnListener(returnListener);
    }
  }

  public void resetReturnListener() {
    channel.setReturnListener(null);
  }

  /**
   * Receives and processes single message from the queue. This method does not block the caller and returns immediately.
   *
   * @param  receiver  receiver implementation that will be used to process incoming message
   *
   * @return  true if the queue returned a message, false if the queue was empty at the time of calling
   *
   * @throws  IOException
   */
  public boolean receiveSingle(MessageReceiver receiver) throws IOException {
    ensureConnectedAndRunning();
    GetResponse response = channel.basicGet(queue, false);
    if (response == null) {
      return false;
    }
    Message message = Message.fromGetResponse(response);
    receiver.receive(message);
    long deliveryTag = response.getEnvelope().getDeliveryTag();
    channel.basicAck(deliveryTag, false);
    return true;
  }

  /**
   * Receives messages from queue, waits (blocks) until queue returns next message. Stops when supplied receiver's
   * {@link MessagesReceiver#isEnough()} returns true, if timeout has been reached or thread is interrupted.
   *
   * @param  receiver  receiver implementation that will be used to process incoming message
   * @param  timeout  max time to wait in blocking state
   *
   * @throws  IOException
   * @throws  ShutdownSignalException
   * @throws  InterruptedException
   */
  public void waitAndReceiveMany(MessagesReceiver receiver, Long timeout) throws IOException, ShutdownSignalException,
    InterruptedException {
    ensureConnectedAndRunning();
    QueueingConsumer consumer = new QueueingConsumer(channel);
    channel.basicConsume(queue, false, consumer);
    Delivery delivery;
    Message message;
    receiver.onStart();
    while (!receiver.isEnough()) {
      if (timeout != null) {
        delivery = consumer.nextDelivery(timeout);
      } else {
        delivery = consumer.nextDelivery();
      }
      if (delivery == null) {
        break;
      }
      message = Message.fromDelivery(delivery);
      receiver.receive(message);
      long deliveryTag = delivery.getEnvelope().getDeliveryTag();
      channel.basicAck(deliveryTag, false);
    }
    receiver.onFinish();
  }

  public void close() {
    closed = true;
    factory.returnChannel(channel);
  }

  private void ensureConnectedAndRunning() throws IOException {
    if (closed) {
      throw new IllegalStateException("Already closed");
    }
    ensureConnected();
  }

  private void ensureConnected() throws IOException {
    if (channel != null && channel.isOpen()) {
      return;
    }

    if (channel == null) {
      channel = factory.openChannel(queue);
      return;
    }
    autoreconnect();
  }

  private void autoreconnect() {
    if (!autoreconnect.isEnabled() || autoreconnect.getAttempts() == 0) {
      throw new IllegalStateException("No channel is available and autoreconnect is disabled");
    }

    int attempt = 0;
    while (attempt <= autoreconnect.getAttempts()) {
      try {
        channel = factory.openChannel(queue);
        break;
      } catch (IOException e) {
        logger.warn(String.format("Attempt %d out of %d to reconnect has failed", attempt, autoreconnect.getAttempts()), e);
        try {
          TimeUnit.MILLISECONDS.sleep(autoreconnect.getDelay());
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
          logger.warn("Sleep between autoreconnection attempts has been interrupted, ignoring", e1);
        }
      }
      attempt++;
    }

    if (channel == null || !channel.isOpen()) {
      throw new IllegalStateException("Failed to automatically reconnect channel");
    }
  }
}
