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
  private boolean durable;
  private ChannelFactory factory;
  private AutoreconnectProperties autoreconnect;

  private boolean closed;
  private boolean inTransaction;

  private Channel channel;

  public ChannelWrapper(QueueProperties properties, ChannelFactory factory) {
    this(properties.getName(), properties.isDurable(), factory);
  }

  public ChannelWrapper(String queue, boolean durable, ChannelFactory factory) {
    this.queue = queue;
    this.durable = durable;
    this.factory = factory;
    this.autoreconnect = new AutoreconnectProperties(false);
  }

  public ChannelWrapper(String queue, ChannelFactory factory, AutoreconnectProperties autoreconnect) {
    this.queue = queue;
    this.factory = factory;
    this.autoreconnect = autoreconnect;
  }

  public void begin() {
    if (inTransaction) {
      throw new IllegalStateException("Already in transaction");
    }
    ensureConnectedAndRunning();
    try {
      channel.txSelect();
      inTransaction = true;
    } catch (IOException e) {
      throw new RuntimeException("Error starting transaction", e);
    }
  }

  public void commit() {
    if (!inTransaction) {
      throw new IllegalStateException("Not in transaction");
    }
    ensureConnectedAndRunning();
    try {
      channel.txCommit();
      inTransaction = false;
    } catch (IOException e) {
      throw new RuntimeException("Error commiting transaction", e);
    }
  }

  public void rollback() {
    if (!inTransaction) {
      // TODO it seems better to throw illegal state exception here
      return;
    }
    ensureConnectedAndRunning();
    try {
      channel.txRollback();
      // TODO: beware of channel remaining in transactional state here (see amqp specs)
      inTransaction = false;
    } catch (IOException e) {
      throw new RuntimeException("Error rolling back transaction", e);
    }
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
    try {
      receiver.onStart();
      while (!receiver.isEnough() && !Thread.currentThread().isInterrupted()) {
        if (timeout != null) {
          delivery = consumer.nextDelivery(timeout);
        } else {
          delivery = consumer.nextDelivery();
        }
        if (delivery == null) {
          break;
        }
        message = Message.fromDelivery(delivery);

        if (Thread.currentThread().isInterrupted()) {
          return;
        }

        receiver.receive(message);

        // if we got the message and processed it we need to send ack even if thread was interrupted
        // so we save interrupted flag after receiver action and restore it after ack action because sometimes RabbitMQ resets it somewhere inside.
        boolean interrupted = Thread.currentThread().isInterrupted();
        long deliveryTag = delivery.getEnvelope().getDeliveryTag();
        channel.basicAck(deliveryTag, false);

        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    } finally {
      receiver.onFinish();
    }
  }

  public void close() {
    closed = true;
    factory.returnChannel(channel);
  }

  private void ensureConnectedAndRunning() {
    if (closed) {
      throw new IllegalStateException("Already closed");
    }
    ensureConnected();
  }

  private void ensureConnected() {
    if (inTransaction) {
      // ignore reconnection attempt, let closed connection throw it's own exception
      return;
    }

    int attempt = 0;
    while (channel == null || !channel.isOpen()) {
      attempt++;
      try {
        logger.debug("Openning channel");
        channel = factory.openChannel(queue, durable);
        logger.debug("Channel is ready");
      } catch (IOException e) {
        if (!autoreconnect.isEnabled() || attempt > autoreconnect.getAttempts()) {
          throw new RuntimeException("Can't open channel", e);
        }
        logger.warn(
          String.format(
            "Attempt %d out of %d to reconnect the channel has failed, sleeping then retrying", attempt,
            autoreconnect.getAttempts()), e);
        try {
          // TODO: random delay is better
          TimeUnit.MILLISECONDS.sleep(autoreconnect.getDelay());
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Sleep between autoreconnection attempts has been interrupted", e1);
        }
      }
    } // while not connected
  }
}
