package ru.hh.rabbitmq.receive;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import ru.hh.rabbitmq.ChannelFactory;
import ru.hh.rabbitmq.ConnectionFactory;
import ru.hh.rabbitmq.ConnectionFailedException;
import ru.hh.rabbitmq.impl.ChannelFactoryImpl;
import ru.hh.rabbitmq.impl.SingleConnectionFactory;
import ru.hh.rabbitmq.simple.Message;
import ru.hh.rabbitmq.simple.MessageReceiver;
import ru.hh.rabbitmq.simple.MessagesReceiver;

public class Receiver {
  private Integer prefetchCount;
  private ConnectionFactory connectionFactory;
  private ChannelFactory channelFactory;
  private Channel channel;

  private volatile boolean closed;

  public Receiver(
      com.rabbitmq.client.ConnectionFactory connectionFactory, TimeUnit retryUnit, long retryDelay, int attempts,
      Integer prefetchCount, Address address) {
    if (connectionFactory == null) {
      throw new IllegalArgumentException("no connection factory specified");
    }
    if (address == null) {
      throw new IllegalArgumentException("no address specified");
    }
    this.prefetchCount = prefetchCount;
    this.connectionFactory = new SingleConnectionFactory(connectionFactory, retryUnit, retryDelay, attempts, address);
    this.channelFactory = new ChannelFactoryImpl(this.connectionFactory);
  }

  /**
   * Receives and processes single message from the queue. This method does not block the caller and returns immediately.
   *
   * @param  receiver  receiver implementation that will be used to process incoming message
   * @param  queueName  name of queue to read messages from
   *
   * @return  true if the queue returned a message, false if the queue was empty at the time of calling
   *
   * @throws  IOException
   * @throws  InterruptedException
   */
  public boolean receiveSingle(MessageReceiver receiver, String queueName) throws IOException, InterruptedException {
    checkQueueName(queueName);
    ensureConnected();
    GetResponse response = channel.basicGet(queueName, false);
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
   * {@link MessagesReceiver#isEnough()} returns true, timeout has been reached or thread is interrupted.
   *
   * @param  receiver  receiver implementation that will be used to process incoming message
   * @param  queueName  name of queue to read messages from
   * @param  timeout  max time to wait in blocking state, null for infinite timeout
   *
   * @throws  IOException
   * @throws  ShutdownSignalException
   * @throws  InterruptedException
   */
  public void receive(MessagesReceiver receiver, String queueName, Long timeout) throws IOException, ShutdownSignalException,
    InterruptedException {
    checkQueueName(queueName);
    ensureConnected();
    if (Thread.currentThread().isInterrupted()) {
      return;
    }
    QueueingConsumer consumer = new QueueingConsumer(channel);
    String consumerTag = channel.basicConsume(queueName, false, consumer);
    Delivery delivery;
    Message message;
    try {
      receiver.onStart();
      do {
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

        if (interrupted && !Thread.currentThread().isInterrupted()) {
          Thread.currentThread().interrupt();
        }
      } while (!receiver.isEnough() && !Thread.currentThread().isInterrupted());
    } finally {
      channel.basicCancel(consumerTag);
      receiver.onFinish();
    }
  }

  public void close() {
    closed = true;
    channelFactory.returnChannel(channel);
    this.connectionFactory.close();
  }

  private void checkQueueName(String queueName) {
    if (queueName == null || queueName.trim().isEmpty()) {
      throw new IllegalArgumentException("no queue name is specified");
    }
  }

  private void ensureConnected() {
    if (closed) {
      throw new IllegalStateException("Already closed");
    }
    if (channel == null || !channel.isOpen()) {
      try {
        channel = channelFactory.getChannel();
        if (prefetchCount != null) {
          channel.basicQos(prefetchCount);
        }
      } catch (IOException e) {
        throw new ConnectionFailedException("Can't open channel", e);
      }
    }
  }
}
