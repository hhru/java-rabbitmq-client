package ru.hh.rabbitmq.simple;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.FlowListener;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.hh.rabbitmq.ChannelFactory;
import ru.hh.rabbitmq.ConnectionFailedException;
import ru.hh.rabbitmq.TransactionException;

public class ChannelWrapper implements FlowListener {
  private static final Logger logger = LoggerFactory.getLogger(ChannelWrapper.class);

  private String queueName;
  private String exchangeName;
  private String routingKey;
  private ChannelFactory factory;
  private boolean transactional;
  private Integer prefetchCount;
  private boolean waitFlow;

  private boolean nonEmptyTransaction;
  private boolean closed;

  private Channel channel;
  private volatile boolean flowActive = true;
  
  public static class Builder {
    private String queueName;
    private String exchangeName;
    private String routingKey;
    private ChannelFactory factory;
    private boolean transactional = false;
    private Integer prefetchCount = null;
    private boolean waitFlow = false;

    public Builder(String queueName, ChannelFactory factory) {
      this.queueName = queueName;
      this.factory = factory;
    }

    public Builder(String exchangeName, String routingKey, ChannelFactory factory) {
      this.exchangeName = exchangeName;
      this.factory = factory;
      this.routingKey = routingKey;
    }

    public Builder(String queueName, String exchangeName, String routingKey, ChannelFactory factory) {
      this.queueName = queueName;
      this.exchangeName = exchangeName;
      this.routingKey = routingKey;
      this.factory = factory;
    }

    public Builder transactional(boolean transactional) {
      this.transactional = transactional;
      return this;
    }
    
    public Builder prefetchCount(int count) {
      this.prefetchCount = count;
      return this;
    }
    
    public Builder waitFlow(boolean wait) {
      this.waitFlow = wait;
      return this;
    }
    
    public ChannelWrapper create() {
      return new ChannelWrapper(this);
    }
  }

  private ChannelWrapper(Builder builder) {
    this.queueName = builder.queueName;
    this.exchangeName = builder.exchangeName;
    this.routingKey = builder.routingKey;
    this.factory = builder.factory;
    this.transactional = builder.transactional;
    this.prefetchCount = builder.prefetchCount;
    this.waitFlow = builder.waitFlow;
  }

  public void commit() {
    try {
      channel.txCommit();
      nonEmptyTransaction = false;
    } catch (IOException e) {
      throw new TransactionException("Error commiting transaction", e);
    }
  }

  public void rollback() {
    try {
      channel.txRollback();
      // TODO: beware of channel remaining in transactional state here (see amqp specs)
      nonEmptyTransaction = false;
    } catch (IOException e) {
      throw new TransactionException("Error rolling back transaction", e);
    }
  }

  public void send(ReturnListener returnListener, Message... message) throws IOException {
    ensureConnected();
    setReturnListener(returnListener);
    send(message);
  }

  private String getTargetExchangeName() {
    if (exchangeName != null) {
      return exchangeName;
    }
    return "";
  }

  private String getTargetRoutingKey() {
    if (routingKey != null) {
      return routingKey;
    }
    return queueName;
  }

  public void send(Message... messages) throws IOException {
    ensureConnected();
    nonEmptyTransaction = true;
    for (Message message : messages) {
      checkFlow();
      channel.basicPublish(
        getTargetExchangeName(), getTargetRoutingKey(), true, false, message.getProperties(), message.getBody());
    }
  }

  public void send(Collection<Message> messages) throws IOException {
    for (Message message : messages) {
      send(message);
    }
  }

  public void send(ReturnListener returnListener, Collection<Message> messages) throws IOException {
    ensureConnected();
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
   * @throws  InterruptedException
   */
  public boolean receiveSingle(MessageReceiver receiver) throws IOException, InterruptedException {
    ensureConnected();
    GetResponse response = channel.basicGet(queueName, false);
    if (response == null) {
      return false;
    }
    Message message = Message.fromGetResponse(response);
    receiver.receive(message);
    long deliveryTag = response.getEnvelope().getDeliveryTag();
    channel.basicAck(deliveryTag, false);
    nonEmptyTransaction = true;
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
        nonEmptyTransaction = true;

        if (interrupted && !Thread.currentThread().isInterrupted()) {
          Thread.currentThread().interrupt();
        }
      } while (!receiver.isEnough() && !Thread.currentThread().isInterrupted());
    } finally {
      channel.basicCancel(consumerTag);
      receiver.onFinish();
    }
  }

  public void purge() throws IOException {
    ensureConnected();
    channel.queuePurge(queueName);
  }

  public void close() {
    closed = true;
    channel.setFlowListener(null);
    factory.returnChannel(channel);
  }

  private void ensureConnected() {
    if (closed) {
      throw new IllegalStateException("Already closed");
    }
    if (transactional && nonEmptyTransaction) {
      // ignore reconnection attempt, let closed connection throw it's own exception
      return;
    }
    if (channel == null || !channel.isOpen()) {
      try {
        channel = factory.getChannel();
        // TODO: channel can be stopped from the beginning, but we are considering it to be active
        // TODO: use channel.getFlow().getActive when it becomes threadsafe 
        flowActive = true;
        channel.setFlowListener(this);
        if (prefetchCount != null) {
          channel.basicQos(prefetchCount);
        }
        if (transactional) {
          channel.txSelect();
        }
      } catch (IOException e) {
        throw new ConnectionFailedException("Can't open channel", e);
      }
    }
  }
  
  private void checkFlow() {
    if (!flowActive && !waitFlow) {
      throw new IllegalStateException("can't send, server sent channel.flow = false");
    }
  }

  @Override
  public void handleFlow(boolean active) throws IOException {
    flowActive = active;
    if (active) {
      logger.info("got channel.flow = true, resuming to send messages");
    } else {
      logger.error("got channel.flow = false, sending is prohibited");
    }
  }
}
