## JAVA-RABBITMQ-CLIENT

RabbitMQ client library that uses Spring-Rabbit as a base. 

# Overview

There are three key classes.

Factory:
```
ru.hh.rabbitmq.spring.ClientFactory
```
Receiving messages is performed using:
```
ru.hh.rabbitmq.spring.Receiver
```
Publishing messages is performed using:
```
ru.hh.rabbitmq.spring.Publisher
```

# Configuration

```

```

# Usage example:

```java
    // create receiver
    Properties properties = new Properties();
    properties.setProperty(HOSTS, "host1,host2");
    properties.setProperty(USERNAME, "guest");
    properties.setProperty(PASSWORD, "guest");
    properties.setProperty(RECEIVER_QUEUES, "springq");

    ClientFactory factory = new ClientFactory(properties);
    Receiver receiver = factory.createReceiver();

    // listener and error handler in one object
    Object jsonListener = new ErrorHandler() {
    
      public void handleError(Throwable t) {
        throw new AmqpRejectAndDontRequeueException(t.getMessage());
      }

      public void handleMessage(Map<String, Object> object) {
        System.out.println(object);
      }
    };

    receiver.withJsonListener(jsonListener).start();

    // create publishers
    properties.setProperty(PUBLISHER_EXCHANGE, "spring");
    properties.setProperty(PUBLISHER_ROUTING_KEY, "do");

    properties.setProperty(HOSTS, "host1");
    factory = new ClientFactory(properties);
    Publisher publisher1 = factory.createPublisher().withJsonMessageConverter();

    properties.setProperty(HOSTS, "host2");
    factory = new ClientFactory(properties);
    Publisher publisher2 = factory.createPublisher().withJsonMessageConverter();

    publisher1.start();
    publisher2.start();

    // send something
    for (int i = 0; i < 100; i++) {
      send(publisher1, "host1", i);
      send(publisher2, "host2", i);
      Thread.sleep(100);
    }

    // shutdown
    Thread.sleep(5000);

    publisher1.stop();
    publisher2.stop();
    receiver.stop();
...    
  private static void send(Publisher publisher, String id, int counter) {
    Map<String, Object> body = new HashMap<String, Object>();
    body.put("counter", counter);
    body.put("id", id);
    publisher.send(body);
  }
```
