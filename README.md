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

Sample configuration (describes both receiver and publisher):
```
rabbit.server.hosts=127.0.0.1:5672,192.168.0.1:5672
rabbit.server.username=guest
rabbit.server.password=guest
rabbit.server.virtualhost=/
rabbit.server.heartbit.interval.seconds=5
rabbit.server.channel.cache.size=1
rabbit.server.close.timeout.millis=100

rabbit.server.receiver.name=myreceiver
rabbit.server.reciever.queues=myqueue1,myqueue2
rabbit.server.receiver.threadpool=1
rabbit.server.receiver.prefetch.count=1

rabbit.server.publisher.name=mypublisher
rabbit.server.publisher.confirms=false
rabbit.server.publisher.returns=false
rabbit.server.publisher.innerqueue.size=1
rabbit.server.publisher.exchange=myexchange
rabbit.server.publisher.routingKey=myroutingkey
rabbit.server.publisher.mandatory=true
rabbit.server.publisher.transactional=false
rabbit.server.publisher.reconnection.delay.millis=60000
```

# Usage example:

```java
    // create receiver
    Properties properties = new Properties();
    properties.setProperty("hosts", "host1,host2");
    properties.setProperty("username", "guest");
    properties.setProperty("password", "guest");
    properties.setProperty("receiver.queues", "myqueue1");

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
    properties.setProperty("publisher.exchange", "spring");
    properties.setProperty("publisher.routingkey", "do");

    properties.setProperty("hosts", "host1");
    factory = new ClientFactory(properties);
    Publisher publisher1 = factory.createPublisher().withJsonMessageConverter();

    properties.setProperty("hosts", "host2");
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
