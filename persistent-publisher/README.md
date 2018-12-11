# Надежный паблишер в rabbit
## Нужна, чтобы не плодить таблички с кронами для их периодической очистки
### Обеспечивает eventual consistency, доставка at least one
### Зависимости:
0. pgq
1. Hibernate
2. Spring, rabbitmq-client
### Для использования:
0. (*)Создать очередь в rabbit
1. Убедиться у dba, что pgq установлен на интересующей вас базе
2. Подготовить скрипты создания очереди и консюмера в базе для этой очереди
    ```
    SELECT * FROM pgq.create_queue(:queueName);
    SELECT * FROM pgq.register_consumer(:queueName, :consumerName);
    ```
3. (*)Создать таблицу для логирования ошибок
    ```
    CREATE TABLE IF NOT EXISTS rabbit_queue_error (
        event_id BIGINT PRIMARY KEY,
        log_date TIMESTAMP NOT NULL,
        queue_name TEXT NOT NULL,
        consumer_name TEXT NOT NULL,
        destination_content TEXT NOT NULL,
        message_content TEXT
    );
    ```
4. Добавить зависимость:
    ```
    <dependency>
        <groupId>ru.hh.rabbitmq.spring</groupId>
        <artifactId>persistent-publisher</artifactId>
        <version>...</version>
    </dependency>
    ```
5. Добавить PersistentPublisherConfig в спринг
6. Добавить конфиг (почти как для обычного rabbit-client'а)
    ```
    persistent.publisher.mq.updateTopicsVisibilityRequest.hosts=
    persistent.publisher.mq.updateTopicsVisibilityRequest.port=
    persistent.publisher.mq.updateTopicsVisibilityRequest.username=
    persistent.publisher.mq.updateTopicsVisibilityRequest.password=
    persistent.publisher.mq.updateTopicsVisibilityRequest.publisher.exchange=
    persistent.publisher.mq.updateTopicsVisibilityRequest.publisher.routingKey=
    persistent.publisher.mq.updateTopicsVisibilityRequest.virtualhost=
    persistent.publisher.mq.updateTopicsVisibilityRequest.databaseQueueName=
    persistent.publisher.mq.updateTopicsVisibilityRequest.databaseQueueConsumerName=
    persistent.publisher.mq.updateTopicsVisibilityRequest.errorLogTableName=
    persistent.publisher.mq.updateTopicsVisibilityRequest.retryDelaySec=
    ```
7. Заюзать PersistentPublisherBuilderFactory для создания паблишеров по конфигам
    ```
    persistentPublisherBuilderFactory.createPublisherBuilder("updateTopicsVisibilityRequest")
    ```
8. Создать крон для отправки сообщений, который вызывает `ru.hh.rabbitmq.spring.persistent.DatabaseQueueService.sendBatch`
> Крон лучше делать на hh-invoker, т.к. один и тот же консюмер не должен читать конкурентно.  
Ну или на каждый инстанс заводить отдельного консюмера))
