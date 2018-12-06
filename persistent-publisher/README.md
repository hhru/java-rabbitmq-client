# Надежный паблишер в rabbit
## Нужна, чтобы не плодить таблички с кронами под
### Обеспечивает eventual consistency, доставка at least one
### Зависимости:
0. pgq
1. Hibernate
2. Spring, spring-amqp
999. hh-invoker
### Для использования:
1. Убедиться у dba, что pgq установлен на интересующей вас базе
2. Добавить модуль в pom проекта
3. Добавить PersistentPublisherConfig спринга
4. Засатисфачить зависимости PersistentPublisherConfig
5. Добавить конфиги, примерно так:
```
persistent.publisher.mq.upstream=
persistent.publisher.mq.jerseyBasePath=
persistent.publisher.mq.updateTopicsVisibilityRequest.hosts=
persistent.publisher.mq.updateTopicsVisibilityRequest.port=
persistent.publisher.mq.updateTopicsVisibilityRequest.username=
persistent.publisher.mq.updateTopicsVisibilityRequest.password=
persistent.publisher.mq.updateTopicsVisibilityRequest.publisher.exchange=
persistent.publisher.mq.updateTopicsVisibilityRequest.publisher.routingKey=
persistent.publisher.mq.updateTopicsVisibilityRequest.databaseQueueName=
persistent.publisher.mq.updateTopicsVisibilityRequest.virtualhost=
persistent.publisher.mq.updateTopicsVisibilityRequest.pollingIntervalSec=
persistent.publisher.mq.updateTopicsVisibilityRequest.retryDelaySec=
```
6.заюзать PersistentPublisherBuilderFactory для создания паблишеров по конфигам
> Для координации отправки используется hh-invoker - джобы создаются/обновляются при поднятии контекста.  
> **НО!** Автоудаление джобов не реализуемо в рамках инстанса. Поэтому если выпиливаете из сервиса эту штуку - не забудьте удалить джобу 
