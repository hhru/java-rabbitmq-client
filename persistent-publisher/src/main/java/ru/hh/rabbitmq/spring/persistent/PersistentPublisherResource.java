package ru.hh.rabbitmq.spring.persistent;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import ru.hh.hhinvoker.client.InvokerClient;
import static ru.hh.hhinvoker.remote.RemoteTaskJob.createTaskJob;
import static ru.hh.rabbitmq.spring.persistent.PersistentPublisherResource.DATABASE_QUEUE_RABBIT_PUBLISH;

@Path(DATABASE_QUEUE_RABBIT_PUBLISH)
public class PersistentPublisherResource {

  public static final String DATABASE_QUEUE_RABBIT_PUBLISH = "/rabbit/db-events/publish";
  public static final String SENDER_KEY = "senderKey";

  private final InvokerClient invokerClient;
  private final DatabaseQueueService databaseQueueService;

  public PersistentPublisherResource(InvokerClient invokerClient, DatabaseQueueService databaseQueueService) {
    this.invokerClient = invokerClient;
    this.databaseQueueService = databaseQueueService;
  }

  @POST
  public void acceptInvoke(@QueryParam("taskId") int taskId, @QueryParam("launchId") int launchId,
    @QueryParam(SENDER_KEY) String senderKey) {
    createTaskJob(invokerClient, taskId, launchId, context -> databaseQueueService.sendBatch(senderKey)).run();
  }
}
