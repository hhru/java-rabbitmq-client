package ru.hh.rabbitmq.spring.persistent;

import java.time.Duration;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import ru.hh.hhinvoker.client.InvokerClient;
import static ru.hh.hhinvoker.remote.RemoteTaskJob.createTaskJob;
import static ru.hh.rabbitmq.spring.persistent.PersistentPublisherResource.PGQ_PUBLISH;

@Path(PGQ_PUBLISH)
public class PersistentPublisherResource {

  public static final String PGQ_PUBLISH = "/pgq-publish";
  public static final String RETRY_MS = "retryEventDelayMs";

  private final InvokerClient invokerClient;
  private final DatabaseQueueService databaseQueueService;

  public PersistentPublisherResource(InvokerClient invokerClient, DatabaseQueueService databaseQueueService) {
    this.invokerClient = invokerClient;
    this.databaseQueueService = databaseQueueService;
  }

  @POST
  public void acceptInvoke(@QueryParam("taskId") int taskId, @QueryParam("launchId") int launchId, @QueryParam(RETRY_MS) long retryEventDelayMs) {
    createTaskJob(invokerClient, taskId, launchId, context -> databaseQueueService.sendBatch(Duration.ofMillis(retryEventDelayMs))).run();
  }
}
