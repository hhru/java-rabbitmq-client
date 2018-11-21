package ru.hh.rabbitmq.spring;

import java.time.Duration;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import ru.hh.hhinvoker.client.InvokerClient;
import static ru.hh.hhinvoker.remote.RemoteTaskJob.createTaskJob;
import static ru.hh.rabbitmq.spring.PersistentPublisherResource.PGQ_PUBLISH;

@Path(PGQ_PUBLISH)
public class PersistentPublisherResource {

  public static final String PGQ_PUBLISH = "/pgq-publish";
  public static final String RETRY_MS = "retryEventDelayMs";

  private final InvokerClient invokerClient;
  private final PgqService pgqService;

  public PersistentPublisherResource(InvokerClient invokerClient, PgqService pgqService) {
    this.invokerClient = invokerClient;
    this.pgqService = pgqService;
  }

  @POST
  public void acceptInvoke(@QueryParam("taskId") int taskId, @QueryParam("launchId") int launchId, @QueryParam(RETRY_MS) long retryEventDelayMs) {
    createTaskJob(invokerClient, taskId, launchId, context -> pgqService.sendBatch(Duration.ofMillis(retryEventDelayMs))).run();
  }
}
