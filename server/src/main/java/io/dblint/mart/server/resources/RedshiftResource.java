package io.dblint.mart.server.resources;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Metered;
import io.dblint.mart.server.ConnectionsCron;

import java.util.concurrent.ExecutorService;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/redshift/")
@Produces(MediaType.APPLICATION_JSON)
public class RedshiftResource {
  final ConnectionsCron cron;
  final ExecutorService service;

  public RedshiftResource(ConnectionsCron cron, ExecutorService service) {
    this.cron = cron;
    this.service = service;
  }

  @POST
  @Path("/high_cpu_capture")
  @Metered
  @ExceptionMetered
  public String highCpuCapture() {
    service.submit(cron);
    return "Redshift HighCpuEvent Capture initiated";
  }
}
