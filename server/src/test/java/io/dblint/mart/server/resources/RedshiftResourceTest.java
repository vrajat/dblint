package io.dblint.mart.server.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import io.dblint.mart.server.ConnectionsCron;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.ws.rs.client.Entity;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(DropwizardExtensionsSupport.class)
public class RedshiftResourceTest {
  private static final ConnectionsCron mockCron = mock(ConnectionsCron.class);
  private static final ExecutorService mockService = mock(ExecutorService.class);

  private static final ObjectMapper OBJECT_MAPPER = Jackson.newObjectMapper()
      .registerModule(new GuavaModule());

  static final ResourceExtension RESOURCE = ResourceExtension.builder()
      .addResource(new RedshiftResource(mockCron, mockService))
      .setMapper(OBJECT_MAPPER)
      .build();

  @BeforeAll
  static void setResource() throws Throwable {
    RESOURCE.before();
  }

  @AfterAll
  static void tearDownResource() throws Throwable {
    RESOURCE.after();
  }

  @Test
  public void callCaptureTest() throws Throwable {
    RESOURCE.before();
    String response = RESOURCE.target("/redshift/high_cpu_capture")
        .request().post(Entity.json("")).readEntity(String.class);
    assertEquals("Redshift HighCpuEvent Capture initiated", response);
    RESOURCE.after();
  }

  @Disabled
  @Test
  public void exceptionTest() throws Throwable {
    when(mockService.submit(any(Callable.class))).thenThrow(RuntimeException.class);
    RESOURCE.before();
    String response = RESOURCE.target("/redshift/high_cpu_capture")
        .request().post(Entity.json("")).readEntity(String.class);
    assertEquals("Redshift HighCpuEvent Capture initiated", response);
    RESOURCE.after();
  }
}
