package io.dblint.mart.server.resources;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(DropwizardExtensionsSupport.class)
public class RootResourceTest {

  private static final ObjectMapper OBJECT_MAPPER = Jackson.newObjectMapper()
      .registerModule(new GuavaModule());

  static final ResourceExtension RESOURCE = ResourceExtension.builder()
      .addResource(new RootResource())
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
  public void callRootTest() throws Throwable {
    RESOURCE.before();
    String response = RESOURCE.target("/")
        .request().get().readEntity(String.class);
    assertEquals(
        "healthy", response);
    RESOURCE.after();
  }

}
