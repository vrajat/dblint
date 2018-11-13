package com.dblint.server.resources;

import static org.junit.jupiter.api.Assertions.assertEquals;

import javax.ws.rs.client.Entity;

import com.dblint.sqlplanner.planner.Parser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(DropwizardExtensionsSupport.class)
public class DbLintResourceTest {
  private static Parser parser = new Parser();

  private static final ObjectMapper OBJECT_MAPPER = Jackson.newObjectMapper()
      .registerModule(new GuavaModule());

  static final ResourceExtension RESOURCE = ResourceExtension.builder()
      .addResource(new DbLintResource(parser))
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
  public void callDigestTest() throws Throwable {
    RESOURCE.before();
    String sql = "select a from t where i = 5";
    String response = RESOURCE.target("/api/dblint/digest")
        .request().post(Entity.json(sql)).readEntity(String.class);
    assertEquals(
        "SELECT `A`\n" +
            "FROM `T`\n" +
            "WHERE `I` = ?", response);
    RESOURCE.after();
  }

  @Disabled
  @Test
  public void exceptionTest() throws Throwable {
    RESOURCE.before();
    String response = RESOURCE.target("/api/dblint/digest")
        .request().post(Entity.json("select a from where i")).readEntity(String.class);
    assertEquals("Redshift HighCpuEvent Capture initiated", response);
    RESOURCE.after();
  }

  @Test
  public void callPrettyTest() throws Throwable {
    RESOURCE.before();
    String sql = "select a from t where i = 5";
    String response = RESOURCE.target("/api/dblint/pretty")
        .request().post(Entity.json(sql)).readEntity(String.class);
    assertEquals(
        "SELECT `A`\n" +
            "FROM `T`\n" +
            "WHERE `I` = 5", response);
    RESOURCE.after();
  }
}
