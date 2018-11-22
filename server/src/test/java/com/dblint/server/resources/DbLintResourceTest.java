package com.dblint.server.resources;

import javax.ws.rs.client.Entity;

import com.dblint.server.pojo.QueryResponse;
import com.dblint.server.pojo.QueryResponseTest;
import com.dblint.server.pojo.SqlQuery;
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

import static org.junit.jupiter.api.Assertions.*;

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
    SqlQuery sql = new SqlQuery("select a from t where i = 5");
    QueryResponse response = RESOURCE.target("/api/dblint/digest")
        .request().post(Entity.json(sql)).readEntity(QueryResponse.class);
    assertEquals(
        "SELECT `A`\n" +
            "FROM `T`\n" +
            "WHERE `I` = ?", response.sql);
    assertTrue(response.success);
    assertNull(response.errorMessage);
    RESOURCE.after();
  }

  @Test
  public void exceptionTest() throws Throwable {
    RESOURCE.before();
    SqlQuery sql = new SqlQuery("select a from where i = 5");
    QueryResponse response = RESOURCE.target("/api/dblint/digest")
        .request().post(Entity.json(sql)).readEntity(QueryResponse.class);
    assertEquals("Encountered \"from where\" at line 1, column 10.",
        response.errorMessage.split("\n")[0]);
    assertFalse(response.success);
    assertNull(response.sql);
    RESOURCE.after();
  }

  @Test
  public void callPrettyTest() throws Throwable {
    RESOURCE.before();
    SqlQuery sql = new SqlQuery("select a from t where i = 5");
    QueryResponse response = RESOURCE.target("/api/dblint/pretty")
        .request().post(Entity.json(sql)).readEntity(QueryResponse.class);
    assertEquals(
        "SELECT `A`\n" +
            "FROM `T`\n" +
            "WHERE `I` = 5", response.sql);
    assertTrue(response.success);
    assertNull(response.errorMessage);
    RESOURCE.after();
  }

  @Test
  public void badDialectTest() throws Throwable {
    RESOURCE.before();
    SqlQuery sql = new SqlQuery("select a from t where i = 5", "someDialect");
    QueryResponse response = RESOURCE.target("/api/dblint/pretty")
        .request().post(Entity.json(sql)).readEntity(QueryResponse.class);
    assertEquals("No enum constant org.apache.calcite.sql.SqlDialect.DatabaseProduct.SOMEDIALECT",
        response.errorMessage);
    assertFalse(response.success);
    assertNull(response.sql);
    RESOURCE.after();
  }
}
