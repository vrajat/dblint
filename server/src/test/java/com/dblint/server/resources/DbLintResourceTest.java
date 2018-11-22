package com.dblint.server.resources;

import javax.ws.rs.client.Entity;

import com.dblint.server.pojo.GitState;
import com.dblint.server.pojo.QueryResponse;
import com.dblint.server.pojo.SqlQuery;
import com.dblint.sqlplanner.planner.Parser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(DropwizardExtensionsSupport.class)
public class DbLintResourceTest {
  private static Parser parser = new Parser();
  private static GitState state = new GitState("",
      "json-response",
      "true",
      "git@github.com:vrajat/mart.git",
      "28010b73bae868ba252fbf2974f93d30ae189ea0",
      "28010b7",
      "28010b7-dirty", "28010b7-dirty",
      "User Name",
      "xxxxx@yyyyy.io",
      "new:usr:Improve Dblint resource parameters and "
          + "responses\n\nUse classes (SqlQuery and QueryResponse) instead of strings for\nREST "
          + "calls to digest and pretty print.",
      "new:usr:Improve Dblint resource parameters and "
          + "responses",
      "2018-11-22T21:24:13+0530",
      "",
      "",
      "User Name",
      "xxxxxx@yyyyyyy.io",
      "2018-11-22T22:33:26+0530",
      "build-machine",
      "0.4.3-SNAPSHOT",
      "94");

  private static final ObjectMapper OBJECT_MAPPER = Jackson.newObjectMapper()
      .registerModule(new GuavaModule());

  static final ResourceExtension RESOURCE = ResourceExtension.builder()
      .addResource(new DbLintResource(parser, state))
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

  @Test
  public void version() throws Throwable {
    RESOURCE.before();
    GitState state = RESOURCE.target("/api/dblint/version")
        .request().get().readEntity(GitState.class);
    assertEquals("0.4.3-SNAPSHOT", state.buildVersion);
    RESOURCE.after();
  }
}
