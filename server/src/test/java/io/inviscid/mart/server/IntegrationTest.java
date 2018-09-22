package io.inviscid.mart.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.core.Response;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Created by rvenkatesh on 9/8/18.
 */
@ExtendWith(DropwizardExtensionsSupport.class)
@Disabled
public class IntegrationTest {
  public static final DropwizardAppExtension<MartConfiguration> EXTENSION =
      new DropwizardAppExtension<>(MartApplication.class,
          ResourceHelpers.resourceFilePath("test_config.yml"));

  @Test
  public void checkAdminIsAlive() {
    Response response = EXTENSION.client()
        .target(String.format("http://localhost:%d/", EXTENSION.getAdminPort()))
        .request()
        .get(Response.class);

    assertEquals(200, response.getStatus());
  }

  @Disabled
  @Test
  void migrationTest() throws SQLException {
    Connection mySql = DriverManager.getConnection("jdbc:h2:mem:IntegrationTestMySQL");
    List<String> tables = new ArrayList<>();
    DatabaseMetaData md = mySql.getMetaData();
    ResultSet rs = md.getTables(null, "PUBLIC", null, null);
    while (rs.next()) {
      tables.add(rs.getString(3));
    }

    List<String> expected = new ArrayList<>();
    expected.add("QUERY_STATS");
    expected.add("flyway_schema_history");
    assertIterableEquals(expected, tables);
  }

}
