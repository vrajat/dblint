package io.inviscid.metricsink.sinks;

import io.inviscid.metricsink.metrics.Metrics;
import io.inviscid.metricsink.metrics.Redshift;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

class MySQLSinkTest {
  private static Connection h2db;
  private static String url = "jdbc:h2:mem:io.inviscid.metricsink.sinks.MySQLSinkTest";

  class H2Sink extends MySQLSink {
    H2Sink(String url, String user, String password, Metrics metrics) {
      super(url, user, password, metrics);
    }
  }

  @BeforeAll
  static void setH2db() throws SQLException {
    h2db = DriverManager.getConnection(url);
  }

  @Test
  void migrationTest() throws SQLException {
    Metrics metrics = new Redshift();
    H2Sink h2Sink = new H2Sink(url, "", "", metrics);

    List<String> tables = new ArrayList<>();
    DatabaseMetaData md = h2db.getMetaData();
    ResultSet rs = md.getTables(null, "PUBLIC", null, null);
    while (rs.next()) {
      tables.add(rs.getString(3));
    }

    List<String> expected = new ArrayList<>();
    expected.add("QUERY_STATS");
    expected.add("flyway_schema_history");
    Assertions.assertIterableEquals(expected, tables);
  }
}