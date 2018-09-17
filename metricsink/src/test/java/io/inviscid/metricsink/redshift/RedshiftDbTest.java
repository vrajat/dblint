package io.inviscid.metricsink.redshift;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RedshiftDbTest {
  private static final String url = "jdbc:h2:mem:io.inviscid.metricsink.redshift.RedshiftDbTest";

  private Connection h2db;

  @BeforeEach
  void setH2db() throws SQLException {
    h2db = DriverManager.getConnection(url);
    Flyway flyway = new Flyway();
    flyway.setDataSource(url, "", "");
    flyway.setLocations("db/redshiftTestMigrations");
    flyway.migrate();
  }

  @AfterEach
  void tearDownH2db() throws SQLException {
    h2db.close();
  }

  @Test
  void checkTableListTest() throws SQLException {
    List<String> tables = new ArrayList<>();
    DatabaseMetaData md = h2db.getMetaData();
    ResultSet rs = md.getTables(null, "PUBLIC", null, null);
    while (rs.next()) {
      tables.add(rs.getString(3));
    }

    List<String> expected = Arrays.asList("PG_USER", "STL_QUERY",
        "STL_WLM_QUERY", "flyway_schema_history");
    assertIterableEquals(expected, tables);
  }

  @Test
  void queryStatsTest() {
    RedshiftDb redshiftDb = new RedshiftDb(url, "", "");
    List<QueryStats> queryStatsList = redshiftDb.getQueryStats(true);
    assertEquals(1, queryStatsList.size());

    QueryStats queryStats = queryStatsList.get(0);

    assertEquals("public", queryStats.db);
    assertEquals("inviscid", queryStats.user);
    assertEquals("label", queryStats.queryGroup);
    assertEquals(LocalDateTime.of(2018, 9, 13, 12, 0), queryStats.timestampHour);
    assertEquals(0.000075, queryStats.minDuration);
    assertEquals(0.000075, queryStats.avgDuration);
    assertEquals(0, queryStats.medianDuration);
    assertEquals(0, queryStats.p75);
    assertEquals(0, queryStats.p90);
    assertEquals(0, queryStats.p95);
    assertEquals(0, queryStats.p99);
    assertEquals(0, queryStats.p999);
    assertEquals(0.000075, queryStats.maxDuration);
  }
}