package io.inviscid.metricsink.redshift;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MySQLSinkTest {
  private static final String url = "jdbc:h2:mem:io.inviscid.metricsink.sinks.MySQLSinkTest";

  private Connection h2db;
  private MySQLSink mySQLSink;

  @BeforeEach
  void setMySQLSink() throws SQLException {
    h2db = DriverManager.getConnection(url);
    mySQLSink = new MySQLSink(url, "", "");
    mySQLSink.initialize();
  }

  @AfterEach
  void dropAllObjects() throws SQLException {
    Statement statement = h2db.createStatement();
    statement.execute("DROP ALL OBJECTS");
    statement.close();
    h2db.close();
  }

  @Test
  void migrationTest() throws SQLException {
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

  @Test
  void insertOneQueryStat() throws SQLException {
    QueryStats queryStats = new QueryStats("db", "user", "user_group", LocalDateTime.now(),
        0.1, 0.2, 0.3, 0.4, 0.5, 0.6,
        0.9, 0.999, 1);
    mySQLSink.insertQueryStats(queryStats);

    Statement statement = h2db.createStatement();
    ResultSet resultSet = statement.executeQuery("select db, user, query_group, day, " +
          "min_duration, avg_duration, median_duration, p75_duration, p90_duration, p95_duration," +
          "p99_duration, p999_duration, max_duration from PUBLIC.query_stats");

    resultSet.next();

    assertEquals(queryStats.db, resultSet.getString("db"));
    assertEquals(queryStats.user, resultSet.getString("user"));
    assertEquals(queryStats.queryGroup, resultSet.getString("query_group"));
    assertEquals(queryStats.day, resultSet.getTimestamp("day").toLocalDateTime());
    assertEquals(queryStats.minDuration, resultSet.getDouble("min_duration"));
    assertEquals(queryStats.avgDuration, resultSet.getDouble("avg_duration"));
    assertEquals(queryStats.medianDuration, resultSet.getDouble("median_duration"));
    assertEquals(queryStats.p75Duration, resultSet.getDouble("p75_duration"));
    assertEquals(queryStats.p90Duration, resultSet.getDouble("p90_duration"));
    assertEquals(queryStats.p95Duration, resultSet.getDouble("p95_duration"));
    assertEquals(queryStats.p99Duration, resultSet.getDouble("p99_duration"));
    assertEquals(queryStats.p999Duration, resultSet.getDouble("p999_duration"));
    assertEquals(queryStats.maxDuration, resultSet.getDouble("max_duration"));
  }
}