package io.dblint.mart.metricsink.mysql;

import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SinkTest {
  private static final String url = "jdbc:sqlite::memory:";

  private MetricRegistry metricRegistry;
  private Connection connection;
  private Sink sink;

  @BeforeEach
  void setSink() throws SQLException {
    connection = DriverManager.getConnection(url);
    metricRegistry = new MetricRegistry();
    sink = new Sink(url, "", "", metricRegistry);
    sink.initialize();
  }

  @Disabled
  @Test
  void migrationTest() throws SQLException {
    List<String> tables = new ArrayList<>();
    DatabaseMetaData md = connection.getMetaData();
    ResultSet rs = md.getTables(null, null, null, null);
    while (rs.next()) {
      tables.add(rs.getString(3));
    }

    List<String> expected = new ArrayList<>();
    expected.add("deadlocks");
    expected.add("flyway_schema_history");
    expected.add("holding_locks");
    expected.add("lock_waits");
    expected.add("locks");
    expected.add("transactions");
    expected.add("user_queries");
    expected.add("waiting_locks");
    Assertions.assertIterableEquals(expected, tables);
  }

  @Disabled
  @Test
  void insertUserQuery() throws SQLException{
    UserQuery userQuery = new UserQuery();
    userQuery.setUserHost("dbadmin2[dbadmin2]");
    userQuery.setIpAddress("172.16.2.208");
    userQuery.setConnectionId("311270893");
    userQuery.setQueryTime("0.000218");
    userQuery.setLockTime("0.000072");
    userQuery.setRowsSent("6");
    userQuery.setRowsExamined("12");
    userQuery.setTime("1552777235");

    sink.insertUserQueries(userQuery);

    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("select user_host, ip_address, id, query_time, "
          + "lock_time, rows_sent, rows_examined, log_time"
          + " from user_queries");

    resultSet.next();

    assertEquals("dbadmin2[dbadmin2]", resultSet.getString("user_host"));
    assertEquals("172.16.2.208", resultSet.getString("ip_address"));
    assertEquals("311270893", resultSet.getString("connectionId"));
    assertEquals(0.000218, resultSet.getDouble("query_time"));
    assertEquals(0.000072, resultSet.getDouble("lock_time"));
    assertEquals(6L, resultSet.getLong("rows_sent"));
    assertEquals(12L, resultSet.getLong("rows_examined"));
    assertEquals(LocalDateTime.of(2019, 3,16,23,0,35),
        resultSet.getTimestamp("log_time").toLocalDateTime());
  }
}
