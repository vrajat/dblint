package io.dblint.mart.metricsink.mysql;

import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SinkTest {
  private static Logger logger = LoggerFactory.getLogger("SinkTest");

  @TempDir
  static Path sharedTempDir;
  static String url;

  private MetricRegistry metricRegistry;
  private Connection connection;
  private Sink sink;
  private static UserQuery testQuery;

  @BeforeAll
  static void setTestQuery() {
    testQuery = new UserQuery();
    testQuery.setUserHost("dbadmin2[dbadmin2]");
    testQuery.setIpAddress("172.16.2.208");
    testQuery.setConnectionId("311270893");
    testQuery.setQueryTime("0.000218");
    testQuery.setLockTime("0.000072");
    testQuery.setRowsSent("6");
    testQuery.setRowsExamined("12");
    testQuery.setTime("1552777235");

    url = "jdbc:sqlite:" + sharedTempDir.resolve("sqldb");
    logger.debug(url);
  }

  @BeforeEach
  void setConnection() throws SQLException {
    connection = DriverManager.getConnection(url);
    metricRegistry = new MetricRegistry();
    sink = new Sink(url, "", "", metricRegistry);
    sink.initialize();
  }

  @AfterEach
  void closeConnection() throws SQLException {
    connection.close();
  }

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
    expected.add("query_attributes");
    expected.add("transactions");
    expected.add("user_queries");
    expected.add("waiting_locks");
    Assertions.assertIterableEquals(expected, tables);
  }

  @Test
  void insertUserQuery() throws SQLException {
    sink.insertUserQuery(testQuery);

    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("select id, user_host, ip_address, id, query_time, "
          + "lock_time, rows_sent, rows_examined, log_time, connection_id"
          + " from user_queries");

    resultSet.next();

    assertEquals(1, resultSet.getInt("id"));
    assertEquals("dbadmin2[dbadmin2]", resultSet.getString("user_host"));
    assertEquals("172.16.2.208", resultSet.getString("ip_address"));
    assertEquals("311270893", resultSet.getString("connection_id"));
    assertEquals(0.000218, resultSet.getDouble("query_time"));
    assertEquals(0.000072, resultSet.getDouble("lock_time"));
    assertEquals(6L, resultSet.getLong("rows_sent"));
    assertEquals(12L, resultSet.getLong("rows_examined"));
    assertEquals(ZonedDateTime.of(LocalDateTime.of(2019, 3,17,4,30,35), ZoneOffset.UTC),
        ZonedDateTime.of(resultSet.getTimestamp("log_time").toLocalDateTime(), ZoneOffset.UTC));
  }

  @Test
  void insertQueryAttribute() throws SQLException {
    QueryAttribute queryAttribute = new QueryAttribute("SELECT `A`, `B`\n"
        + "FROM `D`\n"
        + "WHERE `C` = ?");
    sink.insertQueryAttribute(queryAttribute);

    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(
        "select digest, digest_hash from query_attributes");

    assertEquals("SELECT `A`, `B`\n"
        + "FROM `D`\n"
        + "WHERE `C` = ?", resultSet.getString("digest"));
    assertEquals("bbdd1e7260fdd5fc159a12248d059e4a1a294ecd52c8287ed2e71708908dd142",
        resultSet.getString("digest_hash"));
  }

  @Test
  void updateUserQuery() throws SQLException {
    testQuery.setId(1);
    testQuery.setDigestHash("bbdd1e7260fdd5fc159a12248d059e4a1a294ecd52c8287ed2e71708908dd142");
    sink.updateUserQuery(testQuery);

    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("select id, user_host, ip_address, id, query_time, "
          + "lock_time, rows_sent, rows_examined, log_time, connection_id, digest_hash"
          + " from user_queries");

    resultSet.next();

    assertEquals("bbdd1e7260fdd5fc159a12248d059e4a1a294ecd52c8287ed2e71708908dd142",
        resultSet.getString("digest_hash"));
  }
}
