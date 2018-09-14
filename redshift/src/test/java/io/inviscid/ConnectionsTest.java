package io.inviscid;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ConnectionsTest {
  private static Connection h2db;

  @BeforeAll
  static void setUp() throws SQLException {
    h2db = DriverManager.getConnection("jdbc:h2:mem:io.inviscid.ConnectionsTest");

    // Create the table stv_sessions
    try (Statement createTable = h2db.createStatement()) {
      createTable.execute("create table stv_sessions("
          + "starttime timestamp,"
          + "process int,"
          + "user_name varchar(50),"
          + "db_name varchar(50))");
    }

    //Insert a few rows into the table
    try (Statement insert = h2db.createStatement()) {
      insert.execute("insert into stv_sessions values("
          + "'2018-01-01 00:00:00', 10, 'user_1', 'db_1')");
      insert.execute("insert into stv_sessions values("
          + "'2018-01-02 00:00:00', 11, 'user_1', 'db_2')");
      insert.execute("insert into stv_sessions values("
          + "'2018-01-01 00:00:00', 12, 'user_1', 'db_1')");
      insert.execute("insert into stv_sessions values("
          + "'2018-01-01 00:00:00', 13, 'user_12', 'db_10')");
      insert.execute("insert into stv_sessions values("
          + "'2018-01-01 00:00:00', 14, 'user_13', 'db_1')");
    }
  }

  @AfterAll
  static void tearDown() throws SQLException {
    if (h2db != null) {
      h2db.close();
    }
  }

  @Test
  void sanityTest() throws SQLException {
    Connections connections = new Connections(h2db);
    connections.capture();

    assertEquals(5, connections.getNumConnections());
    assertEquals(4, connections.getTopUsers().size());
  }
}