package io.inviscid.metricsink.mysql;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class SlowQueryLogParserTest {
  String logs =
      "/rdsdbbin/mysql/bin/mysqld, Version: 5.6.34-log (MySQL Community Server (GPL)). "
          + "started with:\n"
      + "Tcp port: 3306  Unix socket: /tmp/mysql.sock\n"
      + "Time                 Id Command    Argument\n"
      + "# Time: 180925 15:05:04\n"
      + "# User@Host: dbadmin2[dbadmin2] @  [192.168.0.1]  Id: 311266503\n"
      + "# Query_time: 50.325666  Lock_time: 0.000052 Rows_sent: 0  Rows_examined: 0\n"
      + "use a_db;\n"
      + "SET timestamp=1537887904;\n"
      + "INSERT INTO `a_table` (`created_on`, `updated_on`, `created_by_id`, `product_id`, "
      + "`quantity`) VALUES ('2018-09-25 20:34:13.682238', '2018-09-25 20:34:13.682586', 101, "
      + "202, 300);\n";

  String logaddendum =
      "# User@Host: dbadmin2[dbadmin2] @  [172.16.2.208]  Id: 311270893\n"
      + "# Query_time: 0.000218  Lock_time: 0.000072 Rows_sent: 6  Rows_examined: 12\n"
      + "SET timestamp=1537887930;\n"
      + "SELECT `store_id`, 'category_id`. `sum_order` FROM `products`;";


  @Test
  void parseTsLine() {
    UserQuery userQuery = new UserQuery();
    SlowQueryLogParser.parseTsLine("# Time: 180925 15:05:04", userQuery);
    assertEquals("180925 15:05:04", userQuery.getTime());
  }

  @Test
  void parseUhLine() {
    UserQuery userQuery = new UserQuery();
    boolean success = SlowQueryLogParser.parseUhLine(
        "# User@Host: dbadmin2[dbadmin2] @  [192.168.0.1]  Id: 311266503", userQuery);

    assertTrue(success);
    assertEquals("dbadmin2[dbadmin2]", userQuery.getUserHost());
    assertEquals("192.168.0.1", userQuery.getIpAddress());
    assertEquals("311266503", userQuery.getId());
  }

  @Test
  void parseSetStatement() {
    assertTrue(SlowQueryLogParser.parseSetUseStatment("SET timestamp=1537938341;"));
  }

  @Test
  void parseMultiSetStatement() {
    assertTrue(SlowQueryLogParser.parseSetUseStatment("SET timestamp=1537938341,insert_id=1;"));
  }

  @Disabled
  @Test
  void parseUseStatement() {
    assertTrue(SlowQueryLogParser.parseSetUseStatment("USE a_db;"));
  }

  @Test
  void replaceComments() {
    assertEquals("select  a from b;",
        SlowQueryLogParser.replaceComments("select /* ContentId:10 */ a from b;"));
  }

  @Test
  void parseEmptyDomainTest() {
    UserQuery userQuery = new UserQuery();
    SlowQueryLogParser.parseUhLine(
        "# User@Host: [] @ []  Id: 3112665503", userQuery);


    assertEquals("[]", userQuery.getUserHost());
    assertEquals("", userQuery.getIpAddress());
    assertEquals("3112665503", userQuery.getId());
  }

  @Test
  void parseQueryMetadataLine() {
    UserQuery userQuery = new UserQuery();
    SlowQueryLogParser.parseQueryMetadataLine(
        "# Query_time: 0.000783  Lock_time: 0.000195 Rows_sent: 22  Rows_examined: 27",
        userQuery
    );

    assertEquals(0.000783, userQuery.getQueryTime().doubleValue());
    assertEquals(0.000195, userQuery.getLockTime().doubleValue());
    assertEquals(22L, userQuery.getRowsSent());
    assertEquals(27L, userQuery.getRowsExamined());
  }

  @Test
  void parseLogTest() throws IOException {
   List<UserQuery> userQueries = SlowQueryLogParser.parseLog(
        new ByteArrayInputStream(logs.getBytes()));
   assertEquals(1, userQueries.size());
  }

  @Test
  void parse2LogTest() throws IOException {
    String completeLogs = logs + logaddendum;
    List<UserQuery> userQueries = SlowQueryLogParser.parseLog(
        new ByteArrayInputStream(completeLogs.getBytes()));
    assertEquals(2, userQueries.size());
  }
}