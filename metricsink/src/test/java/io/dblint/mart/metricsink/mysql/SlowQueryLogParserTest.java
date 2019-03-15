package io.dblint.mart.metricsink.mysql;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import io.dblint.mart.metricsink.util.MetricAgentException;
import org.junit.jupiter.api.Test;

class SlowQueryLogParserTest {
  String fileHeader =
      "/rdsdbbin/mysql/bin/mysqld, Version: 5.6.34-log (MySQL Community Server (GPL)). "
          + "started with:\n"
      + "Tcp port: 3306  Unix socket: /tmp/mysql.sock\n"
      + "Time                 Id Command    Argument\n"
      + "# Time: 190227 23:00:03\n";

  String timeSectionHeader =
      "# User@Host: [] @  []  Id: 33549154\n"
      + "# Query_time: 12.232073  Lock_time: 0.195784 Rows_sent: 1  Rows_examined: 1\n"
      + "use schema;\n"
      + "SET timestamp=1551308403;\n"
      + "throttle:       2878 'index not used' warning(s) suppressed.;\n";

  String querySection =
      "# User@Host: dbadmin2[dbadmin2] @  [172.16.2.208]  Id: 311270893\n"
      + "# Query_time: 0.000218  Lock_time: 0.000072 Rows_sent: 6  Rows_examined: 12\n"
      + "SET timestamp=1537887930;\n"
      + "SELECT `store_id`, 'category_id`. `sum_order` FROM `products`;";


  @Test
  void replaceComments() {
    assertEquals("select  a from b;",
        SlowQueryLogParser.replaceComments("select /* ContentId:10 */ a from b;"));
  }

  @Test
  void parseQueryTest() throws IOException, MetricAgentException {
    RewindBufferedReader bufferedReader = new RewindBufferedReader(
        new InputStreamReader(new ByteArrayInputStream(querySection.getBytes()))
    );
    UserQuery query = SlowQueryLogParser.parseQuery(bufferedReader);

    assertEquals("dbadmin2[dbadmin2]", query.getUserHost());
    assertEquals("172.16.2.208", query.getIpAddress());
    assertEquals("311270893", query.getId());
    assertEquals(0.000218, query.getQueryTime().doubleValue());
    assertEquals(0.000072, query.getLockTime().doubleValue());
    assertEquals(6L, query.getRowsSent());
    assertEquals(12L, query.getRowsExamined());
  }

  @Test
  void parseTimeSectionTest() throws IOException, MetricAgentException {
    String timeSection = timeSectionHeader + querySection;
    RewindBufferedReader bufferedReader = new RewindBufferedReader(
        new InputStreamReader(new ByteArrayInputStream(timeSection.getBytes()))
    );
    List<UserQuery> queries = SlowQueryLogParser.parseTimeSection(bufferedReader);
    assertEquals(1, queries.size());
  }

  @Test
  void parseFileSectionTest() throws IOException, MetricAgentException {
    String file = fileHeader + timeSectionHeader + querySection;
    RewindBufferedReader bufferedReader = new RewindBufferedReader(
        new InputStreamReader(new ByteArrayInputStream(file.getBytes()))
    );
    List<UserQuery> queries = SlowQueryLogParser.parseLog(bufferedReader);
    assertEquals(1, queries.size());
  }

  @Test
  void parseMultipleFileSectionTest() throws IOException, MetricAgentException {
    String file = fileHeader + timeSectionHeader + querySection;
    String doubleFile = file + file;
    RewindBufferedReader bufferedReader = new RewindBufferedReader(
        new InputStreamReader(new ByteArrayInputStream(doubleFile.getBytes()))
    );
    List<UserQuery> queries = SlowQueryLogParser.parseLog(bufferedReader);
    assertEquals(2, queries.size());
  }
}