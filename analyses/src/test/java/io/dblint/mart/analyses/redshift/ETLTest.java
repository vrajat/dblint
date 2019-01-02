package io.dblint.mart.analyses.redshift;

import com.codahale.metrics.MetricRegistry;
import io.dblint.mart.metricsink.redshift.Agent;
import io.dblint.mart.metricsink.redshift.RedshiftCsv;
import io.dblint.mart.metricsink.redshift.UserQuery;
import io.dblint.mart.metricsink.util.MetricAgentException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ETLTest {
  Logger logger = LoggerFactory.getLogger(ETLTest.class);

  MetricRegistry registry;
  Etl etl;

  @BeforeEach
  void setUp() {
    registry = new MetricRegistry();
    etl = new Etl(registry);
  }

  @Disabled
  @Tag("cmdLine")
  @Test
  void cmdLineTest() throws IOException, MetricAgentException {
    assertNotNull(System.getProperty("csvFile"));
    logger.info(System.getProperty("csvFile"));
    InputStream inputStream = new FileInputStream(System.getProperty("csvFile"));
    MetricRegistry registry = new MetricRegistry();

    Agent agent = new RedshiftCsv(inputStream);
    etl.analyze(agent);
  }

  private UserQuery getUserQuery(String query) {
    return new UserQuery(
      100, 5052, 0, 0, LocalDateTime.of(2018, 9, 19, 13, 8, 3),
      LocalDateTime.of(2018, 9, 19, 15, 46, 51),
      90.0, "default", false, query);
  }

  @Test
  void testMaintenanceQuery() {
    List<UserQuery> userQueries = new ArrayList<>();
    userQueries.add(getUserQuery("Vacuum table integrity check before execution"));
    userQueries.add(getUserQuery("padb_fetch_sample: select * from results"));

    List<QueryInfo> queryInfos = etl.parse(userQueries);
    assertEquals(0, queryInfos.size());
    assertEquals(2, registry.counter("io.dblint.Etl.numParsed").getCount());
    assertEquals(2, registry.counter("io.dblint.Etl.numMaintenanceQueries").getCount());
  }

  @Test
  void testSelectQuery() {
    List<UserQuery> userQueries = new ArrayList<>();
    userQueries.add(getUserQuery("select count(*) from results"));

    List<QueryInfo> queryInfos = etl.parse(userQueries);
    assertEquals(0, queryInfos.size());
    assertEquals(1, registry.counter("io.dblint.Etl.numParsed").getCount());
    assertEquals(0, registry.counter("io.dblint.Etl.numMaintenanceQueries").getCount());
    assertEquals(0, registry.counter("io.dblint.Etl.numInserts").getCount());
    assertEquals(0, registry.counter("io.dblint.Etl.numInsertSelects").getCount());
    assertEquals(0, registry.counter("io.dblint.Etl.numCtas").getCount());
  }

  @Test
  void testSelectDml() {
    List<UserQuery> userQueries = new ArrayList<>();
    userQueries.add(getUserQuery("insert into a select b,c from results"));
    userQueries.add(getUserQuery("insert into a(b) select 1"));
    userQueries.add(getUserQuery("create table a as select b,c from results"));
    userQueries.add(getUserQuery("create table a (b int)"));

    List<QueryInfo> queryInfos = etl.parse(userQueries);
    assertEquals(2, queryInfos.size());
    assertEquals(4, registry.counter("io.dblint.Etl.numParsed").getCount());
    assertEquals(0, registry.counter("io.dblint.Etl.numMaintenanceQueries").getCount());
    assertEquals(2, registry.counter("io.dblint.Etl.numInserts").getCount());
    assertEquals(1, registry.counter("io.dblint.Etl.numInsertSelects").getCount());
    assertEquals(1, registry.counter("io.dblint.Etl.numCtas").getCount());
  }

  @Test
  void testSyntaxError() {
    List<UserQuery> userQueries = new ArrayList<>();
    userQueries.add(getUserQuery("insert from results"));

    List<QueryInfo> queryInfos = etl.parse(userQueries);
    assertEquals(0, queryInfos.size());
  }
}
