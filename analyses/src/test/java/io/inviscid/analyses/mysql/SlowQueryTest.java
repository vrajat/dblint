package io.inviscid.analyses.mysql;

import javax.xml.stream.XMLStreamException;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.codahale.metrics.MetricRegistry;
import io.inviscid.metricsink.mysql.SchemaParser;
import io.inviscid.metricsink.mysql.SlowQueryLogParser;
import io.inviscid.metricsink.mysql.UserQuery;
import io.inviscid.sqlplanner.QanException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlowQueryTest {
  private static Logger logger = LoggerFactory.getLogger(SlowQueryTest.class);

  @Disabled
  @Test
  @Tag("cmdline")
  void cmdLineTest() throws XMLStreamException, IOException, QanException {
    logger.info(System.getProperty("schemaFile"));
    SchemaParser.Database database = SchemaParser.parseMySqlDump(
        new FileInputStream(System.getProperty("schemaFile")));


    logger.info(System.getProperty("slowFile"));
    SlowQueryLogParser parser = new SlowQueryLogParser();
    List<UserQuery> queries = parser.parseLog(
        new FileInputStream(System.getProperty("slowFile"))
    );

    MetricRegistry registry = new MetricRegistry();
    SlowQuery slowQuery = new SlowQuery(database, registry);
    slowQuery.analyze(queries);
    Map<String, QueryStats> statsMap = slowQuery.getAggQueryStats();

    logger.info("Sorted Queries");
    statsMap.entrySet().stream()
        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
        .limit(50)
        .forEach((queryStats) -> {
          logger.info(queryStats.toString());
        });
    registry.getCounters()
      .forEach((name, counter) -> logger.info(name + ":" + counter.getCount()));
  }
}
