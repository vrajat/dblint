package io.dblint.mart.server.commands.mysql;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dblint.mart.analyses.mysql.QueryStats;
import io.dblint.mart.analyses.mysql.SlowQuery;
import io.dblint.mart.metricsink.mysql.RewindBufferedReader;
import io.dblint.mart.metricsink.mysql.SchemaParser;
import io.dblint.mart.metricsink.mysql.SlowQueryLogParser;
import io.dblint.mart.metricsink.mysql.UserQuery;
import io.dblint.mart.metricsink.util.MetricAgentException;
import io.dblint.mart.server.MartConfiguration;
import io.dblint.mart.sqlplanner.QanException;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import javax.xml.stream.XMLStreamException;

public class SlowQueryLog extends LogParser {
  private static Logger logger = LoggerFactory.getLogger(SlowQueryLog.class);
  private List<UserQuery> queries;

  /**
   * A command to parse slow query logs.
   */
  public SlowQueryLog() {
    super("slowquerylog", "Analyze Queries in MySQL slow query log");
    this.queries = new ArrayList<>();
    /*
    subparser.addArgument("-s", "--schemaFile")
        .metavar("schemaFile")
        .type(String.class)
        .help("Path to MySQL schema dump in XML");
    */
  }


  @Override
  protected void process(Reader reader)
      throws IOException, MetricAgentException {

    SlowQueryLogParser parser = new SlowQueryLogParser();
    this.queries.addAll(parser.parseLog(
        new RewindBufferedReader(reader)
        )
    );
  }

  @Override
  protected void output(OutputStream os) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.writeValue(os, queries);
    /*
    MetricRegistry registry = new MetricRegistry();
    SlowQuery slowQuery = new SlowQuery(database, registry);
    slowQuery.analyze(queries);
    Map<String, QueryStats> statsMap = slowQuery.getAggQueryStats();

    logger.info("Sorted Queries");
    statsMap.entrySet().stream()
        .filter(entry -> entry.getValue().getNumQueries() > entry.getValue().getIndexUsed())
        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
        .limit(50)
        .forEach(entry -> logger.info(entry.getValue().toString()));

    registry.getCounters()
      .forEach((name, counter) -> logger.info(name + ":" + counter.getCount()));
    */
  }
}
