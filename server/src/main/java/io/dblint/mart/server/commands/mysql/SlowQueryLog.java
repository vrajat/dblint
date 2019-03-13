package io.dblint.mart.server.commands.mysql;

import com.codahale.metrics.MetricRegistry;
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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import javax.xml.stream.XMLStreamException;

public class SlowQueryLog extends ConfiguredCommand<MartConfiguration> {
  private static Logger logger = LoggerFactory.getLogger(SlowQueryLog.class);

  public SlowQueryLog() {
    super("slowquerylog", "Analyze Queries in MySQL slow query log");
  }


  @Override
  public void configure(Subparser subparser) {
    subparser.addArgument("-s", "--schemaFile")
        .metavar("schemaFile")
        .type(String.class)
        .help("Path to MySQL schema dump in XML");

    subparser.addArgument("-l", "--slowQueryLog")
        .metavar("slowQueryLog")
        .type(String.class)
        .help("Path to Slow Query Log");
  }

  @Override
  protected void run(Bootstrap<MartConfiguration> bootstrap, Namespace namespace,
                     MartConfiguration configuration)
      throws XMLStreamException, IOException, QanException, MetricAgentException {
    SchemaParser.Database database = SchemaParser.parseMySqlDump(
        new FileInputStream(namespace.getString("schemaFile")));


    logger.info(namespace.getString("slowQueryLog"));
    SlowQueryLogParser parser = new SlowQueryLogParser();
    List<UserQuery> queries = parser.parseLog(
        new RewindBufferedReader(new InputStreamReader(
            new FileInputStream(namespace.getString("slowQueryLog"))
        ))
    );

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
  }
}
