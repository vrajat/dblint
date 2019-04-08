package io.dblint.mart.server.commands.mysql;

import com.codahale.metrics.MetricRegistry;
import io.dblint.mart.analyses.mysql.SlowQuery;
import io.dblint.mart.metricsink.mysql.QueryAttribute;
import io.dblint.mart.metricsink.mysql.Sink;
import io.dblint.mart.metricsink.mysql.UserQuery;
import io.dblint.mart.server.MartConfiguration;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.apache.calcite.sql.parser.SqlParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

public class AnalyzeSlowQuery extends TimeRange {
  private static Logger logger = LoggerFactory.getLogger(AnalyzeSlowQuery.class);

  public AnalyzeSlowQuery() {
    super("analyze-slow-query", "Analyze Slow Queries and augment with more information");
  }

  @Override
  public void configure(Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("-c", "--connection")
        .type(String.class)
        .help("Connection String to Database");

  }

  @Override
  protected void run(Bootstrap<MartConfiguration> bootstrap,
                     Namespace namespace,
                     MartConfiguration martConfiguration) {
    String startTime = namespace.getString("startTime");
    String endTime = namespace.getString("endTime");

    MetricRegistry registry = new MetricRegistry();
    Sink sink = new Sink("jdbc:sqlite:" + namespace.getString("connection"), "", "", registry);
    sink.initialize();

    List<UserQuery> queryList = sink.selectUserQueries(
        ZonedDateTime.of(LocalDateTime.parse(startTime, dateFormat),
            ZoneOffset.ofHoursMinutes(5, 30)),
            ZonedDateTime.of(LocalDateTime.parse(endTime, dateFormat),
                ZoneOffset.ofHoursMinutes(5, 30)
            ));

    queryList.forEach(userQuery -> {
      SlowQuery slowQuery = new SlowQuery(registry);
      try {
        QueryAttribute attribute = slowQuery.analyze(userQuery.getQuery());
        sink.setQueryAttribute(userQuery, attribute);
      } catch (SqlParseException | UnsupportedOperationException exc) {
        logger.error("Failed to analyze query '" + userQuery.getId() + "'." + exc.getMessage());
      }
    });
    logger.info(registry.toString());
  }
}
