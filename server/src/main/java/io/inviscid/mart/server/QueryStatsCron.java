package io.inviscid.mart.server;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import io.inviscid.mart.server.configuration.QueryStatsCronConfiguration;
import io.inviscid.metricsink.redshift.MySqlSink;
import io.inviscid.metricsink.redshift.QueryStats;
import io.inviscid.metricsink.redshift.RedshiftDb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class QueryStatsCron implements Runnable, Cron {
  private static final Logger logger = LoggerFactory.getLogger(QueryStatsCron.class);

  final MySqlSink mySqlSink;
  final RedshiftDb redshiftDb;
  final QueryStatsCronConfiguration queryStatsCronConfiguration;

  Counter iterations;
  Counter failedIterations;

  QueryStatsCron(QueryStatsCronConfiguration queryStatsCronConfiguration,
                 MetricRegistry metricRegistry) {
    this(queryStatsCronConfiguration, metricRegistry,
        new RedshiftDb(queryStatsCronConfiguration.getRedshift().getUrl(),
            queryStatsCronConfiguration.getRedshift().getUser(),
            queryStatsCronConfiguration.getRedshift().getPassword()),
        new MySqlSink(queryStatsCronConfiguration.getMySql().getUrl(),
            queryStatsCronConfiguration.getMySql().getUser(),
            queryStatsCronConfiguration.getMySql().getPassword())

    );
  }

  QueryStatsCron(QueryStatsCronConfiguration queryStatsCronConfiguration,
                 MetricRegistry metricRegistry, RedshiftDb redshiftDb, MySqlSink mySqlSink) {
    this.queryStatsCronConfiguration = queryStatsCronConfiguration;
    this.redshiftDb = redshiftDb;
    this.mySqlSink = mySqlSink;

    iterations = metricRegistry.counter("queryStatsCron.iterations");
    failedIterations = metricRegistry.counter("queryStatsCron.failedIterations");
  }

  /**
   * Run one iteration to get QueryStats from Redshift and
   * store into MySQL.
   */
  public void run() {
    logger.debug("Run one instance of QueryStatsCron");

    try {
      iterations.inc();
      List<QueryStats> queryStatsList = redshiftDb.getQueryStats(false);
      for (QueryStats queryStats : queryStatsList) {
        mySqlSink.insertQueryStats(queryStats);
      }
    } catch (Exception exc) {
      failedIterations.inc();
      logger.warn("Exception thrown", exc);
    }
  }

  @Override
  public long getIterations() {
    return iterations.getCount();
  }

  @Override
  public long getFailedIterations() {
    return failedIterations.getCount();
  }
}
