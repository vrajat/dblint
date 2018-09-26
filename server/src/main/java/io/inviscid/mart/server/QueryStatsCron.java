package io.inviscid.mart.server;

import com.codahale.metrics.MetricRegistry;
import io.inviscid.metricsink.redshift.MySqlSink;
import io.inviscid.metricsink.redshift.QueryStats;
import io.inviscid.metricsink.redshift.RedshiftDb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;

public class QueryStatsCron extends Cron {
  private static final Logger logger = LoggerFactory.getLogger(QueryStatsCron.class);

  QueryStatsCron(int frequency, MetricRegistry metricRegistry,
                 RedshiftDb redshiftDb, MySqlSink mySqlSink) {
    super(mySqlSink, redshiftDb, frequency, metricRegistry, "queryStatsCron");
  }

  /**
   * Run one iteration to get QueryStats from Redshift and
   * store into MySQL.
   */
  public void run() {
    logger.debug("Run one instance of QueryStatsCron");

    LocalDateTime endRange = LocalDateTime.now();

    try {
      iterations.inc();
      List<QueryStats> queryStatsList = redshiftDb.getQueryStats(false,
          startRange, endRange);
      for (QueryStats queryStats : queryStatsList) {
        mySqlSink.insertQueryStats(queryStats);
      }
    } catch (Exception exc) {
      failedIterations.inc();
      logger.warn("Exception thrown", exc);
    } finally {
      startRange = endRange;
    }
  }
}
