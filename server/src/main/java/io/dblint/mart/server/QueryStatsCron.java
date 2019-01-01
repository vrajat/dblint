package io.dblint.mart.server;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import io.dblint.mart.metricsink.redshift.MySqlSink;
import io.dblint.mart.metricsink.redshift.QueryStats;
import io.dblint.mart.metricsink.redshift.RedshiftDb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;

public class QueryStatsCron extends Cron {
  private static final Logger logger = LoggerFactory.getLogger(QueryStatsCron.class);

  Counter numQueries;

  QueryStatsCron(int frequency, MetricRegistry metricRegistry,
                 RedshiftDb redshiftDb, MySqlSink mySqlSink) {
    super(mySqlSink, redshiftDb, frequency, metricRegistry, "queryStatsCron");
    numQueries = metricRegistry.counter("inviscid.query_stats_cron.num_queries");
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
      redshiftDb.setRangeStart(startRange);
      redshiftDb.setRangeEnd(endRange);
      List<QueryStats> queryStatsList = redshiftDb.getQueryStats(false);
      numQueries.inc(queryStatsList.size());
      logger.info("Processing " + queryStatsList.size() + " queries");
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
