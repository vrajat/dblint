package io.inviscid.mart.server;

import com.codahale.metrics.MetricRegistry;
import io.inviscid.metricsink.redshift.MySqlSink;
import io.inviscid.metricsink.redshift.RedshiftDb;
import io.inviscid.metricsink.redshift.UserQuery;
import io.inviscid.qan.RedshiftClassifier;
import io.inviscid.qan.enums.QueryType;
import io.inviscid.qan.enums.RedshiftEnum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;

public class BadQueriesCron extends Cron {
  private static Logger logger = LoggerFactory.getLogger(BadQueriesCron.class);

  RedshiftClassifier redshiftClassier;

  BadQueriesCron(int frequency, MetricRegistry metricRegistry,
                 RedshiftDb redshiftDb, MySqlSink mySqlSink) {
    super(mySqlSink, redshiftDb, frequency, metricRegistry, "badQueriesCron");

    redshiftClassier = new RedshiftClassifier();
  }

  /**
   * Run one iteration to get QueryStats from Redshift and
   * store into MySQL.
   */
  @Override
  public void run() {
    logger.debug("Run one instance of BadQueriesCron");

    LocalDateTime endRange = LocalDateTime.now();

    try {
      iterations.inc();
      List<UserQuery> userQueryList = redshiftDb.getQueries(startRange, endRange);
      for (UserQuery userQuery : userQueryList) {
        List<QueryType> queryTypes = redshiftClassier.classify(userQuery.sql);
        if (queryTypes.contains(RedshiftEnum.BAD_TOOMANYJOINS)) {
          mySqlSink.insertBadQueries(userQuery);
        }
      }
    } catch (Exception exc) {
      failedIterations.inc();
      logger.warn("Exception thrown", exc);
    } finally {
      startRange = endRange;
    }
  }
}
