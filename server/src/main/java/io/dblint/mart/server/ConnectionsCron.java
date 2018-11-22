package io.dblint.mart.server;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import io.dblint.mart.metricsink.redshift.MySqlSink;
import io.dblint.mart.metricsink.redshift.RedshiftDb;
import io.dblint.mart.metricsink.redshift.RunningQuery;
import io.dblint.mart.metricsink.redshift.UserConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;

public class ConnectionsCron extends Cron {
  private static Logger logger = LoggerFactory.getLogger(ConnectionsCron.class);

  Counter connectionRows;
  Counter queryRows;

  ConnectionsCron(MySqlSink mySqlSink, RedshiftDb redshiftDb, int frequency,
                  MetricRegistry metricRegistry) {
    super(mySqlSink, redshiftDb, frequency, metricRegistry, "connectionsCron");
    connectionRows = metricRegistry.counter("inviscid.connectionsCron.connectionRows");
    queryRows = metricRegistry.counter("inviscid.connectionsCron.queryRows");
  }

  @Override
  public void run() {
    logger.debug("Run one instance of ConnectionsCron");

    try {
      iterations.inc();
      getUserConnections();
      getRunningQueries();
    } catch (Exception exc) {
      failedIterations.inc();
      logger.warn("Exception thrown: ", exc);
    }
  }

  private void getUserConnections() {
    LocalDateTime pollTime = LocalDateTime.now();
    List<UserConnection> userConnections = redshiftDb.getUserConnections();
    connectionRows.inc(userConnections.size());
    logger.info("Processing " + userConnections.size() + " connections");
    for (UserConnection userConnection : userConnections) {
      userConnection.pollTime = pollTime;
      mySqlSink.insertConnections(userConnection);
    }
  }

  private void getRunningQueries() {
    LocalDateTime pollTime = LocalDateTime.now();
    List<RunningQuery> queries = redshiftDb.getRunningQueries();
    queryRows.inc(queries.size());
    logger.info("Processing " + queries.size() + " queries");
    for (RunningQuery query : queries) {
      query.pollTime = pollTime;
      mySqlSink.insertRunningQueries(query);
    }
  }
}
