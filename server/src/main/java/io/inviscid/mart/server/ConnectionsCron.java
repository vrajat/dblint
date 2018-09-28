package io.inviscid.mart.server;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import io.inviscid.metricsink.redshift.MySqlSink;
import io.inviscid.metricsink.redshift.RedshiftDb;
import io.inviscid.metricsink.redshift.UserConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;

public class ConnectionsCron extends Cron {
  private static Logger logger = LoggerFactory.getLogger(ConnectionsCron.class);

  Counter connectionRows;

  ConnectionsCron(MySqlSink mySqlSink, RedshiftDb redshiftDb, int frequency,
                  MetricRegistry metricRegistry) {
    super(mySqlSink, redshiftDb, frequency, metricRegistry, "connectionsCron");
    connectionRows = metricRegistry.counter("inviscid.connections_cron.connection_rows");
  }

  @Override
  public void run() {
    logger.debug("Run one instance of ConnectionsCron");

    try {
      iterations.inc();
      LocalDateTime pollTime = LocalDateTime.now();
      List<UserConnection> userConnections = redshiftDb.getUserConnections();
      connectionRows.inc(userConnections.size());
      logger.info("Processing " + userConnections.size() + " rows");
      for (UserConnection userConnection : userConnections) {
        userConnection.pollTime = pollTime;
        mySqlSink.insertConnections(userConnection);
      }
    } catch (Exception exc) {
      failedIterations.inc();
      logger.warn("Exception thrown: ", exc);
    }
  }
}
