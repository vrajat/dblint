package io.inviscid.mart.server;

import com.codahale.metrics.MetricRegistry;
import io.inviscid.metricsink.redshift.MySqlSink;
import io.inviscid.metricsink.redshift.RedshiftDb;
import io.inviscid.metricsink.redshift.UserConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ConnectionsCron extends Cron {
  private static Logger logger = LoggerFactory.getLogger(ConnectionsCron.class);

  ConnectionsCron(MySqlSink mySqlSink, RedshiftDb redshiftDb, int frequency,
                  MetricRegistry metricRegistry) {
    super(mySqlSink, redshiftDb, frequency, metricRegistry, "connectionsCron");
  }

  @Override
  public void run() {
    logger.debug("Run one instance of ConnectionsCron");

    try {
      iterations.inc();
      List<UserConnection> userConnections = redshiftDb.getUserConnections();
      for (UserConnection userConnection : userConnections) {
        mySqlSink.insertConnections(userConnection);
      }
    } catch (Exception exc) {
      failedIterations.inc();
      logger.warn("Exception thrown: ", exc);
    }
  }
}
