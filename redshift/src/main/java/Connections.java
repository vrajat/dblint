import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connections analyzes the connections to redshift.
 * Analyzes the main users of live sessions
 */

public class Connections {
  private static final Logger logger = LoggerFactory.getLogger(Connections.class);
  final Connection connection;

  Connections(Connection connection) {
    this.connection = connection;
  }

  public void analyze() throws SQLException {
    logger.debug("Start analysis");

    // 1. Get number of active sessions
    Statement activeSessionsStatement = this.connection.createStatement();
    ResultSet activeSessionsResultSet = activeSessionsStatement.executeQuery("select count(*) from stv_sessions");
    try {
      logger.info("No. of active sessions: " + activeSessionsResultSet.getString(1));
    } finally {
      activeSessionsResultSet.close();
      activeSessionsStatement.close();
    }

    // 2. Get top 5 users
    Statement topFiveStatement = this.connection.createStatement();
    ResultSet topFiveResultSet = topFiveStatement.executeQuery(
        "select user_name, db_name, count(*) as count " +
            "from stv_sessions " +
            "group by user_name, db_name " +
            "order by count " +
            "limit 5"
    );

    try {
      logger.info("Top 5 users: ");
      while (topFiveResultSet.next()) {
        logger.info(topFiveResultSet.getString("user_name")
            + ":"
            + topFiveResultSet.getString("db_name"));
      }
    } finally {
      topFiveResultSet.close();
      topFiveStatement.close();
    }
  }
}
