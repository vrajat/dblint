import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connections analyzes the connections to redshift.
 * Analyzes the main users of live sessions
 */

public class Connections {
  private static final Logger logger = LoggerFactory.getLogger(Connections.class);

  class User {
    public final String userName;
    public final String dbName;

    User(String userName, String dbName) {
      this.userName = userName;
      this.dbName = dbName;
    }

    @Override
    public String toString() {
      return userName + ":" + dbName;
    }
  }

  final Connection connection;
  int numConnections;
  List<User> topUsers;

  Connections(Connection connection) {
    this.connection = connection;
    numConnections = 0;
    topUsers = new ArrayList<>();
  }

  public void capture() throws SQLException {
    logger.debug("Start analysis");

    // 1. Get number of active sessions
    Statement activeSessionsStatement = this.connection.createStatement();
    ResultSet activeSessionsResultSet = activeSessionsStatement.executeQuery("select count(*) from stv_sessions");
    try {
      if (activeSessionsResultSet.next()) {
        numConnections = activeSessionsResultSet.getInt(1);
        logger.debug("No. of active sessions: " + numConnections);
      }
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
        User user = new User(topFiveResultSet.getString("user_name"),
            topFiveResultSet.getString("db_name"));
        topUsers.add(user);
        logger.debug(user.toString());
      }
    } finally {
      topFiveResultSet.close();
      topFiveStatement.close();
    }
  }

  public int getNumConnections() {
    return numConnections;
  }

  public List<User> getTopUsers() {
    return topUsers;
  }
}
