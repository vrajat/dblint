package io.dblint.mart.metricsink.mysql;

import com.codahale.metrics.MetricRegistry;
import io.dblint.mart.metricsink.util.DbSink;
import org.flywaydb.core.Flyway;
import org.jdbi.v3.core.mapper.reflect.FieldMapper;

public class Sink extends DbSink {

  public Sink(String url, String user, String password,
                   MetricRegistry metricRegistry) {
    super(url, user, password, metricRegistry, new Flyway());
  }

  /**
   * Create a Sink for MySQL metrics.
   *
   * @param url URL of the MySQL Database
   * @param user user of the MySQL Database
   * @param password password of the MySQL database
   * @param metricRegistry MetricRegistry to store JDBI metrics
   * @param flyway Migrations library to setup the MySQL database
   */

  public Sink(String url, String user, String password,
                   MetricRegistry metricRegistry, Flyway flyway) {
    super(url, user, password, metricRegistry, flyway);
  }

  @Override
  protected String getMigrationsPath() {
    return "db/mySqlMigrations";
  }

  /**
   * Insert one QueryStat row into user_queries table.
   * @param userQuery A POJO of User Query
   */
  public void insertUserQueries(UserQuery userQuery) {
    jdbi.useHandle(handle -> {
      handle.registerRowMapper(FieldMapper.factory(UserQuery.class));
      handle.createUpdate("insert into user_queries("
          + "log_time,"
          + "user_host,"
          + "ip_address,"
          + "connection_id,"
          + "query_time,"
          + "lock_time,"
          + "rows_sent,"
          + "rows_examined,"
          + "query"
          + ") values ("
          + ":time, :userHost, :ipAddress, :connectionId, :queryTime, :lockTime, :rowsSent, "
          + ":rowsExamined, :query)")
          .bindBean(userQuery)
          .execute();
    });
  }
}
