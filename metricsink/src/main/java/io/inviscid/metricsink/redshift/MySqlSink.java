package io.inviscid.metricsink.redshift;

import org.flywaydb.core.Flyway;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.reflect.FieldMapper;

public class MySqlSink {
  final String url;
  final String user;
  final String password;
  final Flyway flyway;
  final Jdbi jdbi;

  public MySqlSink(String url, String user, String password) {
    this(url, user, password, new Flyway());
  }

  /**
   * Create a MySqlSink for Redshift metrics.
   *
   * @param url URL of the MySQL Database
   * @param user user of the MySQL Database
   * @param password password of the MySQL database
   * @param flyway Migrations library to setup the MySQL database
   */

  public MySqlSink(String url, String user, String password, Flyway flyway) {
    this.url = url;
    this.user = user;
    this.password = password;
    this.flyway = flyway;
    this.jdbi = Jdbi.create(url, user, password);
  }

  void initialize() {
    flyway.setDataSource(url, user, password);
    flyway.setLocations("db/redshiftMigrations");
    flyway.migrate();
  }

  public void close() {
    flyway.clean();
  }

  void insertQueryStats(QueryStats queryStats) {
    jdbi.useHandle(handle -> {
      handle.registerRowMapper(FieldMapper.factory(QueryStats.class));
      handle.createUpdate("insert into query_stats(db, user, query_group, day, "
          + "min_duration, avg_duration, median_duration, p75_duration, p90_duration, p95_duration,"
          + "p99_duration, p999_duration, max_duration) values ("
          + ":db, :user, :queryGroup, :day, :minDuration, :avgDuration, :medianDuration, "
          + ":p75, :p90, :p95, :p99, :p999, :maxDuration)")
          .bindFields(queryStats)
          .execute();
    });
  }
}
