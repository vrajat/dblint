package io.inviscid.metricsink.redshift;

import org.flywaydb.core.Flyway;
import org.jdbi.v3.core.Jdbi;

public class MySQLSink {
  final String url;
  final String user;
  final String password;
  final Flyway flyway;
  final Jdbi jdbi;

  public MySQLSink(String url, String user, String password) {
    this(url, user, password, new Flyway());
  }

  public MySQLSink(String url, String user, String password, Flyway flyway) {
    this.url = url;
    this.user = user;
    this.password = password;
    this.flyway = flyway;
    this.jdbi = Jdbi.create(url, user, password);
  }

  public void initialize() {
    flyway.setDataSource(url, user, password);
    flyway.setLocations("db/redshiftMigrations");
    flyway.migrate();
  }

  public void close() {
    flyway.clean();
  }

  public void insertQueryStats(QueryStats queryStats) {
    jdbi.useHandle(handle -> {
      handle.createUpdate("insert into query_stats(db, user, query_group, day, " +
          "min_duration, avg_duration, median_duration, p75_duration, p90_duration, p95_duration," +
          "p99_duration, p999_duration, max_duration) values (" +
          ":db, :user, :queryGroup, :day, :minDuration, :avgDuration, :medianDuration, " +
          ":p75Duration, :p90Duration, :p95Duration, :p99Duration, :p999Duration, :maxDuration)")
          .bindBean(queryStats)
          .execute();
    });
  }
}
