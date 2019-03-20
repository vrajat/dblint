package io.dblint.mart.metricsink.util;

import com.codahale.metrics.MetricRegistry;
import org.flywaydb.core.Flyway;
import org.jdbi.v3.core.Jdbi;

public abstract class DbSink {
  final String url;
  final String user;
  final String password;
  final Flyway flyway;
  protected final Jdbi jdbi;

  /**
   * Create a MySqlSink for Redshift metrics.
   *
   * @param url URL of the MySQL Database
   * @param user user of the MySQL Database
   * @param password password of the MySQL database
   * @param metricRegistry MetricRegistry to store JDBI metrics
   * @param flyway Migrations library to setup the MySQL database
   */

  protected DbSink(String url, String user, String password,
                   MetricRegistry metricRegistry, Flyway flyway) {
    this.url = url;
    this.user = user;
    this.password = password;
    this.flyway = flyway;
    this.jdbi = Jdbi.create(url, user, password);
    this.jdbi.setSqlLogger(new JdbiTimer(metricRegistry));
  }

  /**
   * Setup MySQL with tables to store metrics.
   */
  public void initialize() {
    flyway.setDataSource(url, user, password);
    flyway.setLocations(this.getMigrationsPath());
    flyway.migrate();
  }

  public void close() {
    flyway.clean();
  }

  protected abstract String getMigrationsPath();
}
