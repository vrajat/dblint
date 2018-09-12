package io.inviscid.metricsink.metrics;

import org.flywaydb.core.Flyway;

public class Redshift implements Metrics {
  @Override
  public void setupRelationDb(String url, String user, String password) {
    Flyway flyway = new Flyway();
    this.setupRelationDb(url, user, password, flyway);
  }

  public void setupRelationDb(String url, String user, String password, Flyway flyway) {
    flyway.setDataSource(url, user, password);
    flyway.setLocations("db/redshiftMigrations");
    flyway.migrate();
  }
}
