package io.inviscid.metricsink.sinks;

import io.inviscid.metricsink.metrics.Metrics;

public class MySQLSink {
  public final String url;
  public final String user;
  public final String password;
  public final Metrics metrics;

  public MySQLSink(String url, String user, String password, Metrics metrics) {
    this.url = url;
    this.user = user;
    this.password = password;
    this.metrics = metrics;
    metrics.setupRelationDb(this.url, this.user, this.password);
  }
}
