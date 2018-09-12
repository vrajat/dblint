package io.inviscid.metricsink.metrics;

public interface Metrics {
  public abstract void setupRelationDb(String url, String user, String password);
}
