package io.dblint.mart.metricsink.util;

public class MetricAgentException extends Exception {
  public MetricAgentException(Throwable throwable) {
    super(throwable);
  }

  public MetricAgentException(String message) {
    super(message);
  }
}
