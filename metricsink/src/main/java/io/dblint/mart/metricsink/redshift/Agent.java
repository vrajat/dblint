package io.dblint.mart.metricsink.redshift;

import io.dblint.mart.metricsink.util.MetricAgentException;

import java.util.List;

public interface Agent {
  public List<UserQuery> getQueries() throws MetricAgentException;
}
