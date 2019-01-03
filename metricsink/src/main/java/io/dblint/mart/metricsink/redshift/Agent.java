package io.dblint.mart.metricsink.redshift;

import io.dblint.mart.metricsink.util.MetricAgentException;

import java.time.LocalDateTime;
import java.util.List;

public interface Agent {
  List<UserQuery> getQueries(LocalDateTime rangeStart, LocalDateTime rangeEnd)
      throws MetricAgentException;
}
