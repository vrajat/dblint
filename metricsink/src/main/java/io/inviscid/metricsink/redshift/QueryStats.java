package io.inviscid.metricsink.redshift;

import java.time.LocalDateTime;

public class QueryStats {
  final String db;
  final String user;
  final String queryGroup;
  final LocalDateTime day;
  final double minDuration;
  final double avgDuration;
  final double medianDuration;
  final double p75Duration;
  final double p90Duration;
  final double p95Duration;
  final double p99Duration;
  final double p999Duration;
  final double maxDuration;

  public QueryStats(String db,
                    String user,
                    String queryGroup,
                    LocalDateTime day,
                    double minDuration,
                    double avgDuration,
                    double medianDuration,
                    double p75Duration,
                    double p90Duration,
                    double p95Duration,
                    double p99Duration,
                    double p999Duration,
                    double maxDuration) {
    this.db = db;
    this.user = user;
    this.queryGroup = queryGroup;
    this.day = day;
    this.minDuration = minDuration;
    this.avgDuration = avgDuration;
    this.medianDuration = medianDuration;
    this.p75Duration = p75Duration;
    this.p90Duration = p90Duration;
    this.p95Duration = p95Duration;
    this.p99Duration = p99Duration;
    this.p999Duration = p999Duration;
    this.maxDuration = maxDuration;
  }

  public String getDb() {
    return db;
  }

  public String getUser() {
    return user;
  }

  public String getQueryGroup() {
    return queryGroup;
  }

  public LocalDateTime getDay() {
    return day;
  }

  public double getMinDuration() {
    return minDuration;
  }

  public double getAvgDuration() {
    return avgDuration;
  }

  public double getMedianDuration() {
    return medianDuration;
  }

  public double getP75Duration() {
    return p75Duration;
  }

  public double getP90Duration() {
    return p90Duration;
  }

  public double getP95Duration() {
    return p95Duration;
  }

  public double getP99Duration() {
    return p99Duration;
  }

  public double getP999Duration() {
    return p999Duration;
  }

  public double getMaxDuration() {
    return maxDuration;
  }
}
