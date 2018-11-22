package io.dblint.mart.server;

import com.codahale.metrics.health.HealthCheck;

public class CronHealthCheck extends HealthCheck {
  final Cron cron;
  final double limit;

  CronHealthCheck(Cron cron) {
    this(cron, 0.1);
  }

  CronHealthCheck(Cron cron, double limit) {
    this.cron = cron;
    this.limit = limit;
  }

  @Override
  public Result check() {
    if (cron.getIterations() == 0) {
      return Result.healthy();
    }

    double percentFailed = ((double)cron.getFailedIterations()) / cron.getIterations();
    if (percentFailed <= limit) {
      return Result.healthy();
    }

    return Result.unhealthy(String.format("Failed Percentage is %f", percentFailed));
  }
}
